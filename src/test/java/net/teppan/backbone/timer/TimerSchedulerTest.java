package net.teppan.backbone.timer;

import net.teppan.backbone.AppContext;
import net.teppan.shazo.jdbc.embedded.EmbeddedDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimerSchedulerTest {

    private DataSource ds;
    private TimerScheduler scheduler;

    @BeforeEach
    void setUp() {
        ds = EmbeddedDataSource.inMemory("timer_test_" + System.nanoTime());
        scheduler = TimerScheduler.builder().dataSource(ds).build();
    }

    @AfterEach
    void tearDown() {
        scheduler.close();
    }

    @Test
    void intervalJob_firesRepeatedly() throws InterruptedException {
        var latch = new CountDownLatch(2);
        scheduler.schedule("tick", Duration.ofMillis(50), ctx -> latch.countDown());
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void intervalJob_receivesSystemAppContext() throws InterruptedException {
        var latch = new CountDownLatch(1);
        List<AppContext> contexts = new ArrayList<>();

        scheduler.schedule("ctx-check", Duration.ofMillis(50), ctx -> {
            contexts.add(ctx);
            latch.countDown();
        });
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(contexts.get(0).principal().id()).isEqualTo("system");
    }

    @Test
    void cronJob_firesAtNextMatchingSecond() throws InterruptedException {
        var latch = new CountDownLatch(1);
        // every second
        scheduler.schedule("every-sec", "* * * * * *", ctx -> latch.countDown());
        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void suspendAndResume_stopsAndRestartsJob() throws InterruptedException {
        var fireCount = new java.util.concurrent.atomic.AtomicInteger(0);
        var firstFire = new CountDownLatch(1);

        scheduler.schedule("pausable", Duration.ofMillis(50), ctx -> {
            fireCount.incrementAndGet();
            firstFire.countDown();
        });

        // Wait for at least one fire, then suspend
        firstFire.await(2, TimeUnit.SECONDS);
        scheduler.suspend("pausable");
        assertThat(scheduler.status("pausable")).isEqualTo(TimerScheduler.JobStatus.SUSPENDED);

        int countAfterSuspend = fireCount.get();
        Thread.sleep(200); // job should NOT fire during this window
        assertThat(fireCount.get()).isEqualTo(countAfterSuspend);

        // Resume and verify it fires again
        scheduler.resume("pausable");
        assertThat(scheduler.status("pausable")).isEqualTo(TimerScheduler.JobStatus.RUNNING);
        int countBeforeResumeFire = fireCount.get();
        Thread.sleep(300); // job should fire at least once
        assertThat(fireCount.get()).isGreaterThan(countBeforeResumeFire);
    }

    @Test
    void cancel_permanentlyStopsJob() throws InterruptedException {
        var firstFire = new CountDownLatch(1);
        var fireCount = new java.util.concurrent.atomic.AtomicInteger(0);

        scheduler.schedule("stoppable", Duration.ofMillis(50), ctx -> {
            fireCount.incrementAndGet();
            firstFire.countDown();
        });
        firstFire.await(2, TimeUnit.SECONDS);
        scheduler.cancel("stoppable");
        assertThat(scheduler.status("stoppable")).isEqualTo(TimerScheduler.JobStatus.CANCELLED);

        int countAfterCancel = fireCount.get();
        Thread.sleep(200);
        assertThat(fireCount.get()).isEqualTo(countAfterCancel);
    }

    @Test
    void resumeSuspended_restartsFiring() throws InterruptedException {
        var latch = new CountDownLatch(1);

        scheduler.schedule("resumable", Duration.ofMillis(50), ctx -> latch.countDown());
        Thread.sleep(30);
        scheduler.suspend("resumable");
        Thread.sleep(100);
        scheduler.resume("resumable");

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void jobStatuses_snapshotsAllJobs() {
        scheduler.schedule("j1", Duration.ofSeconds(10), ctx -> {});
        scheduler.schedule("j2", Duration.ofSeconds(10), ctx -> {});
        scheduler.suspend("j2");
        assertThat(scheduler.jobStatuses())
            .containsEntry("j1", TimerScheduler.JobStatus.RUNNING)
            .containsEntry("j2", TimerScheduler.JobStatus.SUSPENDED);
    }

    @Test
    void status_unknownJob_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> scheduler.status("ghost"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("ghost");
    }

    @Test
    void duplicateSchedule_throwsIllegalArgumentException() {
        scheduler.schedule("dup", Duration.ofSeconds(10), ctx -> {});
        assertThatThrownBy(() -> scheduler.schedule("dup", Duration.ofSeconds(10), ctx -> {}))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dup");
    }

    @Test
    void jobException_doesNotKillScheduler() throws InterruptedException {
        var secondFire = new CountDownLatch(2);
        scheduler.schedule("error-prone", Duration.ofMillis(50), ctx -> {
            secondFire.countDown();
            if (secondFire.getCount() > 0) throw new RuntimeException("intentional");
        });
        assertThat(secondFire.await(2, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void builder_withoutDataSource_throwsIllegalStateException() {
        assertThatThrownBy(() -> TimerScheduler.builder().build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("dataSource");
    }

    @Test
    void builder_rejectsNonPositivePoolSize() {
        assertThatThrownBy(() -> TimerScheduler.builder().dataSource(ds).poolSize(0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void longRunningJob_doesNotStarveOtherJobs() throws InterruptedException {
        // With a single scheduler thread the slow job (sleeps past the await
        // window) would block the fast one; the default multi-thread pool lets
        // the fast job fire promptly.
        var fastFired = new CountDownLatch(1);
        scheduler.schedule("slow", Duration.ofMillis(50), ctx -> Thread.sleep(3000));
        scheduler.schedule("fast", Duration.ofMillis(50), ctx -> fastFired.countDown());
        assertThat(fastFired.await(2, TimeUnit.SECONDS)).isTrue();
    }
}
