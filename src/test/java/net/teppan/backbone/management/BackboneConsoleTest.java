package net.teppan.backbone.management;

import net.teppan.backbone.Principal;
import net.teppan.backbone.ServiceRunner;
import net.teppan.backbone.timer.TimerScheduler;
import net.teppan.backbone.timer.TimerScheduler.JobStatus;
import net.teppan.shazo.jdbc.embedded.EmbeddedDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackboneConsoleTest {

    record Note(String text) implements Serializable {}

    private DataSource ds;
    private ServiceRunner runner;
    private TimerScheduler scheduler;

    @BeforeEach
    void setUp() {
        ds = EmbeddedDataSource.inMemory("console_" + System.nanoTime());
    }

    @AfterEach
    void tearDown() {
        if (runner != null) runner.close();
        if (scheduler != null) scheduler.close();
    }

    @Test
    void snapshot_combinesServicesOutboxAndJobs() {
        runner = ServiceRunner.builder().dataSource(ds)
            .durableEvents(Note.class)
            .register("noop", ctx -> null)
            .build();
        scheduler = TimerScheduler.builder().dataSource(ds).build();
        scheduler.schedule("nightly", Duration.ofHours(1), ctx -> {});

        var console = BackboneConsole.builder().serviceRunner(runner).scheduler(scheduler).build();

        var snap = console.snapshot();
        assertThat(snap.services()).containsExactly("noop");
        assertThat(snap.pendingEvents()).isPresent();
        assertThat(snap.deadLetteredEvents()).isPresent();
        assertThat(snap.jobs()).containsEntry("nightly", JobStatus.RUNNING);
    }

    @Test
    void snapshot_withoutOutboxOrScheduler_isEmptyThere() {
        runner = ServiceRunner.builder().dataSource(ds).register("svc", ctx -> null).build();
        var console = BackboneConsole.builder().serviceRunner(runner).build();

        var snap = console.snapshot();
        assertThat(snap.services()).containsExactly("svc");
        assertThat(snap.pendingEvents()).isEmpty();
        assertThat(snap.deadLetteredEvents()).isEmpty();
        assertThat(snap.jobs()).isEmpty();
    }

    @Test
    void retryAllDeadLetters_requeuesEveryDeadLetter() throws Exception {
        var down = new AtomicBoolean(true);
        var delivered = new java.util.concurrent.CopyOnWriteArrayList<String>();
        runner = ServiceRunner.builder().dataSource(ds)
            .durableEvents(Note.class)
            .outboxMaxAttempts(1)
            .outboxPollInterval(Duration.ofMillis(30))
            .subscribe(Note.class, n -> {
                if (down.get()) throw new RuntimeException("down");
                delivered.add(n.text());
            })
            .build();
        var console = BackboneConsole.builder().serviceRunner(runner).build();

        // Publish three events that all dead-letter (subscriber down, maxAttempts=1).
        for (int i = 0; i < 3; i++) {
            final String text = "n" + i;
            runner.run(ctx -> { ctx.publish(new Note(text)); return null; }, Principal.system());
        }
        awaitUntil(() -> runner.deadLetterCount().orElse(0) == 3);

        down.set(false);
        assertThat(console.retryAllDeadLetters()).isEqualTo(3);

        awaitUntil(() -> delivered.size() == 3);
        assertThat(delivered).containsExactlyInAnyOrder("n0", "n1", "n2");
        assertThat(runner.deadLetterCount().getAsLong()).isZero();
    }

    @Test
    void suspendAllAndResumeAllJobs() {
        runner = ServiceRunner.builder().dataSource(ds).build();
        scheduler = TimerScheduler.builder().dataSource(ds).build();
        scheduler.schedule("a", Duration.ofHours(1), ctx -> {});
        scheduler.schedule("b", Duration.ofHours(1), ctx -> {});
        var console = BackboneConsole.builder().serviceRunner(runner).scheduler(scheduler).build();

        assertThat(console.suspendAllJobs()).isEqualTo(2);
        assertThat(console.jobStatuses()).containsValues(JobStatus.SUSPENDED, JobStatus.SUSPENDED);

        assertThat(console.resumeAllJobs()).isEqualTo(2);
        assertThat(console.jobStatuses()).containsValues(JobStatus.RUNNING, JobStatus.RUNNING);
    }

    @Test
    void singleJobControl() {
        runner = ServiceRunner.builder().dataSource(ds).build();
        scheduler = TimerScheduler.builder().dataSource(ds).build();
        scheduler.schedule("job", Duration.ofHours(1), ctx -> {});
        var console = BackboneConsole.builder().serviceRunner(runner).scheduler(scheduler).build();

        console.suspendJob("job");
        assertThat(console.jobStatuses()).containsEntry("job", JobStatus.SUSPENDED);
        console.resumeJob("job");
        assertThat(console.jobStatuses()).containsEntry("job", JobStatus.RUNNING);
        console.cancelJob("job");
        assertThat(console.jobStatuses()).containsEntry("job", JobStatus.CANCELLED);
    }

    @Test
    void jobControlWithoutScheduler() {
        runner = ServiceRunner.builder().dataSource(ds).build();
        var console = BackboneConsole.builder().serviceRunner(runner).build();

        assertThat(console.jobStatuses()).isEmpty();
        assertThat(console.suspendAllJobs()).isZero();
        assertThat(console.resumeAllJobs()).isZero();
        assertThatThrownBy(() -> console.suspendJob("x")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void builderRequiresServiceRunner() {
        assertThatThrownBy(() -> BackboneConsole.builder().build())
            .isInstanceOf(IllegalStateException.class);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier cond) throws Exception {
        long deadline = System.currentTimeMillis() + 3000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(cond.getAsBoolean()).isTrue();
    }
}
