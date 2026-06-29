package net.teppan.backbone.event;

import net.teppan.shazo.jdbc.embedded.EmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxTest {

    record Ping(String msg) implements Serializable {}

    private DataSource ds;

    @BeforeEach
    void setUp() {
        ds = EmbeddedDataSource.inMemory("outbox_" + System.nanoTime());
    }

    private void writeCommitted(Outbox outbox, Object event) throws Exception {
        try (var conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            outbox.write(conn, List.of(event));
            conn.commit();
        }
    }

    @Test
    void deliversCommittedEvents() throws Exception {
        var delivered = new CopyOnWriteArrayList<Object>();
        var latch = new CountDownLatch(1);
        try (var outbox = new Outbox(ds, e -> { delivered.add(e); latch.countDown(); },
                List.of(Ping.class), Duration.ofMillis(50), Duration.ofDays(1))) {

            writeCommitted(outbox, new Ping("hello"));
            outbox.poke();

            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(delivered).containsExactly(new Ping("hello"));
        }
    }

    @Test
    void doesNotDeliverRolledBackEvents() throws Exception {
        var delivered = new CopyOnWriteArrayList<Object>();
        try (var outbox = new Outbox(ds, delivered::add,
                List.of(Ping.class), Duration.ofMillis(50), Duration.ofDays(1))) {

            try (var conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                outbox.write(conn, List.of(new Ping("ghost")));
                conn.rollback();      // event row never commits
            }
            outbox.poke();
            Thread.sleep(300);
            assertThat(delivered).isEmpty();
        }
    }

    @Test
    void retriesUntilDeliverySucceeds() throws Exception {
        var attempts = new AtomicInteger();
        var succeeded = new CountDownLatch(1);
        try (var outbox = new Outbox(ds, e -> {
                    if (attempts.incrementAndGet() < 2) {
                        throw new RuntimeException("transient failure");
                    }
                    succeeded.countDown();
                }, List.of(Ping.class), Duration.ofMillis(50), Duration.ofDays(1))) {

            writeCommitted(outbox, new Ping("retry-me"));
            outbox.poke();

            assertThat(succeeded.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void marksDeliveredEventsProcessed() throws Exception {
        var latch = new CountDownLatch(1);
        try (var outbox = new Outbox(ds, e -> latch.countDown(),
                List.of(Ping.class), Duration.ofMillis(50), Duration.ofDays(1))) {
            writeCommitted(outbox, new Ping("once"));
            outbox.poke();
            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();

            // The row is marked processed just after delivery; wait for it while
            // the poller is still running.
            long deadline = System.currentTimeMillis() + 2000;
            while (countPending() > 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertThat(countPending()).isZero();
        }
    }

    private long countPending() throws Exception {
        try (var conn = ds.getConnection();
             var st = conn.createStatement();
             var rs = st.executeQuery(
                 "SELECT COUNT(*) FROM backbone_outbox WHERE processed_at IS NULL")) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }
}
