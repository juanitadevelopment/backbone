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
                 "SELECT COUNT(*) FROM backbone_outbox WHERE status = 'PENDING'")) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    // ── Dead-lettering, retry, discard ───────────────────────────────────────────

    @Test
    void deadLettersAfterMaxAttempts() throws Exception {
        var failures = new AtomicInteger();
        try (var outbox = new Outbox(ds, e -> {
                    failures.incrementAndGet();
                    throw new RuntimeException("always down");
                }, List.of(Ping.class), Duration.ofMillis(30), Duration.ofDays(1), 3)) {

            writeCommitted(outbox, new Ping("doomed"));
            outbox.poke();

            awaitDeadLetters(outbox, 1);
            assertThat(outbox.pendingCount()).isZero();
            assertThat(outbox.deadLetterCount()).isEqualTo(1);
            assertThat(failures.get()).isEqualTo(3);

            var dead = outbox.peekDeadLetters(10);
            assertThat(dead).hasSize(1);
            assertThat(dead.get(0).type()).isEqualTo(Ping.class.getName());
            assertThat(dead.get(0).attempts()).isEqualTo(3);
            assertThat(dead.get(0).status()).isEqualTo(OutboxEntry.Status.DEAD);
            assertThat(dead.get(0).lastError()).isPresent();
        }
    }

    @Test
    void poisonPayloadIsDeadLetteredImmediately() throws Exception {
        // A row whose payload cannot be decoded can never be delivered.
        try (var conn = ds.getConnection()) {
            // The table exists only after an Outbox has applied the schema; create
            // one first, then insert garbage bypassing the codec.
            try (var outbox = new Outbox(ds, e -> { throw new AssertionError("must not deliver: " + e); },
                    List.of(Ping.class), Duration.ofMillis(30), Duration.ofDays(1), 5)) {

                try (var ps = conn.prepareStatement(
                        "INSERT INTO backbone_outbox (event_type, payload) VALUES (?, ?)")) {
                    ps.setString(1, Ping.class.getName());
                    ps.setBytes(2, new byte[]{0x01, 0x02, 0x03});  // not a serialized object
                    ps.executeUpdate();
                }
                outbox.poke();

                awaitDeadLetters(outbox, 1);
                assertThat(outbox.pendingCount()).isZero();
                var dead = outbox.peekDeadLetters(10);
                assertThat(dead).hasSize(1);
                assertThat(dead.get(0).attempts()).isEqualTo(1);   // not retried
                assertThat(dead.get(0).lastError()).isPresent();
            }
        }
    }

    @Test
    void retryRequeuesADeadLetterAndItIsDelivered() throws Exception {
        var down = new java.util.concurrent.atomic.AtomicBoolean(true);
        var delivered = new CopyOnWriteArrayList<Object>();
        try (var outbox = new Outbox(ds, e -> {
                    if (down.get()) throw new RuntimeException("downstream down");
                    delivered.add(e);
                }, List.of(Ping.class), Duration.ofMillis(30), Duration.ofDays(1), 2)) {

            writeCommitted(outbox, new Ping("recover"));
            outbox.poke();
            awaitDeadLetters(outbox, 1);

            long id = outbox.peekDeadLetters(1).get(0).id();
            down.set(false);                       // downstream fixed
            assertThat(outbox.retry(id)).isTrue();

            awaitUntil(() -> !delivered.isEmpty());
            assertThat(delivered).containsExactly(new Ping("recover"));
            assertThat(outbox.deadLetterCount()).isZero();
            assertThat(outbox.pendingCount()).isZero();
        }
    }

    @Test
    void discardRemovesADeadLetter() throws Exception {
        try (var outbox = new Outbox(ds, e -> { throw new RuntimeException("down"); },
                List.of(Ping.class), Duration.ofMillis(30), Duration.ofDays(1), 1)) {

            writeCommitted(outbox, new Ping("trash"));
            outbox.poke();
            awaitDeadLetters(outbox, 1);

            long id = outbox.peekDeadLetters(1).get(0).id();
            assertThat(outbox.discard(id)).isTrue();
            assertThat(outbox.deadLetterCount()).isZero();
            assertThat(outbox.discard(id)).isFalse();   // already gone
        }
    }

    @Test
    void retryReturnsFalseForUnknownOrLiveRows() throws Exception {
        try (var outbox = new Outbox(ds, e -> {},
                List.of(Ping.class), Duration.ofMillis(30), Duration.ofDays(1), 3)) {
            assertThat(outbox.retry(999_999L)).isFalse();   // no such row
        }
    }

    private static void awaitDeadLetters(Outbox outbox, long n) throws Exception {
        awaitUntil(() -> outbox.deadLetterCount() >= n);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier cond) throws Exception {
        long deadline = System.currentTimeMillis() + 3000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(cond.getAsBoolean()).isTrue();
    }
}
