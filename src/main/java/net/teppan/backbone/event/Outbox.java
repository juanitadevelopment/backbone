package net.teppan.backbone.event;

import net.teppan.shazo.ShazoException;
import net.teppan.shazo.http.Codec;
import net.teppan.shazo.jdbc.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A transactional outbox for domain events.
 *
 * <p>Events are written to the {@code backbone_outbox} table using the
 * <em>same</em> JDBC connection (and therefore the same transaction) as the
 * business change that produced them — see {@link #write(Connection, List)}.
 * Because the event row and the business data commit or roll back together,
 * there is never a committed change without its event, nor an event without its
 * change. After commit, a background poller delivers each unprocessed event to a
 * subscriber callback and marks it processed.
 *
 * <p>Delivery is <strong>at-least-once</strong>: an event is marked processed
 * only after the subscriber returns, so a crash mid-delivery causes
 * re-delivery. Subscribers must therefore be idempotent. A single poller per
 * outbox table is assumed; running several concurrently may duplicate delivery.
 *
 * <p>Events must be {@link Serializable}; their concrete types are declared up
 * front so the deserialization allowlist can reject anything else.
 *
 * @see net.teppan.backbone.ServiceRunner
 */
public final class Outbox implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Outbox.class);

    private static final String SCHEMA_LOCATION = "net/teppan/backbone/schema/";
    private static final int    BATCH_SIZE = 50;

    private final DataSource dataSource;
    private final Codec<Serializable> codec;
    private final Consumer<Object> deliverer;
    private final Duration pollInterval;
    private final Duration retention;

    private final Semaphore signal = new Semaphore(0);
    private volatile boolean running = true;
    private final Thread worker;
    private volatile Instant lastPurge = Instant.EPOCH;

    /**
     * Creates and starts an outbox.
     *
     * @param dataSource   the database holding the outbox table; never {@code null}
     * @param deliverer    receives each event after commit; never {@code null}
     * @param eventTypes   the serializable event classes this outbox carries
     * @param pollInterval how long the poller waits when idle; never {@code null}
     * @param retention    how long processed rows are kept before purging;
     *                     never {@code null}
     */
    public Outbox(DataSource dataSource, Consumer<Object> deliverer,
                  List<Class<?>> eventTypes, Duration pollInterval, Duration retention) {
        this.dataSource   = Objects.requireNonNull(dataSource, "dataSource");
        this.deliverer    = Objects.requireNonNull(deliverer, "deliverer");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        this.retention    = Objects.requireNonNull(retention, "retention");

        var allowed = new ArrayList<Class<?>>(eventTypes);
        this.codec = Codec.java(Serializable.class, allowed.toArray(Class<?>[]::new));

        try {
            SchemaManager.apply(dataSource, SCHEMA_LOCATION);
        } catch (ShazoException e) {
            throw new IllegalStateException("Failed to apply backbone outbox schema", e);
        }
        this.worker = Thread.ofVirtual().name("backbone-outbox-poller").start(this::pollLoop);
    }

    /**
     * Writes the given events to the outbox using {@code txConn}, which must be
     * the connection of the surrounding business transaction so the events
     * commit atomically with it. Does nothing for an empty list.
     *
     * @param txConn the transaction's connection; never {@code null}
     * @param events the events to enqueue; each must be {@link Serializable}
     * @throws ShazoException if an event cannot be serialized
     * @throws SQLException   if the insert fails
     */
    public void write(Connection txConn, List<Object> events)
            throws ShazoException, SQLException {
        if (events.isEmpty()) return;
        try (var ps = txConn.prepareStatement(
                "INSERT INTO backbone_outbox (event_type, payload) VALUES (?, ?)")) {
            for (Object event : events) {
                if (!(event instanceof Serializable s)) {
                    throw new ShazoException(
                        "Durable event must be Serializable: " + event.getClass().getName());
                }
                ps.setString(1, event.getClass().getName());
                ps.setBytes(2, codec.encode(s));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** Wakes the poller to deliver promptly instead of waiting for the next tick. */
    public void poke() {
        signal.release();
    }

    /**
     * Returns the number of events not yet delivered (for monitoring).
     *
     * @return the count of unprocessed outbox rows
     */
    public long pendingCount() {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "SELECT COUNT(*) FROM backbone_outbox WHERE processed_at IS NULL");
             var rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count pending outbox events", e);
        }
    }

    @Override
    public void close() {
        running = false;
        worker.interrupt();
    }

    // ── Poller ──────────────────────────────────────────────────────────────────

    private void pollLoop() {
        while (running) {
            try {
                int delivered = drainOnce();
                purgeIfDue();
                if (delivered == 0) {
                    signal.tryAcquire(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("Outbox poll iteration failed", e);
                try {
                    signal.tryAcquire(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private int drainOnce() throws SQLException {
        var pending = fetchPending();
        for (var row : pending) {
            Object event;
            try {
                event = codec.decode(row.payload());
            } catch (ShazoException e) {
                log.error("Failed to decode outbox event id={} ({})", row.id(), row.type(), e);
                markProcessed(row.id());  // poison message: do not retry forever
                continue;
            }
            try {
                deliverer.accept(event);
            } catch (Exception e) {
                // Leave unprocessed so it is retried on the next cycle.
                log.warn("Delivery failed for outbox event id={}; will retry", row.id(), e);
                continue;
            }
            markProcessed(row.id());
        }
        return pending.size();
    }

    private List<Pending> fetchPending() throws SQLException {
        var rows = new ArrayList<Pending>(BATCH_SIZE);
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "SELECT id, event_type, payload FROM backbone_outbox"
                 + " WHERE processed_at IS NULL ORDER BY id LIMIT " + BATCH_SIZE)) {
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new Pending(rs.getLong(1), rs.getString(2), rs.getBytes(3)));
                }
            }
        }
        return rows;
    }

    private void markProcessed(long id) throws SQLException {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "UPDATE backbone_outbox SET processed_at = ? WHERE id = ? AND processed_at IS NULL")) {
            ps.setTimestamp(1, Timestamp.from(Instant.now()));
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    private void purgeIfDue() throws SQLException {
        var now = Instant.now();
        if (Duration.between(lastPurge, now).compareTo(pollInterval.multipliedBy(50)) < 0) {
            return;  // purge at most once per ~50 poll intervals
        }
        lastPurge = now;
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "DELETE FROM backbone_outbox WHERE processed_at IS NOT NULL AND processed_at < ?")) {
            ps.setTimestamp(1, Timestamp.from(now.minus(retention)));
            int purged = ps.executeUpdate();
            if (purged > 0) log.debug("Purged {} processed outbox rows", purged);
        }
    }

    private record Pending(long id, String type, byte[] payload) {}
}
