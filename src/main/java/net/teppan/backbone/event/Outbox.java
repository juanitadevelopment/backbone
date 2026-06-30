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
import java.util.Optional;
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
 * <h2>Dead-lettering</h2>
 * <p>Each delivery attempt is counted. An event that keeps failing is not
 * retried forever: after {@code maxAttempts} failures it moves to a terminal
 * {@code DEAD} status and the poller skips it. An event whose payload cannot be
 * decoded (a "poison" event) is dead-lettered immediately, since it can never
 * succeed. Dead-lettered events can be {@linkplain #peekDeadLetters(int)
 * inspected} and then {@linkplain #retry(long) retried} (e.g. once the
 * downstream is fixed) or {@linkplain #discard(long) discarded}.
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
    private static final int    MAX_ERROR_LEN = 2000;

    /** Default number of delivery attempts before an event is dead-lettered. */
    public static final int DEFAULT_MAX_ATTEMPTS = 10;

    private final DataSource dataSource;
    private final Codec<Serializable> codec;
    private final Consumer<Object> deliverer;
    private final Duration pollInterval;
    private final Duration retention;
    private final int maxAttempts;

    private final Semaphore signal = new Semaphore(0);
    private volatile boolean running = true;
    private final Thread worker;
    private volatile Instant lastPurge = Instant.EPOCH;

    /**
     * Creates and starts an outbox with the default maximum attempts.
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
        this(dataSource, deliverer, eventTypes, pollInterval, retention, DEFAULT_MAX_ATTEMPTS);
    }

    /**
     * Creates and starts an outbox.
     *
     * @param dataSource   the database holding the outbox table; never {@code null}
     * @param deliverer    receives each event after commit; never {@code null}
     * @param eventTypes   the serializable event classes this outbox carries
     * @param pollInterval how long the poller waits when idle; never {@code null}
     * @param retention    how long processed rows are kept before purging;
     *                     never {@code null}
     * @param maxAttempts  delivery attempts before dead-lettering; must be &ge; 1
     */
    public Outbox(DataSource dataSource, Consumer<Object> deliverer,
                  List<Class<?>> eventTypes, Duration pollInterval, Duration retention,
                  int maxAttempts) {
        this.dataSource   = Objects.requireNonNull(dataSource, "dataSource");
        this.deliverer    = Objects.requireNonNull(deliverer, "deliverer");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        this.retention    = Objects.requireNonNull(retention, "retention");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1: " + maxAttempts);
        }
        this.maxAttempts = maxAttempts;

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
     * Returns the number of events awaiting delivery (for monitoring). Excludes
     * dead-lettered events; see {@link #deadLetterCount()}.
     *
     * @return the count of pending outbox rows
     */
    public long pendingCount() {
        return count("PENDING");
    }

    /**
     * Returns the number of dead-lettered events — those that exhausted their
     * delivery attempts or could not be decoded.
     *
     * @return the count of dead-lettered outbox rows
     */
    public long deadLetterCount() {
        return count("DEAD");
    }

    /**
     * Returns up to {@code limit} pending events, oldest first, for inspection.
     *
     * @param limit the maximum number of rows to return; must be &ge; 0
     * @return the pending entries (metadata only)
     */
    public List<OutboxEntry> peekPending(int limit) {
        return peek("PENDING", limit);
    }

    /**
     * Returns up to {@code limit} dead-lettered events, oldest first, for triage.
     *
     * @param limit the maximum number of rows to return; must be &ge; 0
     * @return the dead-lettered entries (metadata only)
     */
    public List<OutboxEntry> peekDeadLetters(int limit) {
        return peek("DEAD", limit);
    }

    /**
     * Requeues a dead-lettered event for delivery, resetting its attempt count.
     * Use after fixing whatever caused delivery to fail.
     *
     * @param id the outbox row id
     * @return {@code true} if a dead-lettered row was requeued; {@code false} if
     *         no such dead-lettered row exists
     */
    public boolean retry(long id) {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "UPDATE backbone_outbox SET status = 'PENDING', attempts = 0,"
                 + " last_error = NULL, failed_at = NULL WHERE id = ? AND status = 'DEAD'")) {
            ps.setLong(1, id);
            boolean requeued = ps.executeUpdate() > 0;
            if (requeued) poke();
            return requeued;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to retry outbox event id=" + id, e);
        }
    }

    /**
     * Permanently deletes an outbox row, whatever its status. Intended for
     * discarding a dead-lettered event that should not be delivered.
     *
     * @param id the outbox row id
     * @return {@code true} if a row was deleted
     */
    public boolean discard(long id) {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement("DELETE FROM backbone_outbox WHERE id = ?")) {
            ps.setLong(1, id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to discard outbox event id=" + id, e);
        }
    }

    @Override
    public void close() {
        running = false;
        worker.interrupt();
    }

    // ── Queries shared by monitoring methods ─────────────────────────────────────

    private long count(String status) {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "SELECT COUNT(*) FROM backbone_outbox WHERE status = ?")) {
            ps.setString(1, status);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count " + status + " outbox events", e);
        }
    }

    private List<OutboxEntry> peek(String status, int limit) {
        if (limit < 0) throw new IllegalArgumentException("limit must be >= 0: " + limit);
        if (limit == 0) return List.of();
        var entries = new ArrayList<OutboxEntry>(Math.min(limit, BATCH_SIZE));
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "SELECT id, event_type, created_at, attempts, status, last_error"
                 + " FROM backbone_outbox WHERE status = ? ORDER BY id LIMIT ?")) {
            ps.setString(1, status);
            ps.setInt(2, limit);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var createdAt = rs.getTimestamp(3);
                    entries.add(new OutboxEntry(
                        rs.getLong(1),
                        rs.getString(2),
                        createdAt == null ? null : createdAt.toInstant(),
                        rs.getInt(4),
                        OutboxEntry.Status.valueOf(rs.getString(5)),
                        Optional.ofNullable(rs.getString(6))));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read " + status + " outbox events", e);
        }
        return entries;
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
                // A payload that cannot be decoded can never be delivered: send it
                // straight to the dead-letter state instead of dropping it.
                log.error("Cannot decode outbox event id={} ({}); dead-lettering",
                    row.id(), row.type(), e);
                deadLetter(row.id(), row.attempts() + 1, "decode failed: " + e.getMessage());
                continue;
            }
            try {
                deliverer.accept(event);
            } catch (Exception e) {
                int next = row.attempts() + 1;
                if (next >= maxAttempts) {
                    log.error("Delivery failed for outbox event id={} after {} attempts;"
                        + " dead-lettering", row.id(), next, e);
                    deadLetter(row.id(), next, e.toString());
                } else {
                    log.warn("Delivery failed for outbox event id={} (attempt {}/{}); will retry",
                        row.id(), next, maxAttempts, e);
                    recordFailedAttempt(row.id(), next, e.toString());
                }
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
                 "SELECT id, event_type, payload, attempts FROM backbone_outbox"
                 + " WHERE status = 'PENDING' ORDER BY id LIMIT " + BATCH_SIZE)) {
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new Pending(rs.getLong(1), rs.getString(2), rs.getBytes(3), rs.getInt(4)));
                }
            }
        }
        return rows;
    }

    private void markProcessed(long id) throws SQLException {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "UPDATE backbone_outbox SET status = 'PROCESSED', processed_at = ?"
                 + " WHERE id = ? AND status = 'PENDING'")) {
            ps.setTimestamp(1, Timestamp.from(Instant.now()));
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    /** Records a failed attempt that will be retried; the row stays {@code PENDING}. */
    private void recordFailedAttempt(long id, int attempts, String error) throws SQLException {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "UPDATE backbone_outbox SET attempts = ?, last_error = ?"
                 + " WHERE id = ? AND status = 'PENDING'")) {
            ps.setInt(1, attempts);
            ps.setString(2, truncate(error));
            ps.setLong(3, id);
            ps.executeUpdate();
        }
    }

    /** Moves a row to the terminal {@code DEAD} status. */
    private void deadLetter(long id, int attempts, String error) throws SQLException {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(
                 "UPDATE backbone_outbox SET status = 'DEAD', attempts = ?, last_error = ?,"
                 + " failed_at = ? WHERE id = ? AND status = 'PENDING'")) {
            ps.setInt(1, attempts);
            ps.setString(2, truncate(error));
            ps.setTimestamp(3, Timestamp.from(Instant.now()));
            ps.setLong(4, id);
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
                 "DELETE FROM backbone_outbox WHERE status = 'PROCESSED' AND processed_at < ?")) {
            ps.setTimestamp(1, Timestamp.from(now.minus(retention)));
            int purged = ps.executeUpdate();
            if (purged > 0) log.debug("Purged {} processed outbox rows", purged);
        }
    }

    private static String truncate(String error) {
        if (error == null) return null;
        return error.length() <= MAX_ERROR_LEN ? error : error.substring(0, MAX_ERROR_LEN);
    }

    private record Pending(long id, String type, byte[] payload, int attempts) {}
}
