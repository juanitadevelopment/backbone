package net.teppan.backbone.event;

import java.time.Instant;
import java.util.Optional;

/**
 * A read-only view of one row in the transactional outbox, for inspection and
 * operations such as retrying or discarding a dead-lettered event.
 *
 * <p>This carries metadata only, not the serialized payload: it is meant for
 * monitoring and triage (which events are stuck, how many times they have been
 * attempted, and why they failed), not for re-reading event contents.
 *
 * @param id        the outbox row id, used by {@link Outbox#retry(long)} and
 *                  {@link Outbox#discard(long)}
 * @param type      the fully-qualified event class name
 * @param createdAt when the event was written to the outbox
 * @param attempts  how many delivery attempts have been made
 * @param status    the row's delivery status
 * @param lastError the last delivery error, if any
 * @see Outbox#peekPending(int)
 * @see Outbox#peekDeadLetters(int)
 */
public record OutboxEntry(long id, String type, Instant createdAt, int attempts,
                          Status status, Optional<String> lastError) {

    /** Delivery status of an outbox row. */
    public enum Status {
        /** Awaiting delivery; the poller will attempt it. */
        PENDING,
        /** Delivered to subscribers and retained only until purged. */
        PROCESSED,
        /** Failed too many times (or could not be decoded); will not be retried
         *  automatically. Inspect, then {@link Outbox#retry(long)} or
         *  {@link Outbox#discard(long)}. */
        DEAD
    }
}
