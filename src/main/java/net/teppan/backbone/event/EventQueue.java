package net.teppan.backbone.event;

import java.util.function.Consumer;

/**
 * A typed message queue for intra-application event delivery.
 *
 * <p>Events are delivered to all registered {@link #subscribe subscribers}
 * asynchronously on a background thread. Ordering is FIFO within a single
 * queue instance.
 *
 * <p>{@link TransientEventQueue} is the in-memory implementation (events are
 * lost on restart). For durable, transactional, at-least-once delivery tied to
 * a service's commit, use {@link net.teppan.backbone.ServiceRunner}'s
 * {@linkplain net.teppan.backbone.ServiceRunner.Builder#durableEvents(Class[])
 * outbox} instead of this lower-level queue.
 *
 * <p>Callers should register subscribers before publishing events to avoid
 * a brief window where events are dispatched to no listener.
 *
 * @param <E> the event type
 */
public interface EventQueue<E> extends AutoCloseable {

    /**
     * Publishes an event to this queue.
     *
     * <p>Returns immediately; delivery to subscribers happens asynchronously.
     *
     * @param event the event to publish; must not be {@code null}
     */
    void publish(E event);

    /**
     * Registers a subscriber to receive all future events from this queue.
     *
     * <p>The listener is called on the queue's internal delivery thread.
     * Listeners must not block for extended periods.
     *
     * @param listener the event consumer; never {@code null}
     */
    void subscribe(Consumer<E> listener);

    /**
     * Returns the number of events waiting to be dispatched.
     *
     * @return the pending event count
     */
    int size();

    /**
     * Returns {@code true} if events survive a process restart.
     *
     * @return {@code true} for persistent implementations
     */
    boolean isPersistent();

    /**
     * Stops the queue's background thread and releases resources.
     *
     * <p>After calling {@code close()}, calls to {@link #publish} have no effect.
     */
    @Override
    void close();
}
