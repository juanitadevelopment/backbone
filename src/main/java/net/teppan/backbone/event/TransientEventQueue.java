package net.teppan.backbone.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * An in-memory {@link EventQueue} backed by a {@link LinkedBlockingQueue}.
 *
 * <p>Events are delivered to subscribers on a single virtual-thread worker.
 * All events are lost when the queue is closed or the process restarts.
 *
 * <pre>{@code
 * var queue = new TransientEventQueue<OrderCreated>();
 * queue.subscribe(event -> System.out.println("Order: " + event.orderId()));
 * queue.publish(new OrderCreated("o-1"));
 * }</pre>
 *
 * @param <E> the event type
 */
public final class TransientEventQueue<E> implements EventQueue<E> {

    private static final Logger log = LoggerFactory.getLogger(TransientEventQueue.class);

    private final LinkedBlockingQueue<E> queue = new LinkedBlockingQueue<>();
    private final List<Consumer<E>> listeners  = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;
    private final Thread worker;

    /**
     * Creates and starts a {@code TransientEventQueue}.
     */
    public TransientEventQueue() {
        worker = Thread.ofVirtual()
            .name("transient-event-worker")
            .start(this::deliveryLoop);
    }

    @Override
    public void publish(E event) {
        if (!running) return;
        queue.offer(event);
    }

    @Override
    public void subscribe(Consumer<E> listener) {
        listeners.add(listener);
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public void close() {
        running = false;
        worker.interrupt();
    }

    private void deliveryLoop() {
        while (running || !queue.isEmpty()) {
            try {
                E event = queue.poll(100, TimeUnit.MILLISECONDS);
                if (event != null) {
                    dispatch(event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // drain remaining events after close
        E event;
        while ((event = queue.poll()) != null) {
            dispatch(event);
        }
    }

    private void dispatch(E event) {
        for (Consumer<E> listener : listeners) {
            try {
                listener.accept(event);
            } catch (Exception e) {
                log.warn("Event listener threw", e);
            }
        }
    }
}
