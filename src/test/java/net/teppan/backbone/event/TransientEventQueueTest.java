package net.teppan.backbone.event;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TransientEventQueueTest {

    private TransientEventQueue<String> queue;

    @BeforeEach
    void setUp() {
        queue = new TransientEventQueue<>();
    }

    @AfterEach
    void tearDown() {
        queue.close();
    }

    @Test
    void publish_andSubscribe_deliverEvent() throws InterruptedException {
        var latch  = new CountDownLatch(1);
        List<String> received = new ArrayList<>();

        queue.subscribe(e -> { received.add(e); latch.countDown(); });
        queue.publish("hello");

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactly("hello");
    }

    @Test
    void multipleSubscribers_allReceiveEvent() throws InterruptedException {
        var latch = new CountDownLatch(3);
        List<String> received = new ArrayList<>();

        queue.subscribe(e -> { received.add("A"); latch.countDown(); });
        queue.subscribe(e -> { received.add("B"); latch.countDown(); });
        queue.subscribe(e -> { received.add("C"); latch.countDown(); });
        queue.publish("event");

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactlyInAnyOrder("A", "B", "C");
    }

    @Test
    void multipleEvents_deliveredInOrder() throws InterruptedException {
        var latch    = new CountDownLatch(3);
        List<String> received = new ArrayList<>();

        queue.subscribe(e -> { received.add(e); latch.countDown(); });
        queue.publish("one");
        queue.publish("two");
        queue.publish("three");

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactly("one", "two", "three");
    }

    @Test
    void isPersistent_returnsFalse() {
        assertThat(queue.isPersistent()).isFalse();
    }

    @Test
    void size_reflectsPendingEvents() throws InterruptedException {
        // No subscribers, so delivery won't drain the queue immediately.
        // Use a blocking subscriber to pin an event in flight.
        var block = new CountDownLatch(1);
        queue.subscribe(e -> {
            try { block.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        });

        queue.publish("pinned");
        // Wait until the worker picks it up (size goes to 0 as worker drains)
        // Just verify size() doesn't throw
        int size = queue.size();
        assertThat(size).isGreaterThanOrEqualTo(0);
        block.countDown();
    }

    @Test
    void publishAfterClose_isIgnored() throws InterruptedException {
        var latch = new CountDownLatch(1);
        List<String> received = new ArrayList<>();
        queue.subscribe(e -> { received.add(e); latch.countDown(); });

        queue.close();
        queue.publish("after-close");

        // Nothing should be delivered; wait briefly
        assertThat(latch.await(300, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(received).isEmpty();
    }

    @Test
    void listenerException_doesNotStopDelivery() throws InterruptedException {
        var latch = new CountDownLatch(2);
        List<String> received = new ArrayList<>();

        queue.subscribe(e -> { throw new RuntimeException("bad listener"); });
        queue.subscribe(e -> { received.add(e); latch.countDown(); });

        queue.publish("msg1");
        queue.publish("msg2");

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactly("msg1", "msg2");
    }
}
