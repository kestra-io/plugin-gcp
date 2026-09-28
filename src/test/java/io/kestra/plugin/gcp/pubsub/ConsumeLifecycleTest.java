package io.kestra.plugin.gcp.pubsub;

import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsub.v1.Subscriber;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ConsumeLifecycleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeLifecycleTest.class);

    @Test
    void killStopsSubscriberAndCountsDownLatch() {
        Consume task = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        task.trackConsumer(subscriber, latch, LOGGER);
        task.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void secondKillIsNoOp() {
        Consume task = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        task.trackConsumer(subscriber, latch, LOGGER);
        task.kill();
        task.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void killWithoutTrackedConsumerIsNoOp() {
        Consume task = Consume.builder().build();

        assertDoesNotThrow(task::kill);
    }

    @Test
    void killSwallowsSubscriberStopException() {
        Consume task = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);
        doThrow(new RuntimeException("subscriber stop error")).when(subscriber).stopAsync();

        task.trackConsumer(subscriber, latch, LOGGER);

        assertDoesNotThrow(task::kill);
        assertEquals(0, latch.getCount());
    }

    @Test
    void stopStopsSubscriberAndCountsDownLatch() {
        Consume task = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        task.trackConsumer(subscriber, latch, LOGGER);
        task.stop();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void secondStopIsNoOp() {
        Consume task = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        task.trackConsumer(subscriber, latch, LOGGER);
        task.stop();
        task.stop();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void stopAfterKillIsNoOp() {
        Consume task = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        task.trackConsumer(subscriber, latch, LOGGER);
        task.kill();
        task.stop();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void killAfterStopIsNoOp() {
        Consume task = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        task.trackConsumer(subscriber, latch, LOGGER);
        task.stop();
        task.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void stopWithoutTrackedConsumerIsNoOp() {
        Consume task = Consume.builder().build();

        assertDoesNotThrow(task::stop);
    }

    @Test
    void stopSwallowsSubscriberStopException() {
        Consume task = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);
        doThrow(new RuntimeException("subscriber stop error")).when(subscriber).stopAsync();

        task.trackConsumer(subscriber, latch, LOGGER);

        assertDoesNotThrow(task::stop);
        assertEquals(0, latch.getCount());
    }
}
