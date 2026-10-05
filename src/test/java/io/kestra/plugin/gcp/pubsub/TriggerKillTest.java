package io.kestra.plugin.gcp.pubsub;

import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsub.v1.Subscriber;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TriggerKillTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerKillTest.class);

    @Test
    void killStopsTrackedSubscriber() {
        Trigger trigger = Trigger.builder().build();
        Consume consume = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        consume.trackConsumer(subscriber, latch, LOGGER);
        trigger.trackTask(consume);

        trigger.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void secondKillDoesNotStopSubscriberAgain() {
        Trigger trigger = Trigger.builder().build();
        Consume consume = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        consume.trackConsumer(subscriber, latch, LOGGER);
        trigger.trackTask(consume);

        trigger.kill();
        trigger.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void killBeforeSubscriberIsTrackedIsSafe() {
        Trigger trigger = Trigger.builder().build();
        Consume consume = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        trigger.trackTask(consume);

        assertDoesNotThrow(trigger::kill);

        consume.trackConsumer(subscriber, latch, LOGGER);
        trigger.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void stopStopsTrackedSubscriber() {
        Trigger trigger = Trigger.builder().build();
        Consume consume = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        consume.trackConsumer(subscriber, latch, LOGGER);
        trigger.trackTask(consume);

        trigger.stop();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void completedPollIsNotStoppedByLaterKill() {
        Trigger trigger = Trigger.builder().build();
        Consume consume = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        consume.trackConsumer(subscriber, latch, LOGGER);
        trigger.trackTask(consume);

        trigger.clearTask(consume);
        trigger.kill();

        verify(subscriber, times(0)).stopAsync();
        assertEquals(1, latch.getCount());
    }

    @Test
    void killWithoutNestedConsumeTaskIsNoOp() {
        Trigger trigger = Trigger.builder().build();

        assertDoesNotThrow(trigger::kill);
    }
}
