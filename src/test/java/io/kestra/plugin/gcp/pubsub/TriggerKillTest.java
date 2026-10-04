package io.kestra.plugin.gcp.pubsub;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
    void killStopsTrackedSubscriber() throws Exception {
        Trigger trigger = Trigger.builder().build();
        Consume consume = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        consume.trackConsumer(subscriber, latch, LOGGER);
        trackedConsumeTask(trigger).set(consume);

        trigger.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void secondKillDoesNotStopSubscriberAgain() throws Exception {
        Trigger trigger = Trigger.builder().build();
        Consume consume = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        consume.trackConsumer(subscriber, latch, LOGGER);
        trackedConsumeTask(trigger).set(consume);

        trigger.kill();
        trigger.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void killBeforeSubscriberIsTrackedIsSafe() throws Exception {
        Trigger trigger = Trigger.builder().build();
        Consume consume = Consume.builder().build();
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch latch = new CountDownLatch(1);

        trackedConsumeTask(trigger).set(consume);

        assertDoesNotThrow(trigger::kill);

        consume.trackConsumer(subscriber, latch, LOGGER);
        trigger.kill();

        verify(subscriber, times(1)).stopAsync();
        assertEquals(0, latch.getCount());
    }

    @Test
    void killWithoutNestedConsumeTaskIsNoOp() {
        Trigger trigger = Trigger.builder().build();

        assertDoesNotThrow(trigger::kill);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Consume> trackedConsumeTask(Trigger trigger) throws Exception {
        Field field = Trigger.class.getDeclaredField("consumeTask");
        field.setAccessible(true);
        return (AtomicReference<Consume>) field.get(trigger);
    }
}
