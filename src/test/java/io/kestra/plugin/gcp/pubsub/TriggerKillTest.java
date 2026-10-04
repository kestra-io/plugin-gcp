package io.kestra.plugin.gcp.pubsub;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TriggerKillTest {
    @Test
    void killForwardsToNestedConsumeTask() throws Exception {
        Trigger task = Trigger.builder().build();
        Consume consume = mock(Consume.class);

        trackedConsumeTask(task).set(consume);
        task.kill();

        verify(consume, times(1)).kill();
    }

    @Test
    void killWithoutNestedConsumeTaskIsNoOp() {
        Trigger task = Trigger.builder().build();

        assertDoesNotThrow(task::kill);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Consume> trackedConsumeTask(Trigger task) throws Exception {
        Field field = Trigger.class.getDeclaredField("consumeTask");
        field.setAccessible(true);
        return (AtomicReference<Consume>) field.get(task);
    }
}
