package io.kestra.plugin.gcp.bigquery;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TriggerKillTest {
    @Test
    void killForwardsToNestedQueryTask() throws Exception {
        Trigger task = Trigger.builder().build();
        Query query = mock(Query.class);

        trackedQueryTask(task).set(query);
        task.kill();

        verify(query, times(1)).kill();
    }

    @Test
    void killWithoutNestedQueryTaskIsNoOp() {
        Trigger task = Trigger.builder().build();

        assertDoesNotThrow(task::kill);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Query> trackedQueryTask(Trigger task) throws Exception {
        Field field = Trigger.class.getDeclaredField("queryTask");
        field.setAccessible(true);
        return (AtomicReference<Query>) field.get(task);
    }
}
