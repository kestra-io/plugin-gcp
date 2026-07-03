package io.kestra.plugin.gcp.bigquery;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CopyPartitionsKillTest {
    @Test
    void killForwardsToNestedCopyTask() throws Exception {
        CopyPartitions task = CopyPartitions.builder().build();
        Copy copy = mock(Copy.class);

        trackedCopyTask(task).set(copy);
        task.kill();

        verify(copy, times(1)).kill();
    }

    @Test
    void killWithoutNestedCopyTaskIsNoOp() {
        CopyPartitions task = CopyPartitions.builder().build();

        assertDoesNotThrow(task::kill);
    }

    @Test
    void stopForwardsToNestedCopyTask() throws Exception {
        CopyPartitions task = CopyPartitions.builder().build();
        Copy copy = mock(Copy.class);

        trackedCopyTask(task).set(copy);
        task.stop();

        verify(copy, times(1)).stop();
    }

    @Test
    void stopWithoutNestedCopyTaskIsNoOp() {
        CopyPartitions task = CopyPartitions.builder().build();

        assertDoesNotThrow(task::stop);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Copy> trackedCopyTask(CopyPartitions task) throws Exception {
        Field field = CopyPartitions.class.getDeclaredField("copyTask");
        field.setAccessible(true);
        return (AtomicReference<Copy>) field.get(task);
    }
}
