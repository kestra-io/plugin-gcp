package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DeleteBucketTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void forceFalse_shouldDeleteBucket() throws Exception {
        // given
        DeleteBucket task = Mockito.spy(DeleteBucket.builder()
            .id(DeleteBucket.class.getSimpleName())
            .type(DeleteBucket.class.getName())
            .name(Property.ofValue("my-bucket"))
            .force(Property.ofValue(false))
            .build());

        // Mock GCS Storage delete so no real bucket is touched.
        Storage storage = mock(Storage.class);
        doReturn(storage).when(task).connection(any(RunContext.class));
        when(storage.delete("my-bucket")).thenReturn(true);

        // Real (non-mocked) Kestra RunContext
        var runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        // when
        DeleteBucket.Output output = task.run(runContext);

        // then
        verify(storage, times(1)).delete("my-bucket");
        assertThat(output.getBucket(), is("my-bucket"));
        assertThat(output.getBucketUri(), is(new URI("gs://my-bucket")));
    }

    @Test
    void forceTrue_shouldCallDeleteListBeforeDeletingBucket() throws Exception {
        // given
        DeleteBucket task = Mockito.spy(DeleteBucket.builder()
            .id(DeleteBucket.class.getSimpleName())
            .type(DeleteBucket.class.getName())
            .name(Property.ofValue("my-bucket"))
            .force(Property.ofValue(true))
            .build());

        Storage storage = mock(Storage.class);
        doReturn(storage).when(task).connection(any(RunContext.class));
        when(storage.delete("my-bucket")).thenReturn(true);

        var runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        // Intercept DeleteList construction so we don't delete anything real.
        try (MockedConstruction<DeleteList> mocked = Mockito.mockConstruction(
            DeleteList.class,
            (mock, context) -> {
                when(mock.run(any(RunContext.class)))
                    .thenReturn(DeleteList.Output.builder().count(0).size(0).build());
            }
        )) {
            // when
            DeleteBucket.Output output = task.run(runContext);

            // then: DeleteList constructed once and executed once
            assertThat(mocked.constructed().size(), is(1));
            verify(mocked.constructed().getFirst(), times(1)).run(runContext);

            // and: bucket delete executed
            verify(storage, times(1)).delete("my-bucket");

            // and: output ok
            assertThat(output.getBucket(), is("my-bucket"));
            assertThat(output.getBucketUri(), is(new URI("gs://my-bucket")));
        }
    }

    @Test
    void shouldThrow404WhenBucketNotFound() throws Exception {
        // given
        DeleteBucket task = Mockito.spy(DeleteBucket.builder()
            .id(DeleteBucket.class.getSimpleName())
            .type(DeleteBucket.class.getName())
            .name(Property.ofValue("missing-bucket"))
            .force(Property.ofValue(false))
            .build());

        Storage storage = mock(Storage.class);
        doReturn(storage).when(task).connection(any(RunContext.class));
        when(storage.delete("missing-bucket")).thenReturn(false);

        var runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        // when / then
        StorageException ex = assertThrows(StorageException.class, () -> task.run(runContext));
        assertThat(ex.getCode(), is(404));
        assertThat(ex.getMessage(), containsString("Couldn't find bucket 'missing-bucket'"));
    }
}
