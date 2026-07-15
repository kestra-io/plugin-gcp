package io.kestra.plugin.gcp.compute;

import java.time.Duration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.Operation;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
public class DeleteTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.dataproc.project}")
    private String project;

    @Test
    void defaultsToWaitingWithATenMinuteTimeout() throws Exception {
        var delete = Delete.builder()
            .id("compute-delete-" + FriendlyId.createFriendlyId())
            .type(Delete.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("kestra-job-42"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of());

        assertThat(runContext.render(delete.getWaitUntilDeleted()).as(Boolean.class).orElseThrow(), is(true));
        assertThat(runContext.render(delete.getTimeout()).as(Duration.class).orElseThrow(), is(Duration.ofMinutes(10)));
    }

    @Test
    void runWaitsAndReturnsDeleted() throws Exception {
        var delete = spy(
            Delete.builder()
                .id("compute-delete")
                .type(Delete.class.getName())
                .projectId(Property.ofValue("my-project"))
                .zone(Property.ofValue("us-central1-a"))
                .instanceName(Property.ofValue("vm1"))
                .build()
        );

        var client = mock(InstancesClient.class);
        @SuppressWarnings("unchecked")
        OperationFuture<Operation, Operation> future = mock(OperationFuture.class);
        when(future.get(anyLong(), any())).thenReturn(Operation.newBuilder().build());
        when(client.get("my-project", "us-central1-a", "vm1")).thenReturn(
            Instance.newBuilder().setName("vm1").setId(123L).setStatus("RUNNING").build()
        );
        when(client.deleteAsync("my-project", "us-central1-a", "vm1")).thenReturn(future);

        doReturn(mock(GoogleCredentials.class)).when(delete).credentials(any());
        doReturn(client).when(delete).instancesClient(any());

        var runContext = TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of());
        var output = delete.run(runContext);

        assertThat(output.getInstanceName(), is("vm1"));
        assertThat(output.getInstanceId(), is("123"));
        assertThat(output.getStatus(), is("DELETED"));
    }

    @Test
    void runIsIdempotentWhenInstanceAlreadyGone() throws Exception {
        var delete = spy(
            Delete.builder()
                .id("compute-delete")
                .type(Delete.class.getName())
                .projectId(Property.ofValue("my-project"))
                .zone(Property.ofValue("us-central1-a"))
                .instanceName(Property.ofValue("vm1"))
                .build()
        );

        var client = mock(InstancesClient.class);
        when(client.get("my-project", "us-central1-a", "vm1")).thenThrow(mock(NotFoundException.class));

        doReturn(mock(GoogleCredentials.class)).when(delete).credentials(any());
        doReturn(client).when(delete).instancesClient(any());

        var runContext = TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of());
        var output = delete.run(runContext);

        assertThat(output.getInstanceName(), is("vm1"));
        assertThat(output.getInstanceId(), is(nullValue()));
        assertThat(output.getStatus(), is("DELETED"));
        // Must not attempt the delete RPC when the instance is already gone.
        verify(client, never()).deleteAsync(anyString(), anyString(), anyString());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    @Disabled("Requires a real Compute Engine VM, run manually against GCP")
    void run() throws Exception {
        var delete = Delete.builder()
            .id(Delete.class.getSimpleName())
            .type(Delete.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("kestra-job-42"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, delete, ImmutableMap.of());

        Delete.Output output = delete.run(runContext);
        assertThat(output.getStatus(), is("DELETED"));
    }
}
