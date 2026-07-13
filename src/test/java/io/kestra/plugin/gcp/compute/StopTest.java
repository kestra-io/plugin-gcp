package io.kestra.plugin.gcp.compute;

import java.time.Duration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.api.gax.longrunning.OperationFuture;
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
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@KestraTest
public class StopTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.dataproc.project}")
    private String project;

    @Test
    void defaultsToWaitingWithATenMinuteTimeout() throws Exception {
        var stop = Stop.builder()
            .id("compute-stop-" + FriendlyId.createFriendlyId())
            .type(Stop.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("my-batch-vm"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, stop, ImmutableMap.of());

        assertThat(runContext.render(stop.getWaitUntilStopped()).as(Boolean.class).orElseThrow(), is(true));
        assertThat(runContext.render(stop.getTimeout()).as(Duration.class).orElseThrow(), is(Duration.ofMinutes(10)));
    }

    @Test
    void runWaitsAndReturnsInstanceOutput() throws Exception {
        var stop = spy(
            Stop.builder()
                .id("compute-stop")
                .type(Stop.class.getName())
                .projectId(Property.ofValue("my-project"))
                .zone(Property.ofValue("us-central1-a"))
                .instanceName(Property.ofValue("vm1"))
                .build()
        );

        var client = mock(InstancesClient.class);
        @SuppressWarnings("unchecked")
        OperationFuture<Operation, Operation> future = mock(OperationFuture.class);
        when(future.get(anyLong(), any())).thenReturn(Operation.newBuilder().build());
        when(client.stopAsync("my-project", "us-central1-a", "vm1")).thenReturn(future);
        when(client.get("my-project", "us-central1-a", "vm1")).thenReturn(
            Instance.newBuilder().setName("vm1").setId(123L).setStatus("TERMINATED").build()
        );

        doReturn(mock(GoogleCredentials.class)).when(stop).credentials(any());
        doReturn(client).when(stop).instancesClient(any());

        var runContext = TestsUtils.mockRunContext(runContextFactory, stop, ImmutableMap.of());
        var output = stop.run(runContext);

        assertThat(output.getInstanceName(), is("vm1"));
        assertThat(output.getStatus(), is("TERMINATED"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    @Disabled
    void run() throws Exception {
        var stop = Stop.builder()
            .id(Stop.class.getSimpleName())
            .type(Stop.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("my-batch-vm"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, stop, ImmutableMap.of());

        Stop.Output output = stop.run(runContext);
        assertThat(output.getStatus(), notNullValue());
    }
}
