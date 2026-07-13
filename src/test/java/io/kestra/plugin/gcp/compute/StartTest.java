package io.kestra.plugin.gcp.compute;

import java.time.Duration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
public class StartTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.dataproc.project}")
    private String project;

    @Test
    void defaultsToWaitingWithATenMinuteTimeout() throws Exception {
        var start = Start.builder()
            .id("compute-start-" + FriendlyId.createFriendlyId())
            .type(Start.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("my-batch-vm"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, start, ImmutableMap.of());

        assertThat(runContext.render(start.getWaitUntilRunning()).as(Boolean.class).orElseThrow(), is(true));
        assertThat(runContext.render(start.getTimeout()).as(Duration.class).orElseThrow(), is(Duration.ofMinutes(10)));
    }

    @Test
    void runWithoutWaitingReturnsStagingWithoutQueryingInstance() throws Exception {
        var start = spy(
            Start.builder()
                .id("compute-start")
                .type(Start.class.getName())
                .projectId(Property.ofValue("my-project"))
                .zone(Property.ofValue("us-central1-a"))
                .instanceName(Property.ofValue("vm1"))
                .waitUntilRunning(Property.ofValue(false))
                .build()
        );

        var client = mock(InstancesClient.class);
        @SuppressWarnings("unchecked")
        OperationFuture<Operation, Operation> future = mock(OperationFuture.class);
        when(client.startAsync("my-project", "us-central1-a", "vm1")).thenReturn(future);

        doReturn(mock(GoogleCredentials.class)).when(start).credentials(any());
        doReturn(client).when(start).instancesClient(any());

        var runContext = TestsUtils.mockRunContext(runContextFactory, start, ImmutableMap.of());
        var output = start.run(runContext);

        assertThat(output.getInstanceName(), is("vm1"));
        assertThat(output.getStatus(), is("STAGING"));
        verify(client, never()).get(anyString(), anyString(), anyString());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    @Disabled("Starts a real billable Compute Engine VM, run manually against GCP")
    void run() throws Exception {
        var start = Start.builder()
            .id(Start.class.getSimpleName())
            .type(Start.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("my-batch-vm"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, start, ImmutableMap.of());

        Start.Output output = start.run(runContext);
        assertThat(output.getStatus(), notNullValue());
    }
}
