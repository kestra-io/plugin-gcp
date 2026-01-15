package io.kestra.plugin.gcp.dataform;

import com.google.cloud.dataform.v1.DataformClient;
import com.google.cloud.dataform.v1.WorkflowInvocation;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@KestraTest
class InvokeWorkflowTest {
    @Inject
    private DataformTestUtils testUtils;

    @Value("${kestra.tasks.dataform.repositoryId}")
    String repositoryId;

    @Value("${kestra.tasks.dataform.workflowConfigId}")
    String workflowConfigId;

    @Test
    void shouldInvokeWorkflowWithWaitTrue() throws Exception {
        DataformClient client = mock(DataformClient.class);

        WorkflowInvocation running = WorkflowInvocation.newBuilder()
            .setName("projects/test/locations/us/repositories/repo/workflowInvocations/123")
            .setState(WorkflowInvocation.State.RUNNING)
            .build();

        WorkflowInvocation succeeded = WorkflowInvocation.newBuilder()
            .setName(running.getName())
            .setState(WorkflowInvocation.State.SUCCEEDED)
            .build();

        when(client.createWorkflowInvocation(any())).thenReturn(running);

        when(client.getWorkflowInvocation(running.getName())).thenReturn(succeeded);

        InvokeWorkflow task = spy(testUtils.defaultInvokeWorkflowTask("repo", "config", true));

        doReturn(client).when(task).createClient(any());

        RunContext runContext = testUtils.runContext(task);
        InvokeWorkflow.Output output = task.run(runContext);

        assertEquals("SUCCEEDED", output.getWorkflowInvocationState());
    }

    @Test
    void shouldInvokeWorkflowWithWaitFalse() throws Exception {
        DataformClient client = mock(DataformClient.class);

        WorkflowInvocation running = WorkflowInvocation.newBuilder()
            .setName("projects/test/locations/us/repositories/repo/workflowInvocations/123")
            .setState(WorkflowInvocation.State.RUNNING)
            .build();

        when(client.createWorkflowInvocation(any())).thenReturn(running);

        InvokeWorkflow task = spy(testUtils.defaultInvokeWorkflowTask("repo", "config", false));

        doReturn(client).when(task).createClient(any());

        RunContext runContext = testUtils.runContext(task);
        InvokeWorkflow.Output output = task.run(runContext);

        assertEquals("RUNNING", output.getWorkflowInvocationState());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    void shouldFailWithInvalidRepositoryId() {
        String repositoryId = "nonexistent-repo";
        boolean wait = true;

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));
        assertTrue(exception.getMessage().contains("not found") || exception.getMessage().contains("404"), "Expected failure for invalid repository");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    void shouldFailWithInvalidWorkflowConfigId() {
        String workflowConfigId = "nonexistent-config";
        boolean wait = true;

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));
        assertTrue(exception.getMessage().contains("nonexistent-config does not exist") || exception.getMessage().contains("NOT_FOUND"), "Expected failure for invalid config");
    }
}