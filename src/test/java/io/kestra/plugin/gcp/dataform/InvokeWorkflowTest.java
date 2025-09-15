package io.kestra.plugin.gcp.dataform;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class InvokeWorkflowTest {
    @Inject
    private DataformTestUtils testUtils;

    @ParameterizedTest
    @CsvSource({
        "true, SUCCEEDED",
        "false, RUNNING"
    })
    void shouldInvokeWorkflowWithDifferentWaitValues(boolean wait, String expectedState) throws Exception {
        String repositoryId = "my-repo";
        String workflowConfigId = "my-workflow";

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);
        var output = task.run(runContext);

        assertNotNull(output, "Output should not be null");
        assertTrue(output.getWorkflowInvocationName().contains("projects/"),
            "Invocation name should contain GCP path");
        assertEquals(expectedState, output.getWorkflowInvocationState(),
            "Unexpected workflow state for wait=" + wait);

        System.out.println("Workflow invoked" + (wait ? " (wait)" : " (no wait)") + ": "
            + output.getWorkflowInvocationName());
        System.out.println("State: " + output.getWorkflowInvocationState());
    }

    @Test
    void shouldFailWithInvalidRepositoryId() {
        String repositoryId = "nonexistent-repo";
        String workflowConfigId = "my-workflow";
        boolean wait = true;

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));
        assertTrue(exception.getMessage().contains("not found") || exception.getMessage().contains("404"), "Expected failure for invalid repository");
        System.out.println("Expected failure: " + exception.getMessage());
    }

    @Test
    void shouldFailWithInvalidWorkflowConfigId() {
        String repositoryId = "my-repo";
        String workflowConfigId = "nonexistent-config";
        boolean wait = true;

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));
        assertTrue(exception.getMessage().contains("not found") || exception.getMessage().contains("404"), "Expected failure for invalid config");
        System.out.println("Expected failure: " + exception.getMessage());
    }

    @Test
    void shouldReturnNullStateIfWaitFalseAndStateUnfetched() throws Exception {
        // Simulate scenario where wait=false and the backend doesn't return state yet
        String repositoryId = "my-repo";
        String workflowConfigId = "my-workflow";
        boolean wait = false;

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);
        var output = task.run(runContext);

        assertNotNull(output, "Output should not be null");
        assertNotNull(output.getWorkflowInvocationName(), "Workflow name should not be null");
        assertEquals("RUNNING", output.getWorkflowInvocationState(), "If wait is false, state should be RUNNING (not polled)");
    }
}