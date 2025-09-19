package io.kestra.plugin.gcp.dataform;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class InvokeWorkflowTest {
    @Inject
    private DataformTestUtils testUtils;

    @Value("${kestra.tasks.dataform.project}")
    String project;

    @Value("${kestra.tasks.dataform.region}")
    String region;

    @Value("${kestra.tasks.dataform.repositoryId}")
    String repositoryId;

    @Value("${kestra.tasks.dataform.workflowConfigId}")
    String workflowConfigId;

    @ParameterizedTest
    @CsvSource({
        "true, SUCCEEDED",
        "false, RUNNING"
    })
    void shouldInvokeWorkflowWithDifferentWaitValues(boolean wait, String expectedState) throws Exception {
        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);
        var output = task.run(runContext);

        assertNotNull(output, "Output should not be null");
        assertTrue(output.getWorkflowInvocationName().contains("projects/"), "Invocation name should contain GCP path");
        assertEquals(expectedState, output.getWorkflowInvocationState(), "Unexpected workflow state for wait=" + wait);
    }

    @Test
    void shouldFailWithInvalidRepositoryId() {
        String repositoryId = "nonexistent-repo";
        boolean wait = true;

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));
        assertTrue(exception.getMessage().contains("not found") || exception.getMessage().contains("404"), "Expected failure for invalid repository");
    }

    @Test
    void shouldFailWithInvalidWorkflowConfigId() {
        String workflowConfigId = "nonexistent-config";
        boolean wait = true;

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);

        Exception exception = assertThrows(Exception.class, () -> task.run(runContext));
        assertTrue(exception.getMessage().contains("not found") || exception.getMessage().contains("404"), "Expected failure for invalid config");
    }

    @Test
    void shouldReturnNullStateIfWaitFalseAndStateUnfetched() throws Exception {
        // Simulate scenario where wait=false and the backend doesn't return state yet
        boolean wait = false;

        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, wait);
        RunContext runContext = testUtils.runContext(task);
        var output = task.run(runContext);

        assertNotNull(output, "Output should not be null");
        assertNotNull(output.getWorkflowInvocationName(), "Workflow name should not be null");
        assertEquals("RUNNING", output.getWorkflowInvocationState(), "If wait is false, state should be RUNNING (not polled)");
    }
}