package io.kestra.plugin.gcp.dataform;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class InvokeWorkflowTest {
    @Inject
    private DataformTestUtils testUtils;

    @Value("${kestra.tasks.dataform.repositoryId}")
    String repositoryId;

    @Value("${kestra.tasks.dataform.workflowConfigId}")
    String workflowConfigId;

    @Test
    @Order(1)
    void shouldInvokeWorkflowWithWaitTrue() throws Exception {
        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, true);
        RunContext runContext = testUtils.runContext(task);
        var output = task.run(runContext);

        assertNotNull(output, "Output should not be null");
        assertTrue(output.getWorkflowInvocationName().contains("projects/"), "Invocation name should contain GCP path");
        assertEquals("SUCCEEDED", output.getWorkflowInvocationState(), "Unexpected workflow state for wait=true");
    }

    @Test
    @Order(2)
    void shouldInvokeWorkflowWithWaitFalse() throws Exception {
        var task = testUtils.defaultInvokeWorkflowTask(repositoryId, workflowConfigId, false);
        RunContext runContext = testUtils.runContext(task);
        var output = task.run(runContext);

        assertNotNull(output, "Output should not be null");
        assertTrue(output.getWorkflowInvocationName().contains("projects/"), "Invocation name should contain GCP path");
        assertEquals("RUNNING", output.getWorkflowInvocationState(), "Unexpected workflow state for wait=false");
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
        assertTrue(exception.getMessage().contains("nonexistent-config does not exist") || exception.getMessage().contains("NOT_FOUND"), "Expected failure for invalid config");
    }
}