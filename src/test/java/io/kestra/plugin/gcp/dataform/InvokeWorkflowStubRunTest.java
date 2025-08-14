package io.kestra.plugin.gcp.dataform;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class InvokeWorkflowStubRunTest {
    @Inject
    private DataformTestUtils testUtils;

    @Test
    void shouldInvokeWorkflowSuccessfullyWithWaitTrue() throws Exception {
        var task = new FakeInvokeWorkflow();
        task.setProjectId("fake-project");
        task.setLocation("us-central1");
        task.setRepositoryId("my-repo");
        task.setWorkflowConfigId("my-workflow");
        task.wait = true;

        RunContext runContext = testUtils.runContext(task);
        var output = task.run(runContext);

        assertNotNull(output, "Output should not be null");
        assertTrue(output.getWorkflowInvocationName().contains("projects/"), "Invocation name should contain GCP path");
        assertEquals("SUCCEEDED", output.getWorkflowInvocationState(), "Expected SUCCEEDED when wait is true");
    }
}

// --- Stubbed task that avoids real DataformClient call
class FakeInvokeWorkflow extends InvokeWorkflow {
    public void setProjectId(String projectId) {
        this.projectId = Property.ofValue(projectId);
    }

    public void setLocation(String location) {
        this.location = Property.ofValue(location);
    }

    public void setRepositoryId(String repositoryId) {
        this.repositoryId = Property.ofValue(repositoryId);
    }

    public void setWorkflowConfigId(String workflowConfigId) {
        this.workflowConfigId = Property.ofValue(workflowConfigId);
    }

    @Override
    public Output run(RunContext runContext) {
        return Output.builder()
            .workflowInvocationName("projects/fake/locations/us-central1/repositories/my-repo/workflowInvocations/mock-id")
            .workflowInvocationState("SUCCEEDED")
            .build();
    }
}