package io.kestra.plugin.gcp.dataform;

import com.google.cloud.dataform.v1.CreateWorkflowInvocationRequest;
import com.google.cloud.dataform.v1.DataformClient;
import com.google.cloud.dataform.v1.WorkflowInvocation;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@KestraTest
class InvokeWorkflowStubRunTest {
    @Inject
    private DataformTestUtils testUtils;

    @Test
    void shouldInvokeWorkflowSuccessfullyWithWaitTrue() throws Exception {
        DataformClient mockClient = mock(DataformClient.class);
        when(mockClient.createWorkflowInvocation(any(CreateWorkflowInvocationRequest.class)))
            .thenReturn(WorkflowInvocation.newBuilder()
                .setName("projects/fake/locations/us-central1/repositories/my-repo/workflowInvocations/mock-id")
                .setState(WorkflowInvocation.State.SUCCEEDED)
                .build());

        InvokeWorkflow task = spy(InvokeWorkflow.builder()
            .projectId(Property.ofValue("fake-project"))
            .location(Property.ofValue("us-central1"))
            .repositoryId(Property.ofValue("my-repo"))
            .workflowConfigId(Property.ofValue("my-workflow"))
            .wait(true)
            .build());

        doReturn(mockClient).when(task).dataformClient(any());

        RunContext runContext = testUtils.runContext(task);
        InvokeWorkflow.Output output = task.run(runContext);

        assertNotNull(output);
        assertTrue(output.getWorkflowInvocationName().contains("projects/"));
        assertEquals("SUCCEEDED", output.getWorkflowInvocationState());
    }
}
