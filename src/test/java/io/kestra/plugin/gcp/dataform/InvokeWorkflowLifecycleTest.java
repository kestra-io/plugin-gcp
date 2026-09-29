package io.kestra.plugin.gcp.dataform;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataform.v1.CancelWorkflowInvocationRequest;
import com.google.cloud.dataform.v1.DataformClient;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class InvokeWorkflowLifecycleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(InvokeWorkflowLifecycleTest.class);

    @Test
    void killCancelsTrackedInvocation() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";

        task.trackInvocation(client, invocationName, LOGGER);
        task.kill();

        CancelWorkflowInvocationRequest expectedRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        verify(client, times(1)).cancelWorkflowInvocation(eq(expectedRequest));
    }

    @Test
    void secondKillIsNoOp() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";

        task.trackInvocation(client, invocationName, LOGGER);
        task.kill();
        task.kill();

        CancelWorkflowInvocationRequest expectedRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        verify(client, times(1)).cancelWorkflowInvocation(eq(expectedRequest));
    }

    @Test
    void killOnRetryTracksLatestInvocationOnly() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String staleInvocation = "projects/test/locations/us/repositories/repo/workflowInvocations/stale";
        String liveInvocation = "projects/test/locations/us/repositories/repo/workflowInvocations/live";

        task.trackInvocation(client, staleInvocation, LOGGER);
        task.trackInvocation(client, liveInvocation, LOGGER);
        task.kill();

        CancelWorkflowInvocationRequest liveRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(liveInvocation)
            .build();
        CancelWorkflowInvocationRequest staleRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(staleInvocation)
            .build();
        verify(client, times(1)).cancelWorkflowInvocation(eq(liveRequest));
        verify(client, times(0)).cancelWorkflowInvocation(eq(staleRequest));
    }

    @Test
    void killWithoutTrackedInvocationIsNoOp() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();

        assertDoesNotThrow(task::kill);
    }

    @Test
    void killSwallowsCancelException() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";
        CancelWorkflowInvocationRequest request = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        doThrow(new RuntimeException("API error")).when(client).cancelWorkflowInvocation(eq(request));

        task.trackInvocation(client, invocationName, LOGGER);

        assertDoesNotThrow(task::kill);
    }

    @Test
    void stopCancelsTrackedInvocation() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";

        task.trackInvocation(client, invocationName, LOGGER);
        task.stop();

        CancelWorkflowInvocationRequest expectedRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        verify(client, times(1)).cancelWorkflowInvocation(eq(expectedRequest));
    }

    @Test
    void secondStopIsNoOp() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";

        task.trackInvocation(client, invocationName, LOGGER);
        task.stop();
        task.stop();

        CancelWorkflowInvocationRequest expectedRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        verify(client, times(1)).cancelWorkflowInvocation(eq(expectedRequest));
    }

    @Test
    void stopAfterKillIsNoOp() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";

        task.trackInvocation(client, invocationName, LOGGER);
        task.kill();
        task.stop();

        CancelWorkflowInvocationRequest expectedRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        verify(client, times(1)).cancelWorkflowInvocation(eq(expectedRequest));
    }

    @Test
    void killAfterStopIsNoOp() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";

        task.trackInvocation(client, invocationName, LOGGER);
        task.stop();
        task.kill();

        CancelWorkflowInvocationRequest expectedRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        verify(client, times(1)).cancelWorkflowInvocation(eq(expectedRequest));
    }

    @Test
    void stopWithoutTrackedInvocationIsNoOp() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();

        assertDoesNotThrow(task::stop);
    }

    @Test
    void stopSwallowsCancelException() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";
        CancelWorkflowInvocationRequest request = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        doThrow(new RuntimeException("API error")).when(client).cancelWorkflowInvocation(eq(request));

        task.trackInvocation(client, invocationName, LOGGER);

        assertDoesNotThrow(task::stop);
    }

    @Test
    void killBeforeTrackingCancelsOnceTracked() {
        InvokeWorkflow task = InvokeWorkflow.builder().build();
        DataformClient client = mock(DataformClient.class);
        String invocationName = "projects/test/locations/us/repositories/repo/workflowInvocations/123";

        task.kill();
        task.trackInvocation(client, invocationName, LOGGER);
        task.kill();

        CancelWorkflowInvocationRequest expectedRequest = CancelWorkflowInvocationRequest.newBuilder()
            .setName(invocationName)
            .build();
        verify(client, times(1)).cancelWorkflowInvocation(eq(expectedRequest));
    }
}
