package io.kestra.plugin.gcp.compute;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.google.cloud.compute.v1.AccessConfig;
import com.google.cloud.compute.v1.Error;
import com.google.cloud.compute.v1.Errors;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.NetworkInterface;
import com.google.cloud.compute.v1.Operation;

import io.kestra.core.models.property.Property;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AbstractComputeTaskTest {

    private static Delete task() {
        return Delete.builder()
            .id("t")
            .type(Delete.class.getName())
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("vm"))
            .build();
    }

    @Test
    void killRunsRegisteredActionOnce() {
        var task = task();
        var count = new AtomicInteger();
        task.onKill(count::incrementAndGet);

        task.kill();
        task.kill();

        assertThat(count.get(), is(1));
    }

    @Test
    void onKillRunsImmediatelyWhenAlreadyKilled() {
        var task = task();
        task.kill();

        var count = new AtomicInteger();
        task.onKill(count::incrementAndGet);

        assertThat(count.get(), is(1));
    }

    @Test
    void killWithoutRegisteredActionIsNoOp() {
        assertDoesNotThrow(() -> task().kill());
    }

    @Test
    void externalIpReturnsFirstNatIp() {
        var instance = Instance.newBuilder()
            .addNetworkInterfaces(
                NetworkInterface.newBuilder()
                    .setNetworkIP("10.0.0.5")
                    .addAccessConfigs(AccessConfig.newBuilder().setNatIP("34.1.2.3").build())
                    .build()
            )
            .build();

        assertThat(AbstractComputeTask.externalIp(instance), is("34.1.2.3"));
        assertThat(AbstractComputeTask.internalIp(instance), is("10.0.0.5"));
    }

    @Test
    void externalIpIsNullWhenNoAccessConfig() {
        var instance = Instance.newBuilder()
            .addNetworkInterfaces(NetworkInterface.newBuilder().setNetworkIP("10.0.0.5").build())
            .build();

        assertThat(AbstractComputeTask.externalIp(instance), is(nullValue()));
        assertThat(AbstractComputeTask.internalIp(instance), is("10.0.0.5"));
    }

    @Test
    void checkOperationErrorPassesWhenNoError() {
        var operation = Operation.newBuilder().setStatus(Operation.Status.DONE).build();

        AbstractComputeTask.checkOperationError(operation);
    }

    @Test
    void checkOperationErrorThrowsWhenOperationFailed() {
        var operation = Operation.newBuilder()
            .setError(
                Error.newBuilder()
                    .addErrors(Errors.newBuilder().setCode("QUOTA_EXCEEDED").setMessage("Not enough quota").build())
                    .build()
            )
            .build();

        var exception = assertThrows(IllegalStateException.class, () -> AbstractComputeTask.checkOperationError(operation));
        assertThat(exception.getMessage(), is("Compute Engine operation failed: QUOTA_EXCEEDED: Not enough quota"));
    }
}
