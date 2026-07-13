package io.kestra.plugin.gcp.compute;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.compute.v1.AccessConfig;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.NetworkInterface;
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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@KestraTest
public class CreateTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.dataproc.project}")
    private String project;

    @Test
    void buildsInstanceResourceFromRenderedProperties() throws Exception {
        var create = Create.builder()
            .id("compute-create-" + FriendlyId.createFriendlyId())
            .type(Create.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("kestra-test-vm"))
            .machineType(Property.ofValue("n1-standard-1"))
            .sourceImage(Property.ofValue("projects/debian-cloud/global/images/family/debian-11"))
            .tags(Property.ofValue(List.of("kestra", "batch")))
            .metadata(Property.ofValue(Map.of("env", "test")))
            .startupScript(Property.ofValue("echo hello"))
            .diskSizeGb(Property.ofValue(50))
            .diskType(Property.ofValue("pd-ssd"))
            .preemptible(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());

        var instance = create.buildInstanceResource(runContext, "us-central1-a", "kestra-test-vm");

        assertThat(instance.getName(), is("kestra-test-vm"));
        assertThat(instance.getMachineType(), is("zones/us-central1-a/machineTypes/n1-standard-1"));
        assertThat(instance.getScheduling().getPreemptible(), is(true));

        assertThat(instance.getDisksList(), hasSize(1));
        var disk = instance.getDisks(0);
        assertThat(disk.getBoot(), is(true));
        assertThat(disk.getAutoDelete(), is(true));
        assertThat(disk.getInitializeParams().getSourceImage(), is("projects/debian-cloud/global/images/family/debian-11"));
        assertThat(disk.getInitializeParams().getDiskSizeGb(), is(50L));
        assertThat(disk.getInitializeParams().getDiskType(), is("zones/us-central1-a/diskTypes/pd-ssd"));

        assertThat(instance.getNetworkInterfacesList(), hasSize(1));
        var networkInterface = instance.getNetworkInterfaces(0);
        assertThat(networkInterface.getNetwork(), is("global/networks/default"));
        assertThat(networkInterface.getAccessConfigsList(), hasSize(1));

        assertThat(instance.getTags().getItemsList(), is(List.of("kestra", "batch")));

        var metadataItems = instance.getMetadata().getItemsList();
        assertThat(metadataItems.stream().anyMatch(item -> item.getKey().equals("env") && item.getValue().equals("test")), is(true));
        assertThat(metadataItems.stream().anyMatch(item -> item.getKey().equals("startup-script") && item.getValue().equals("echo hello")), is(true));
    }

    @Test
    void buildsMinimalInstanceResourceWithDefaults() throws Exception {
        var create = Create.builder()
            .id("compute-create-" + FriendlyId.createFriendlyId())
            .type(Create.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("kestra-test-vm"))
            .machineType(Property.ofValue("n1-standard-1"))
            .sourceImage(Property.ofValue("projects/debian-cloud/global/images/family/debian-11"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());

        var instance = create.buildInstanceResource(runContext, "us-central1-a", "kestra-test-vm");

        assertThat(instance.getTags().getItemsList(), hasSize(0));
        assertThat(instance.getMetadata().getItemsList(), hasSize(0));
        assertThat(instance.getScheduling().getPreemptible(), is(false));
        assertThat(instance.getNetworkInterfaces(0).getNetwork(), is("global/networks/default"));
    }

    @Test
    void doesNotForceDefaultNetworkWhenOnlySubnetworkIsSet() throws Exception {
        // On projects with a custom subnet-mode network, forcing the default network alongside a subnetwork is
        // rejected by GCP. When only the subnetwork is set, the network must be left unset so GCP infers it.
        var create = Create.builder()
            .id("compute-create-" + FriendlyId.createFriendlyId())
            .type(Create.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("kestra-test-vm"))
            .machineType(Property.ofValue("n1-standard-1"))
            .sourceImage(Property.ofValue("projects/debian-cloud/global/images/family/debian-11"))
            .subnetworkName(Property.ofValue("regions/us-central1/subnetworks/my-subnet"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());

        var networkInterface = create.buildInstanceResource(runContext, "us-central1-a", "kestra-test-vm")
            .getNetworkInterfaces(0);

        assertThat(networkInterface.getSubnetwork(), is("regions/us-central1/subnetworks/my-subnet"));
        assertThat(networkInterface.getNetwork(), is(""));
    }

    @Test
    void runWaitsAndReturnsInstanceOutput() throws Exception {
        var create = spy(
            Create.builder()
                .id("compute-create")
                .type(Create.class.getName())
                .projectId(Property.ofValue("my-project"))
                .zone(Property.ofValue("us-central1-a"))
                .instanceName(Property.ofValue("vm1"))
                .machineType(Property.ofValue("e2-micro"))
                .sourceImage(Property.ofValue("projects/debian-cloud/global/images/family/debian-12"))
                .build()
        );

        var client = mock(InstancesClient.class);
        @SuppressWarnings("unchecked")
        OperationFuture<Operation, Operation> future = mock(OperationFuture.class);
        when(future.get(anyLong(), any())).thenReturn(Operation.newBuilder().build());
        when(client.insertAsync(anyString(), anyString(), any(Instance.class))).thenReturn(future);
        when(client.get("my-project", "us-central1-a", "vm1")).thenReturn(
            Instance.newBuilder()
                .setName("vm1")
                .setId(123L)
                .setStatus("RUNNING")
                .addNetworkInterfaces(
                    NetworkInterface.newBuilder()
                        .setNetworkIP("10.0.0.2")
                        .addAccessConfigs(AccessConfig.newBuilder().setNatIP("34.1.2.3").build())
                )
                .build()
        );

        doReturn(mock(GoogleCredentials.class)).when(create).credentials(any());
        doReturn(client).when(create).instancesClient(any());

        var runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());
        var output = create.run(runContext);

        assertThat(output.getInstanceName(), is("vm1"));
        assertThat(output.getInstanceId(), is("123"));
        assertThat(output.getStatus(), is("RUNNING"));
        assertThat(output.getExternalIp(), is("34.1.2.3"));
        assertThat(output.getInternalIp(), is("10.0.0.2"));
    }

    @Test
    void runWithoutWaitingReturnsProvisioningWithoutQueryingInstance() throws Exception {
        var create = spy(
            Create.builder()
                .id("compute-create")
                .type(Create.class.getName())
                .projectId(Property.ofValue("my-project"))
                .zone(Property.ofValue("us-central1-a"))
                .instanceName(Property.ofValue("vm1"))
                .machineType(Property.ofValue("e2-micro"))
                .sourceImage(Property.ofValue("projects/debian-cloud/global/images/family/debian-12"))
                .waitUntilRunning(Property.ofValue(false))
                .build()
        );

        var client = mock(InstancesClient.class);
        @SuppressWarnings("unchecked")
        OperationFuture<Operation, Operation> future = mock(OperationFuture.class);
        when(client.insertAsync(anyString(), anyString(), any(Instance.class))).thenReturn(future);

        doReturn(mock(GoogleCredentials.class)).when(create).credentials(any());
        doReturn(client).when(create).instancesClient(any());

        var runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());
        var output = create.run(runContext);

        assertThat(output.getInstanceName(), is("vm1"));
        assertThat(output.getStatus(), is("PROVISIONING"));
        // Must not query a not-yet-existing instance when the caller opted out of waiting.
        org.mockito.Mockito.verify(client, org.mockito.Mockito.never()).get(anyString(), anyString(), eq("vm1"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    @Disabled
    void run() throws Exception {
        var create = Create.builder()
            .id(Create.class.getSimpleName())
            .type(Create.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("kestra-test-vm"))
            .machineType(Property.ofValue("n1-standard-1"))
            .sourceImage(Property.ofValue("projects/debian-cloud/global/images/family/debian-11"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());

        Create.Output output = create.run(runContext);
        assertThat(output.getInstanceName(), is("kestra-test-vm"));
        assertThat(output.getStatus(), notNullValue());
    }
}
