package io.kestra.plugin.gcp.compute;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
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
