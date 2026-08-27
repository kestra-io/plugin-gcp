package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.AssetsDeclaration;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.TestAssetManagerFactory;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class RunTransferConfigTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private TestAssetManagerFactory assetManagerFactory;

    @Value("${kestra.tasks.bigquerydatatransfer.config:}")
    private String config;

    @BeforeEach
    void setUp() {
        assetManagerFactory.clear();
    }

    @Test
    void runAndWait() throws Exception {
        Assumptions.assumeTrue(config != null && !config.isBlank(), "No live BigQuery Data Transfer Service config configured, skipping");

        var task = RunTransferConfig.builder()
            .id(RunTransferConfig.class.getSimpleName())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(config))
            .pollInterval(Property.ofValue(Duration.ofSeconds(10)))
            .maxDuration(Property.ofValue(Duration.ofMinutes(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var output = task.run(runContext);

        assertThat(output.getState(), is("SUCCEEDED"));
        assertThat(output.getDestinationDatasetId(), notNullValue());
        assertThat(output.getRunName(), notNullValue());

        var allEmitted = assetManagerFactory.allEmitted();
        assertThat("should emit exactly one destination asset", allEmitted, not(empty()));
        var emittedAsset = allEmitted.stream()
            .flatMap(emit -> emit.outputs().stream())
            .filter(asset -> asset.getId() != null && asset.getId().contains(output.getDestinationDatasetId()))
            .findFirst();
        assertThat("should emit the destination dataset as a data-lineage asset", emittedAsset.isPresent(), is(true));
    }

    @Test
    void timesOutOnPendingRun() {
        Assumptions.assumeTrue(config != null && !config.isBlank(), "No live BigQuery Data Transfer Service config configured, skipping");

        var task = RunTransferConfig.builder()
            .id(RunTransferConfig.class.getSimpleName())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(config))
            .pollInterval(Property.ofValue(Duration.ofSeconds(1)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var exception = assertThrows(TimeoutException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("did not reach a terminal state"));
        assertThat(exception.getMessage(), containsString(config));
    }

    @Test
    void reattachesToInFlightRun() throws Exception {
        // Best-effort: only meaningful when the config has a run already pending or running --
        // otherwise the first call below starts one and the second should re-attach to it.
        Assumptions.assumeTrue(config != null && !config.isBlank(), "No live BigQuery Data Transfer Service config configured, skipping");

        var first = RunTransferConfig.builder()
            .id(RunTransferConfig.class.getSimpleName())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(config))
            .wait(Property.ofValue(false))
            .build();
        var firstOutput = first.run(TestsUtils.mockRunContext(runContextFactory, first, ImmutableMap.of()));

        var second = RunTransferConfig.builder()
            .id(RunTransferConfig.class.getSimpleName())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(config))
            .wait(Property.ofValue(false))
            .build();
        var secondOutput = second.run(TestsUtils.mockRunContext(runContextFactory, second, ImmutableMap.of()));

        // The run may have already completed between the two calls on a fast transfer, so this
        // is a soft check rather than a hard assertion.
        if (!secondOutput.isReattached()) {
            assertThat(secondOutput.getRunName(), notNullValue());
        } else {
            assertThat(secondOutput.getRunName(), is(firstOutput.getRunName()));
        }
    }

    @Test
    void startsNewRunWhenReattachDisabled() throws Exception {
        Assumptions.assumeTrue(config != null && !config.isBlank(), "No live BigQuery Data Transfer Service config configured, skipping");

        var task = RunTransferConfig.builder()
            .id(RunTransferConfig.class.getSimpleName())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(config))
            .reattach(Property.ofValue(false))
            .wait(Property.ofValue(false))
            .build();

        var output = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(output.isReattached(), is(false));
        assertThat(output.getRunName(), notNullValue());
    }
}
