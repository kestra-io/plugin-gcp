package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceSettings;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.AssetsDeclaration;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.TestAssetManagerFactory;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unconditional happy-path test of {@link RunTransferConfig#run(RunContext, DataTransferServiceClient)}: drives the
 * task end to end against WireMock over the client's HTTP-JSON transport, no live GCP credentials required.
 */
@KestraTest
@WireMockTest
class RunTransferConfigWireMockTest {
    private static final String CONFIG_NAME = "projects/my-project/locations/us/transferConfigs/615123456789012345";
    private static final String RUN_NAME = CONFIG_NAME + "/runs/run-abc123";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private TestAssetManagerFactory assetManagerFactory;

    @BeforeEach
    void setUp() {
        assetManagerFactory.clear();
    }

    @Test
    void runsAndWaitsForCompletionThenEmitsTableAsset(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(
            post(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "runs": [
                                {
                                  "name": "%s",
                                  "state": "PENDING",
                                  "scheduleTime": "2026-01-01T00:00:00Z"
                                }
                              ]
                            }
                            """.formatted(RUN_NAME))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + RUN_NAME))
                .inScenario("transfer-run")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "PENDING",
                              "scheduleTime": "2026-01-01T00:00:00Z"
                            }
                            """.formatted(RUN_NAME))
                )
                .willSetStateTo("succeeded")
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + RUN_NAME))
                .inScenario("transfer-run")
                .whenScenarioStateIs("succeeded")
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "SUCCEEDED",
                              "scheduleTime": "2026-01-01T00:00:00Z"
                            }
                            """.formatted(RUN_NAME))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + CONFIG_NAME))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "destinationDatasetId": "my_dataset",
                              "params": {
                                "destination_table_name_template": "my_table"
                              }
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(RunTransferConfigWireMockTest.class.getSimpleName())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(false))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (
            DataTransferServiceClient client = DataTransferServiceClient.create(
                DataTransferServiceSettings.newHttpJsonBuilder()
                    .setEndpoint(wmRuntimeInfo.getHttpBaseUrl())
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .setTransportChannelProvider(DataTransferServiceSettings.defaultHttpJsonTransportProviderBuilder().build())
                    .build()
            )
        ) {
            var output = task.run(runContext, client);

            assertThat(output.getState(), is("SUCCEEDED"));
            assertThat(output.getRunName(), notNullValue());
            assertThat(output.getDestinationDatasetId(), is("my_dataset"));

            var emitted = assetManagerFactory.allEmitted();
            assertThat("should emit the destination as a data-lineage asset", emitted, not(empty()));
            var emittedAsset = emitted.stream()
                .flatMap(emit -> emit.outputs().stream())
                .filter(asset -> "io.kestra.plugin.ee.assets.Table".equals(asset.getType()))
                .findFirst();
            assertThat("should emit a Table asset since the config has a literal destination table", emittedAsset.isPresent(), is(true));
        }
    }
}
