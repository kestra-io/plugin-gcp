package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeoutException;

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
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.TestAssetManagerFactory;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unconditional happy-path test of {@link RunTransferConfig#run(RunContext, DataTransferServiceClient)}: drives the
 * task end to end against WireMock over the client's HTTP-JSON transport, no live GCP credentials required.
 */
@KestraTest
@WireMockTest
class RunTransferConfigWireMockTest {
    private static final String CONFIG_NAME = "projects/my-project/locations/us/transferConfigs/615123456789012345";
    private static final String RUN_NAME = CONFIG_NAME + "/runs/run-abc123";

    // A literal schedule time picked as future at write time silently becomes past, flipping which branch runs.
    private static String scheduleTimeFromNow(Duration offset) {
        return Instant.now().plus(offset).toString();
    }

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

    @Test
    void reattachAdoptsInFlightRunAndDoesNotStartNewOne(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var inFlightRunName = CONFIG_NAME + "/runs/run-inflight";

        stubFor(
            get(urlPathEqualTo("/v1/" + CONFIG_NAME + "/runs"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "transferRuns": [
                                {
                                  "name": "%s",
                                  "state": "RUNNING",
                                  "scheduleTime": "2020-01-01T00:00:00Z"
                                }
                              ]
                            }
                            """.formatted(inFlightRunName))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + inFlightRunName))
                .inScenario("in-flight-run")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "RUNNING",
                              "scheduleTime": "2020-01-01T00:00:00Z"
                            }
                            """.formatted(inFlightRunName))
                )
                .willSetStateTo("succeeded")
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + inFlightRunName))
                .inScenario("in-flight-run")
                .whenScenarioStateIs("succeeded")
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "SUCCEEDED",
                              "scheduleTime": "2020-01-01T00:00:00Z"
                            }
                            """.formatted(inFlightRunName))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var output = task.run(runContext, client);

            assertThat("an old in-flight run must still be adopted when reattachMaxAge is unset", output.isReattached(), is(true));
            assertThat(output.getRunName(), is(inFlightRunName));
            assertThat(output.getState(), is("SUCCEEDED"));
        }

        verify(0, postRequestedFor(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns")));
    }

    @Test
    void reattachMaxAgeSkipsOldRunAndStartsNew(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var staleRunName = CONFIG_NAME + "/runs/run-stale";
        var newRunName = CONFIG_NAME + "/runs/run-fresh";

        stubFor(
            get(urlPathEqualTo("/v1/" + CONFIG_NAME + "/runs"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "transferRuns": [
                                {
                                  "name": "%s",
                                  "state": "RUNNING",
                                  "scheduleTime": "2020-01-01T00:00:00Z"
                                }
                              ]
                            }
                            """.formatted(staleRunName))
                )
        );

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
                            """.formatted(newRunName))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + newRunName))
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
                            """.formatted(newRunName))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(true))
            .reattachMaxAge(Property.ofValue(Duration.ofMinutes(5)))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var output = task.run(runContext, client);

            assertThat("an in-flight run older than reattachMaxAge must be treated as stale", output.isReattached(), is(false));
            assertThat(output.getRunName(), is(newRunName));
            assertThat(output.getState(), is("SUCCEEDED"));
        }

        verify(1, postRequestedFor(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns")));
    }

    @Test
    void reattachIgnoresFutureScheduledRunAndStartsNewOne(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var queuedRunName = CONFIG_NAME + "/runs/run-queued";
        var newRunName = CONFIG_NAME + "/runs/run-fresh";
        var newRunScheduleTime = scheduleTimeFromNow(Duration.ZERO);

        // A refresh-window transfer permanently keeps runs queued ahead of now.
        stubFor(
            get(urlPathEqualTo("/v1/" + CONFIG_NAME + "/runs"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "transferRuns": [
                                {
                                  "name": "%s",
                                  "state": "PENDING",
                                  "scheduleTime": "%s"
                                }
                              ]
                            }
                            """.formatted(queuedRunName, scheduleTimeFromNow(Duration.ofHours(1))))
                )
        );

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
                                  "scheduleTime": "%s"
                                }
                              ]
                            }
                            """.formatted(newRunName, newRunScheduleTime))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + newRunName))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "SUCCEEDED",
                              "scheduleTime": "%s"
                            }
                            """.formatted(newRunName, newRunScheduleTime))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var output = task.run(runContext, client);

            assertThat("a run scheduled in the future is queued, not in-flight, so it must not be adopted", output.isReattached(), is(false));
            assertThat(output.getRunName(), is(newRunName));
            assertThat(output.getState(), is("SUCCEEDED"));
        }

        verify(1, postRequestedFor(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns")));
    }

    @Test
    void reattachAdoptsRunScheduledWithinGrace(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var inFlightRunName = CONFIG_NAME + "/runs/run-just-started";
        var scheduleTime = scheduleTimeFromNow(Duration.ofSeconds(2));

        // A marginally future schedule time still belongs to a run this task started.
        stubFor(
            get(urlPathEqualTo("/v1/" + CONFIG_NAME + "/runs"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "transferRuns": [
                                {
                                  "name": "%s",
                                  "state": "PENDING",
                                  "scheduleTime": "%s"
                                }
                              ]
                            }
                            """.formatted(inFlightRunName, scheduleTime))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + inFlightRunName))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "SUCCEEDED",
                              "scheduleTime": "%s"
                            }
                            """.formatted(inFlightRunName, scheduleTime))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var output = task.run(runContext, client);

            assertThat("a run scheduled just ahead of now is the task's own run and must still be adopted", output.isReattached(), is(true));
            assertThat(output.getRunName(), is(inFlightRunName));
        }

        verify(0, postRequestedFor(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns")));
    }

    @Test
    void reattachMaxAgeAndFutureCutoffBothApply(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var staleRunName = CONFIG_NAME + "/runs/run-stale";
        var queuedRunName = CONFIG_NAME + "/runs/run-queued";
        var newRunName = CONFIG_NAME + "/runs/run-fresh";
        var newRunScheduleTime = scheduleTimeFromNow(Duration.ZERO);

        // One candidate fails each bound, so neither cutoff can rescue a candidate the other rejects.
        stubFor(
            get(urlPathEqualTo("/v1/" + CONFIG_NAME + "/runs"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(
                            """
                                {
                                  "transferRuns": [
                                    {
                                      "name": "%s",
                                      "state": "RUNNING",
                                      "scheduleTime": "%s"
                                    },
                                    {
                                      "name": "%s",
                                      "state": "PENDING",
                                      "scheduleTime": "%s"
                                    }
                                  ]
                                }
                                """.formatted(
                                staleRunName, scheduleTimeFromNow(Duration.ofHours(-6)),
                                queuedRunName, scheduleTimeFromNow(Duration.ofHours(1))
                            )
                        )
                )
        );

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
                                  "scheduleTime": "%s"
                                }
                              ]
                            }
                            """.formatted(newRunName, newRunScheduleTime))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + newRunName))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "SUCCEEDED",
                              "scheduleTime": "%s"
                            }
                            """.formatted(newRunName, newRunScheduleTime))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(true))
            .reattachMaxAge(Property.ofValue(Duration.ofMinutes(30)))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var output = task.run(runContext, client);

            assertThat("a stale candidate and a queued candidate leave nothing to adopt", output.isReattached(), is(false));
            assertThat(output.getRunName(), is(newRunName));
        }

        verify(1, postRequestedFor(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns")));
    }

    @Test
    void negativeReattachMaxAgeThrows(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(true))
            .reattachMaxAge(Property.ofValue(Duration.ofHours(-1)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext, client));

            assertThat(exception.getMessage(), containsString("`reattachMaxAge` must not be negative"));
        }

        verify(0, postRequestedFor(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns")));
    }

    @Test
    void reattachPrefersRunningOverPending(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var runningRunName = CONFIG_NAME + "/runs/run-running";
        var pendingRunName = CONFIG_NAME + "/runs/run-pending";
        var runningScheduleTime = scheduleTimeFromNow(Duration.ofMinutes(-30));

        // The PENDING candidate has the later scheduleTime, so schedule-time-only ordering would pick it.
        stubFor(
            get(urlPathEqualTo("/v1/" + CONFIG_NAME + "/runs"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(
                            """
                                {
                                  "transferRuns": [
                                    {
                                      "name": "%s",
                                      "state": "RUNNING",
                                      "scheduleTime": "%s"
                                    },
                                    {
                                      "name": "%s",
                                      "state": "PENDING",
                                      "scheduleTime": "%s"
                                    }
                                  ]
                                }
                                """.formatted(
                                runningRunName, runningScheduleTime,
                                pendingRunName, scheduleTimeFromNow(Duration.ofMinutes(-5))
                            )
                        )
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + runningRunName))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "SUCCEEDED",
                              "scheduleTime": "%s"
                            }
                            """.formatted(runningRunName, runningScheduleTime))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(true))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var output = task.run(runContext, client);

            assertThat(output.isReattached(), is(true));
            assertThat("a RUNNING candidate must outrank a PENDING one with a later schedule time", output.getRunName(), is(runningRunName));
        }

        verify(0, postRequestedFor(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns")));
    }

    @Test
    void failedRunThrows(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var runName = CONFIG_NAME + "/runs/run-failed";

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
                            """.formatted(runName))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + runName))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "%s",
                              "state": "FAILED",
                              "scheduleTime": "2026-01-01T00:00:00Z",
                              "errorStatus": {
                                "message": "boom"
                              }
                            }
                            """.formatted(runName))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(false))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var exception = assertThrows(IllegalStateException.class, () -> task.run(runContext, client));

            assertThat(exception.getMessage(), containsString("boom"));
            assertThat(exception.getMessage(), containsString(runName));
            assertThat(exception.getMessage(), containsString("FAILED"));
        }
    }

    @Test
    void timesOutWhenNeverTerminal(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var runName = CONFIG_NAME + "/runs/run-pending-forever";

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
                            """.formatted(runName))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + runName))
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
                            """.formatted(runName))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(false))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofMillis(500)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var exception = assertThrows(TimeoutException.class, () -> task.run(runContext, client));

            assertThat(exception.getMessage(), containsString("did not reach a terminal state"));
        }
    }

    @Test
    void waitFalseReturnsImmediatelyWithoutAsset(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var runName = CONFIG_NAME + "/runs/run-no-wait";

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
                            """.formatted(runName))
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
                              "destinationDatasetId": "my_dataset"
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(false))
            .wait(Property.ofValue(false))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var output = task.run(runContext, client);

            assertThat(output.getState(), is("PENDING"));
            assertThat(output.getRunName(), is(runName));
        }

        assertThat("wait=false must not confirm completion, so no destination asset is emitted", assetManagerFactory.allEmitted(), is(empty()));
        verify(0, getRequestedFor(urlPathEqualTo("/v1/" + runName)));
    }

    @Test
    void emptyStartResponseThrows(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(
            post(urlPathEqualTo("/v1/" + CONFIG_NAME + ":startManualRuns"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "runs": []
                            }
                            """)
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(false))
            .wait(Property.ofValue(true))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var exception = assertThrows(IllegalStateException.class, () -> task.run(runContext, client));

            assertThat(exception.getMessage(), containsString("started no run"));
            assertThat(exception.getMessage(), containsString(CONFIG_NAME));
        }
    }

    @Test
    void emitsDatasetAssetWhenNoLiteralTable(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        var runName = CONFIG_NAME + "/runs/run-dataset-asset";

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
                            """.formatted(runName))
                )
        );

        stubFor(
            get(urlPathEqualTo("/v1/" + runName))
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
                            """.formatted(runName))
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
                                "destination_table_name_template": "my_table_{run_date}"
                              }
                            }
                            """.formatted(CONFIG_NAME))
                )
        );

        var task = RunTransferConfig.builder()
            .id(IdUtils.create())
            .type(RunTransferConfig.class.getName())
            .transferConfigName(Property.ofValue(CONFIG_NAME))
            .reattach(Property.ofValue(false))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .assets(new AssetsDeclaration(true, List.of(), List.of()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        try (DataTransferServiceClient client = httpJsonClient(wmRuntimeInfo)) {
            var output = task.run(runContext, client);

            assertThat(output.getState(), is("SUCCEEDED"));

            var emitted = assetManagerFactory.allEmitted();
            var datasetAsset = emitted.stream()
                .flatMap(emit -> emit.outputs().stream())
                .filter(asset -> "io.kestra.plugin.ee.assets.Dataset".equals(asset.getType()))
                .findFirst();
            assertThat("should emit a Dataset asset since the destination table is templated", datasetAsset.isPresent(), is(true));
        }
    }

    private static DataTransferServiceClient httpJsonClient(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        return DataTransferServiceClient.create(
            DataTransferServiceSettings.newHttpJsonBuilder()
                .setEndpoint(wmRuntimeInfo.getHttpBaseUrl())
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setTransportChannelProvider(DataTransferServiceSettings.defaultHttpJsonTransportProviderBuilder().build())
                .build()
        );
    }
}
