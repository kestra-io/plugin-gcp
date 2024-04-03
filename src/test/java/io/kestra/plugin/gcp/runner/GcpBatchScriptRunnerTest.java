package io.kestra.plugin.gcp.runner;

import io.kestra.core.models.script.AbstractScriptRunnerTest;
import io.kestra.core.models.script.ScriptRunner;
import io.micronaut.context.annotation.Value;

import java.util.List;

class GcpBatchScriptRunnerTest extends AbstractScriptRunnerTest {

    @Value("${kestra.variables.globals.project}")
    private String project;

    @Value("${kestra.variables.globals.bucket}")
    private String bucket;

    @Value("${kestra.variables.globals.network}")
    private String network;

    @Value("${kestra.variables.globals.subnetwork}")
    private String subnetwork;

    @Override
    protected ScriptRunner scriptRunner() {
        return GcpBatchScriptRunner.builder()
            .projectId(project)
            .region("us-central1")
            .bucket(bucket)
            .networkInterfaces(List.of(GcpBatchScriptRunner.NetworkInterface.builder().network(network).subnetwork(subnetwork).build()))
            .delete(false)
            .build();
    }
}