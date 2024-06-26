package io.kestra.plugin.gcp.runner;

import io.kestra.core.models.tasks.runners.AbstractTaskRunnerTest;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.micronaut.context.annotation.Value;
import org.junit.jupiter.api.Disabled;

import java.time.Duration;
import java.util.List;

@Disabled("Need complex CI setup still needed to be done")
class BatchTest extends AbstractTaskRunnerTest {

    @Value("${kestra.variables.globals.project}")
    private String project;

    @Value("${kestra.variables.globals.bucket}")
    private String bucket;

    @Value("${kestra.variables.globals.network}")
    private String network;

    @Value("${kestra.variables.globals.subnetwork}")
    private String subnetwork;

    @Override
    protected TaskRunner taskRunner() {
        return Batch.builder()
            .projectId(project)
            .region("us-central1")
            .bucket(bucket)
            .networkInterfaces(List.of(Batch.NetworkInterface.builder().network(network).subnetwork(subnetwork).build()))
            .delete(false)
            .completionCheckInterval(Duration.ofMillis(100))
            .build();
    }

    @Override
    protected boolean needsToSpecifyWorkingDirectory() {
        return true;
    }
}