package io.kestra.plugin.gcp.runner;

import io.kestra.core.models.tasks.runners.AbstractTaskRunnerTest;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.micronaut.context.annotation.Value;
import org.junit.jupiter.api.Disabled;

import java.time.Duration;
import java.util.List;

@Disabled("Need complex CI setup still needed to be done")
class GcpCloudRunTaskRunnerTest extends AbstractTaskRunnerTest {

    @Value("${kestra.variables.globals.project}")
    private String project;

    @Value("${kestra.variables.globals.bucket}")
    private String bucket;

    @Override
    protected TaskRunner taskRunner() {
        return GcpCloudRunTaskRunner.builder()
            .projectId(project)
            .region("us-central1")
            .bucket(bucket)
            .delete(false)
            .completionCheckInterval(Duration.ofMillis(100))
            .build();
    }
}