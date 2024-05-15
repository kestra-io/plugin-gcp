package io.kestra.plugin.gcp.runner;

import io.kestra.core.models.tasks.runners.AbstractTaskRunnerTest;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.micronaut.context.annotation.Value;
import org.junit.jupiter.api.Disabled;

import java.time.Duration;

@Disabled("Need complex CI setup still needed to be done")
class CloudRunTest extends AbstractTaskRunnerTest {

    @Value("${kestra.variables.globals.project}")
    private String project;

    @Value("${kestra.variables.globals.bucket}")
    private String bucket;

    @Override
    protected TaskRunner taskRunner() {
        return CloudRun.builder()
            .projectId(project)
            .region("us-central1")
            .bucket(bucket)
            .delete(false)
            .completionCheckInterval(Duration.ofMillis(100))
            .build();
    }
}