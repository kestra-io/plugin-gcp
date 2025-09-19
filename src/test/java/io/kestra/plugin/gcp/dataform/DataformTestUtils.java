package io.kestra.plugin.gcp.dataform;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
class DataformTestUtils {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.dataform.project}")
    private String project;

    @Value("${kestra.tasks.dataform.region}")
    private String region;

    RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }

    InvokeWorkflow defaultInvokeWorkflowTask(String repositoryId, String workflowConfigId, boolean wait) {
        return InvokeWorkflow.builder()
            .id("invoke-workflow")
            .type(InvokeWorkflow.class.getName())
            .projectId(Property.ofValue(project))
            .location(Property.ofValue(region))
            .repositoryId(Property.ofValue(repositoryId))
            .workflowConfigId(Property.ofValue(workflowConfigId))
            .wait(wait)
            .build();
    }
}