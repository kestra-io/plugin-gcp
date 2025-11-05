package io.kestra.plugin.gcp.pubsub;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.bigquery.Trigger;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class RealtimeTriggerTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.project}")
    private String project;

    @Test
    void flow() throws Exception {
        Publish task = Publish.builder()
            .id(Publish.class.getSimpleName())
            .type(Publish.class.getName())
            .topic(Property.ofValue("test-topic"))
            .projectId(Property.ofValue(this.project))
            .from(
                List.of(
                    Message.builder().data("Hello World").build()
                )
            )
            .build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        var trigger = RealtimeTrigger.builder()
            .id("watch")
            .type(RealtimeTrigger.class.getName())
            .projectId(Property.ofValue(project))
            .subscription(Property.ofValue("test-subscription"))
            .topic(Property.ofValue("test-topic"))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Mono<Execution> executionMono = Mono.from(trigger.evaluate(context.getKey(), context.getValue()));

        Execution execution = executionMono.flux().blockFirst();
        assertThat(execution, notNullValue());

        Map<String, Object> variables = execution.getTrigger().getVariables();
        assertThat(new String((byte[]) variables.get("data"), StandardCharsets.UTF_8), is("Hello World"));
    }
}
