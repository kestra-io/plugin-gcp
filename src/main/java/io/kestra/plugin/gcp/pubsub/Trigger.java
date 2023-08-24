package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.validation.constraints.NotNull;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for messages in a Pub/Sub topic"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "topic: tes-topic",
                "maxRecords: 10"
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, PubSubConnectionInterface {

    private String projectId;

    private String serviceAccount;

    @Builder.Default
    private List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");

    private String topic;

    @Schema(
        title = "The Pub/Sub subscription",
        description = "The Pub/Sub subscription. It will be created automatically if it didn't exist and 'autoCreateSubscription' is enabled."
    )
    @PluginProperty(dynamic = true)
    private String subscription;

    @Schema(
        title = "Whether the Pub/Sub subscription should be created if not exist"
    )
    @PluginProperty
    @Builder.Default
    private Boolean autoCreateSubscription = true;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @PluginProperty
    @Schema(title = "Max number of records, when reached the task will end.")
    private Integer maxRecords;

    @PluginProperty
    @Schema(title = "Max duration in the Duration ISO format, after that the task will end.")
    private Duration maxDuration;

    @Builder.Default
    @PluginProperty
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume task = Consume.builder()
            .topic(this.topic)
            .subscription(this.subscription)
            .autoCreateSubscription(this.autoCreateSubscription)
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .scopes(this.scopes)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .serdeType(this.serdeType)
            .build();

        Consume.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Consumed '{}' messaged.", run.getCount());
        }

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
            this,
            run
        );

        Execution execution = Execution.builder()
            .id(runContext.getTriggerExecutionId())
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .trigger(executionTrigger)
            .build();

        return Optional.of(execution);
    }
}
