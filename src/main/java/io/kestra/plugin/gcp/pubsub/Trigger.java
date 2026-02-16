package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on periodic Pub/Sub pulls",
    description = "Polls a subscription every `interval` (default 60s) via the Consume task and starts a Flow when messages are fetched. Use RealtimeTrigger for one-execution-per-message."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Received: {{ trigger.data }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.gcp.pubsub.Trigger
                    projectId: test-project-id
                    subscription: test-subscription
                    topic: test-topic
                    maxRecords: 10
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, PubSubConnectionInterface {

    private Property<String> projectId;

    private Property<String> serviceAccount;

    private Property<String> impersonatedServiceAccount;

    @Builder.Default
    private Property<List<String>> scopes = Property.ofValue(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

    private Property<String> topic;

    @Schema(
        title = "Subscription",
        description = "Subscription name; auto-created if `autoCreateSubscription` is true"
    )
    private Property<String> subscription;

    @Schema(
        title = "Auto-create subscription",
        description = "Create the subscription when missing; default true"
    )
    @Builder.Default
    private Property<Boolean> autoCreateSubscription = Property.ofValue(true);

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Max records",
        description = "Stop polling iteration after this many messages"
    )
    private Property<Integer> maxRecords;

    @Schema(
        title = "Max duration",
        description = "Duration limit for a polling run (ISO-8601); optional"
    )
    private Property<Duration> maxDuration;

    @Builder.Default
    @NotNull
    @Schema(
        title = "Serde type",
        description = "Serializer/deserializer for message payloads; defaults to STRING"
    )
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

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

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }
}
