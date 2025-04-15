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
    title = "Trigger a flow on periodic message consumption from a Google Pub/Sub topic.",
    description = "If you would like to consume each message from a Pub/Sub topic in real-time and create one execution per message, you can use the [io.kestra.plugin.gcp.pubsub.RealtimeTrigger](https://kestra.io/plugins/plugin-gcp/triggers/io.kestra.plugin.gcp.pubsub.realtimetrigger) instead."
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
    private Property<List<String>> scopes = Property.of(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

    private Property<String> topic;

    @Schema(
        title = "The Pub/Sub subscription",
        description = "The Pub/Sub subscription. It will be created automatically if it didn't exist and 'autoCreateSubscription' is enabled."
    )
    private Property<String> subscription;

    @Schema(
        title = "Whether the Pub/Sub subscription should be created if not exist"
    )
    @Builder.Default
    private Property<Boolean> autoCreateSubscription = Property.of(true);

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Max number of records, when reached the task will end.")
    private Property<Integer> maxRecords;

    @Schema(title = "Max duration in the Duration ISO format, after that the task will end.")
    private Property<Duration> maxDuration;

    @Builder.Default
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private Property<SerdeType> serdeType = Property.of(SerdeType.STRING);

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
