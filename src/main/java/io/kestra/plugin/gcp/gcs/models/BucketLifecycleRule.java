package io.kestra.plugin.gcp.gcs.models;

import com.google.cloud.storage.BucketInfo;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class BucketLifecycleRule {
    @NotNull
    @Schema(
        title = "The condition"
    )
    @PluginProperty(dynamic = true)
    private final Condition condition;

    @NotNull
    @Schema(
        title = "The action to take when a lifecycle condition is met"
    )
    @PluginProperty(dynamic = true)
    private final Action action;

    @Getter
    @Builder
    @Jacksonized
    public static class Condition {
        @NotNull
        @Schema(
            title = "The Age condition is satisfied when an object reaches the specified age (in days). Age is measured from the object's creation time."
        )
        private final Property<Integer> age;
    }

    @Getter
    @Builder
    @Jacksonized
    public static class Action {
        @NotNull
        @Schema(
            title = "The type of the action (DELETE ...)"
        )
        private final Property<Type> type;

        @Schema(
            title = "The value for the action (if any)"
        )
        private final Property<String> value;

        public enum Type {
            DELETE,
            SET_STORAGE_CLASS
        }
    }

    @Getter
    @Builder
    @Jacksonized
    public static class DeleteAction implements LifecycleAction {
        @Override
        public BucketInfo.LifecycleRule convert(Condition condition, RunContext runContext) throws IllegalVariableEvaluationException {
            return new BucketInfo.LifecycleRule(
                BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction(),
                BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(runContext.render(condition.getAge()).as(Integer.class).orElseThrow()).build()
            );
        }
    }

    @Getter
    @Builder
    @Jacksonized
    public static class SetStorageAction implements LifecycleAction {
        @NotNull
        @Schema(
            title = "The storage class (standard, nearline, coldline ...)"
        )
        private final Property<StorageClass> storageClass;

        public BucketInfo.LifecycleRule convert(Condition condition, RunContext runContext) throws IllegalVariableEvaluationException {
            return new BucketInfo.LifecycleRule(
                BucketInfo.LifecycleRule.LifecycleAction.newSetStorageClassAction(com.google.cloud.storage.StorageClass.valueOf(
                    runContext.render(this.storageClass).as(StorageClass.class).orElseThrow().name())),
                BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(runContext.render(condition.getAge()).as(Integer.class).orElseThrow()).build()
            );
        }
    }

    public interface LifecycleAction {
        BucketInfo.LifecycleRule convert(Condition condition, RunContext runContext) throws IllegalVariableEvaluationException;
    }

    public static List<BucketInfo.LifecycleRule> convert(List<BucketLifecycleRule> rules, RunContext runContext) {
        return rules
            .stream()
            .map(c -> {
                try {
                    return c.convert(runContext);
                } catch (IllegalVariableEvaluationException e) {
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.toList());
    }

    public BucketInfo.LifecycleRule convert(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.getCondition() == null || this.getAction() == null) {
            return null;
        }

        switch (runContext.render(this.getAction().getType()).as(Action.Type.class).orElseThrow()) {
            case DELETE:
                return BucketLifecycleRule.DeleteAction.builder()
                    .build()
                    .convert(this.getCondition(), runContext);
            case SET_STORAGE_CLASS:
                return SetStorageAction.builder()
                    .storageClass(Property.of(
                        StorageClass.valueOf(runContext.render(this.getAction().getValue()).as(String.class).orElse(null)))
                    )
                    .build()
                    .convert(this.getCondition(), runContext);
            default:
                return null;

        }
    }
}
