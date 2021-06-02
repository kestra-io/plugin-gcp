package io.kestra.plugin.gcp.gcs.models;

import com.google.cloud.storage.BucketInfo;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.gcp.gcs.models.StorageClass;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import javax.validation.constraints.NotNull;

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
        @PluginProperty(dynamic = true)
        private final Integer age;
    }

    @Getter
    @Builder
    @Jacksonized
    public static class Action {
        @NotNull
        @Schema(
            title = "The type of the action (DELETE ...)"
        )
        @PluginProperty(dynamic = true)
        private final Action.Type type;

        @Schema(
            title = "The value for the action (if any)"
        )
        @PluginProperty(dynamic = true)
        private final String value;

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
        public BucketInfo.LifecycleRule convert(Condition condition) {
            return new BucketInfo.LifecycleRule(
                BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction(),
                BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(condition.getAge()).build()
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
        @PluginProperty(dynamic = true)
        private final StorageClass storageClass;

        @Override
        public BucketInfo.LifecycleRule convert(Condition condition) {
            return new BucketInfo.LifecycleRule(
                BucketInfo.LifecycleRule.LifecycleAction.newSetStorageClassAction(com.google.cloud.storage.StorageClass.valueOf(this.storageClass.name())),
                BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(condition.getAge()).build()
            );
        }
    }

    public interface LifecycleAction {
        BucketInfo.LifecycleRule convert(Condition condition);
    }
}
