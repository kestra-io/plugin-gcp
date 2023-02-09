package io.kestra.plugin.gcp.vertexai.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.Locale;

@Getter
@Builder
@Jacksonized
public class DiscSpec {
    @Schema(
        title = "Type of the boot disk."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private DiskType bootDiskType = DiskType.PD_SSD;

    @Schema(
        title = "Size in GB of the boot disk."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Integer bootDiskSizeGb = 100;

    public com.google.cloud.aiplatform.v1.DiskSpec to(RunContext runContext) throws IllegalVariableEvaluationException {
        return com.google.cloud.aiplatform.v1.DiskSpec.newBuilder()
            .setBootDiskType(this.getBootDiskType().value())
            .setBootDiskSizeGb(this.getBootDiskSizeGb())
            .build();
    }

    public enum DiskType {
        PD_SSD,
        PD_STANDARD;

        public String value() {
            return this.name().replace("_", "-").toLowerCase(Locale.ROOT);
        }
    }
}
