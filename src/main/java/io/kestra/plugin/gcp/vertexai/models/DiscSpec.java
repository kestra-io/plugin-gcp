package io.kestra.plugin.gcp.vertexai.models;

import com.google.cloud.aiplatform.v1.DiskSpec;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    @Builder.Default
    private Property<DiskType> bootDiskType = Property.of(DiskType.PD_SSD);

    @Schema(
        title = "Size in GB of the boot disk."
    )
    @Builder.Default
    private Property<Integer> bootDiskSizeGb = Property.of(100);

    public  com.google.cloud.aiplatform.v1.DiskSpec to(RunContext runContext) throws IllegalVariableEvaluationException {
        return DiskSpec.newBuilder()
            .setBootDiskType(runContext.render(this.getBootDiskType()).as(DiskType.class).orElseThrow().value())
            .setBootDiskSizeGb(runContext.render(this.getBootDiskSizeGb()).as(Integer.class).orElseThrow())
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
