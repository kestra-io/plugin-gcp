package io.kestra.plugin.gcp.dataproc.batches;

import java.util.List;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSparkSubmit extends AbstractBatch {
    @Schema(
        title = "File URIs",
        description = "HCFS URIs copied into each executor working dir (gs://, hdfs://, or file://)"
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> fileUris;

    @Schema(
        title = "Archive URIs",
        description = "HCFS URIs of archives extracted into each executor dir (.jar, .tar, .tar.gz, .tgz, .zip)"
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> archiveUris;

    @Schema(
        title = "Jar URIs",
        description = "HCFS URIs of JARs added to driver and executor classpaths"
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> jarFileUris;

    @Schema(
        title = "Driver arguments",
        description = "Arguments passed to the driver; avoid options that belong in batch properties (e.g., --conf)"
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> args;
}
