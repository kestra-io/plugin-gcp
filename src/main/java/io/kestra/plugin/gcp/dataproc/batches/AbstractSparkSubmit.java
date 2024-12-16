package io.kestra.plugin.gcp.dataproc.batches;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSparkSubmit extends AbstractBatch {
    @Schema(
        title = "HCFS URIs of files to be placed in the working directory of each executor.",
        description = "Hadoop Compatible File System (HCFS) URIs should be accessible from the cluster. Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix"
    )
    protected Property<List<String>> fileUris;

    @Schema(
        title = "HCFS URIs of archives to be extracted into the working director of each executor. Supported file types: `.jar`, `.tar`, `.tar.gz`, `.tgz`, and `.zip`.",
        description = "Hadoop Compatible File System (HCFS) URIs should be accessible from the cluster. Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix"
    )
    protected Property<List<String>> archiveUris;

    @Schema(
        title = "HCFS URIs of jar files to add to the classpath of the Spark driver and tasks.",
        description = "Hadoop Compatible File System (HCFS) URIs should be accessible from the cluster. Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix"
    )
    protected Property<List<String>> jarFileUris;

    @Schema(
        title = "The arguments to pass to the driver.",
        description = "Do not include arguments that can be set as batch properties, such as `--conf`, since a collision " +
            "can occur that causes an incorrect batch submission."
    )
    protected Property<List<String>> args;
}
