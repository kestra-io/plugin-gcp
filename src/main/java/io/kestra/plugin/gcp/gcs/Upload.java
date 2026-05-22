package io.kestra.plugin.gcp.gcs;

import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.zip.CRC32C;

import org.slf4j.Logger;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Uploada FILE input to GCS",
            code = """
                id: gcp_gcs_upload
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: upload
                    type: io.kestra.plugin.gcp.gcs.Upload
                    from: "{{ inputs.file }}"
                    to: "gs://my_bucket/dir/file.csv"
                """
        ),
        @Example(
            full = true,
            title = "Download data and upload to Google Cloud Storage",
            code = """
                id: load_to_cloud_storage
                namespace: company.team

                tasks:
                  - id: data
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv

                  - id: cloud_storage
                    type: io.kestra.plugin.gcp.gcs.Upload
                    from: "{{ outputs.data.uri }}"
                    to: gs://kestra-demo/data.csv
                """
        )
    },
    metrics = {
        @Metric(
            name = "file.size",
            type = Counter.TYPE,
            unit = "bytes",
            description = "Size of the uploaded file."
        )
    }
)
@Schema(
    title = "Upload a file to GCS",
    description = "Reads a file from Kestra internal storage and writes it to a gs:// destination. Supports content metadata settings."
)
public class Upload extends AbstractGcs implements RunnableTask<Upload.Output> {
    @Schema(
        title = "Source file URI",
        description = "Kestra internal storage URI to upload"
    )
    @PluginProperty(internalStorageURI = true, group = "source")
    private Property<String> from;

    @Schema(
        title = "Destination object URI",
        description = "gs:// bucket/path for the uploaded object"
    )
    @PluginProperty(group = "destination")
    private Property<String> to;

    @Schema(
        title = "Content-Type"
    )
    @PluginProperty(group = "advanced")
    private Property<String> contentType;

    @Schema(
        title = "Content-Encoding"
    )
    @PluginProperty(group = "advanced")
    private Property<String> contentEncoding;

    @Schema(
        title = "Content-Disposition"
    )
    @PluginProperty(group = "advanced")
    private Property<String> contentDisposition;

    @Schema(
        title = "Cache-Control"
    )
    @PluginProperty(group = "advanced")
    private Property<String> cacheControl;

    @Schema(
        title = "Validate checksum after upload",
        description = "When true (default), compute MD5 and CRC32C client-side while streaming and compare them against what GCS reports server-side once the upload completes. When false, the implicit transit integrity check is skipped; explicit `expectedMd5`/`expectedCrc32c` assertions are still honored when set."
    )
    @PluginProperty(group = "reliability")
    @Builder.Default
    private Property<Boolean> validateChecksum = Property.ofValue(true);

    @Schema(
        title = "Expected MD5 of the source",
        description = "Optional base64-encoded MD5 digest. When set, the task fails if the source data read from Kestra internal storage does not match this digest. Use this to assert end-to-end integrity against a checksum produced upstream."
    )
    @PluginProperty(group = "reliability")
    private Property<String> expectedMd5;

    @Schema(
        title = "Expected CRC32C of the source",
        description = "Optional base64-encoded big-endian CRC32C. When set, the task fails if the source data read from Kestra internal storage does not match this checksum."
    )
    @PluginProperty(group = "reliability")
    private Property<String> expectedCrc32c;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        URI from = encode(runContext, runContext.render(this.from).as(String.class).orElse(null));
        URI to = encode(runContext, runContext.render(this.to).as(String.class).orElse(null));

        BlobInfo.Builder builder = BlobInfo
            .newBuilder(
                BlobId.of(
                    to.getScheme().equals("gs") ? to.getAuthority() : to.getScheme(),
                    blobPath(to.getPath().substring(1))
                )
            );

        if (this.contentType != null) {
            builder.setContentType(runContext.render(this.contentType).as(String.class).orElseThrow());
        }

        if (this.contentEncoding != null) {
            builder.setContentEncoding(runContext.render(this.contentEncoding).as(String.class).orElseThrow());
        }

        if (this.contentDisposition != null) {
            builder.setContentDisposition(runContext.render(this.contentDisposition).as(String.class).orElseThrow());
        }

        if (this.cacheControl != null) {
            builder.setCacheControl(runContext.render(this.cacheControl).as(String.class).orElseThrow());
        }

        BlobInfo destination = builder.build();

        boolean rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(true);
        String rExpectedMd5 = runContext.render(this.expectedMd5).as(String.class).orElse(null);
        String rExpectedCrc32c = runContext.render(this.expectedCrc32c).as(String.class).orElse(null);
        boolean computeLocally = rValidateChecksum || rExpectedMd5 != null || rExpectedCrc32c != null;

        logger.debug("Upload from '{}' to '{}'", from, to);

        MessageDigest md5Digest = computeLocally ? MessageDigest.getInstance("MD5") : null;
        CRC32C crc32c = computeLocally ? new CRC32C() : null;

        try (InputStream rawData = runContext.storage().getFile(from);
             InputStream data = computeLocally ? new DigestInputStream(rawData, md5Digest) : rawData) {
            long size = 0;
            try (WriteChannel writer = connection.writer(destination)) {
                byte[] buffer = new byte[10_240];

                int limit;
                while ((limit = data.read(buffer)) >= 0) {
                    if (computeLocally) {
                        crc32c.update(buffer, 0, limit);
                    }
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                    size += limit;
                }
            }

            String computedMd5 = null;
            String computedCrc32c = null;
            if (computeLocally) {
                computedMd5 = Base64.getEncoder().encodeToString(md5Digest.digest());
                computedCrc32c = Base64.getEncoder().encodeToString(
                    ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt((int) crc32c.getValue()).array()
                );

                if (rExpectedMd5 != null && !rExpectedMd5.equals(computedMd5)) {
                    throw new IllegalStateException(
                        "Source MD5 mismatch: expected=" + rExpectedMd5 + ", computed=" + computedMd5
                    );
                }
                if (rExpectedCrc32c != null && !rExpectedCrc32c.equals(computedCrc32c)) {
                    throw new IllegalStateException(
                        "Source CRC32C mismatch: expected=" + rExpectedCrc32c + ", computed=" + computedCrc32c
                    );
                }

                if (rValidateChecksum) {
                    Blob uploaded = connection.get(destination.getBlobId());
                    if (uploaded == null) {
                        throw new IllegalStateException("Uploaded blob not found for integrity check: " + destination.getBlobId());
                    }

                    if (!computedMd5.equals(uploaded.getMd5())) {
                        throw new IllegalStateException(
                            "MD5 mismatch between client and GCS: client=" + computedMd5 + ", gcs=" + uploaded.getMd5()
                        );
                    }
                    if (!computedCrc32c.equals(uploaded.getCrc32c())) {
                        throw new IllegalStateException(
                            "CRC32C mismatch between client and GCS: client=" + computedCrc32c + ", gcs=" + uploaded.getCrc32c()
                        );
                    }

                    logger.debug("Upload integrity verified (md5={}, crc32c={})", computedMd5, computedCrc32c);
                }
            }

            runContext.metric(Counter.of("file.size", size));

            return Output
                .builder()
                .uri(new URI("gs://" + destination.getBucket() + "/" + encode(destination.getName())))
                .md5(computedMd5)
                .crc32c(computedCrc32c)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Uploaded object URI")
        private URI uri;

        @Schema(title = "Base64-encoded MD5 of the uploaded data")
        private String md5;

        @Schema(title = "Base64-encoded big-endian CRC32C of the uploaded data")
        private String crc32c;
    }
}
