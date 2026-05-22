package io.kestra.plugin.gcp.gcs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.zip.CRC32C;

import org.slf4j.Logger;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;

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
            code = """
                id: gcp_gcs_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.gcp.gcs.Download
                    from: "gs://my_bucket/dir/file.csv"
                """
        ),
        @Example(
            full = true,
            title = "Download with end-to-end checksum validation",
            code = """
                id: gcp_gcs_download_validated
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.gcp.gcs.Download
                    from: "gs://my_bucket/dir/file.csv"
                    validateChecksum: true
                """
        )
    },
    metrics = {
        @Metric(
            name = "checksum.validated",
            type = Counter.TYPE,
            description = "Number of files whose checksum was successfully validated after download."
        )
    }
)
@Schema(
    title = "Download a GCS object",
    description = "Reads a gs:// object to a temp file and stores it in Kestra internal storage. Optionally validates the file integrity by recomputing its checksum locally and comparing it to the value stored in GCS."
)
public class Download extends AbstractGcs implements RunnableTask<Download.Output> {
    private static final int COPY_BUFFER_SIZE = 16 * 1024;

    @Schema(
        title = "Source object URI",
        description = "gs:// path to download"
    )
    @PluginProperty(group = "source")
    private Property<String> from;

    @Schema(
        title = "Validate the downloaded file against the GCS checksum",
        description = "When `true`, the file is streamed through a CRC32C hasher (or MD5 if the object has no CRC32C) and compared to the checksum reported by Google Cloud Storage. If the values differ, the task fails and the temporary file is deleted. Used for end-to-end integrity, not for security/authentication."
    )
    @PluginProperty(group = "reliability")
    @Builder.Default
    private Property<Boolean> validateChecksum = Property.ofValue(false);

    static File download(RunContext runContext, Storage connection, BlobId source, boolean validateChecksum) throws IOException {
        Blob blob = connection.get(source);
        if (blob == null) {
            throw new IllegalArgumentException("Unable to find blob on bucket '" + source.getBucket() + "' with path '" + source.getName() + "'");
        }

        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(source.getName())).toFile();

        boolean success = false;
        try {
            if (validateChecksum) {
                downloadAndValidate(runContext, blob, tempFile);
            } else {
                try (
                    ReadChannel readChannel = blob.reader();
                    FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
                    FileChannel channel = fileOutputStream.getChannel()
                ) {
                    channel.transferFrom(readChannel, 0, Long.MAX_VALUE);
                }
            }
            success = true;
            return tempFile;
        } finally {
            if (!success) {
                deleteQuietly(runContext, tempFile);
            }
        }
    }

    private static void downloadAndValidate(RunContext runContext, Blob blob, File tempFile) throws IOException {
        String expectedCrc32c = blob.getCrc32c();
        String expectedMd5 = blob.getMd5();
        boolean useCrc32c = expectedCrc32c != null;

        if (expectedCrc32c == null && expectedMd5 == null) {
            throw new IOException(
                "Checksum validation requested but blob 'gs://" + blob.getBucket() + "/" + blob.getName()
                    + "' has neither CRC32C nor MD5 metadata."
            );
        }

        CRC32C crc = useCrc32c ? new CRC32C() : null;
        MessageDigest md5 = useCrc32c ? null : md5Digest();

        // Equality on a server-provided integrity checksum is not a secret comparison,
        // so a plain String.equals is appropriate (no timing-attack concern).
        try (
            ReadChannel readChannel = blob.reader();
            InputStream inputStream = Channels.newInputStream(readChannel);
            OutputStream out = new FileOutputStream(tempFile)
        ) {
            byte[] buffer = new byte[COPY_BUFFER_SIZE];
            int n;
            while ((n = inputStream.read(buffer)) != -1) {
                if (useCrc32c) {
                    crc.update(buffer, 0, n);
                } else {
                    md5.update(buffer, 0, n);
                }
                out.write(buffer, 0, n);
            }
        }

        String expected = useCrc32c ? expectedCrc32c : expectedMd5;
        String actual = useCrc32c
            ? encodeCrc32c((int) crc.getValue())
            : Base64.getEncoder().encodeToString(md5.digest());
        String algorithm = useCrc32c ? "CRC32C" : "MD5";

        if (!expected.equals(actual)) {
            throw new IOException(String.format(
                "Checksum mismatch for gs://%s/%s (%s): expected=%s actual=%s",
                blob.getBucket(), blob.getName(), algorithm, expected, actual
            ));
        }

        Long expectedSize = blob.getSize();
        if (expectedSize != null && tempFile.length() != expectedSize) {
            throw new IOException(String.format(
                "Downloaded size mismatch for gs://%s/%s: expected=%d actual=%d",
                blob.getBucket(), blob.getName(), expectedSize, tempFile.length()
            ));
        }

        runContext.metric(Counter.of("checksum.validated", 1, "algorithm", algorithm));
    }

    static String encodeCrc32c(int crc32c) {
        // GCS stores CRC32C as base64 of a 4-byte big-endian integer.
        return Base64.getEncoder().encodeToString(
            ByteBuffer.allocate(Integer.BYTES).putInt(crc32c).array()
        );
    }

    private static MessageDigest md5Digest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // MD5 is a required algorithm in every JRE per the JCA spec.
            throw new IllegalStateException("MD5 algorithm unavailable", e);
        }
    }

    private static void deleteQuietly(RunContext runContext, File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (!file.delete()) {
            runContext.logger().warn("Failed to delete temporary file '{}'", file.getAbsolutePath());
        }
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        URI from = encode(runContext, runContext.render(this.from).as(String.class).orElse(null));

        BlobId source = BlobId.of(
            from.getAuthority(),
            blobPath(from.getPath().substring(1))
        );

        boolean rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);

        File tempFile = download(runContext, connection, source, rValidateChecksum);
        logger.debug("Download from '{}'", from);

        return Output
            .builder()
            .bucket(source.getBucket())
            .path(source.getName())
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Bucket"
        )
        private final String bucket;

        @Schema(
            title = "Object path"
        )
        private final String path;

        @Schema(
            title = "Kestra storage URI",
            description = "Internal storage URI where the file was saved"
        )
        private final URI uri;
    }
}
