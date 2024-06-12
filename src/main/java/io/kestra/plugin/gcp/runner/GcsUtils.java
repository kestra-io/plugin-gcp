package io.kestra.plugin.gcp.runner;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.runners.TaskCommands;
import io.kestra.core.runners.RunContext;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Utility class for Google Cloud Storage.
 */
public final class GcsUtils {

    private static final int BUFFER_SIZE = 8 * 1024;

    private final GoogleCredentials credentials;
    private final String projectId;

    private GcsUtils(final String projectId,
                     final GoogleCredentials credentials) {
        this.projectId = projectId;
        this.credentials = credentials;
    }

    public static GcsUtils of(final String projectId, final GoogleCredentials credentials) {
        return new GcsUtils(projectId, credentials);
    }

    public void downloadFile(RunContext runContext,
                             TaskCommands taskCommands,
                             List<String> filesToDownload,
                             String bucket,
                             Path workingDirectory,
                             Path outputDirectory,
                             boolean outputDirectoryEnabled) throws Exception {
        try (Storage storage = storage(runContext)) {
            if (filesToDownload != null) {
                for (String relativePath : filesToDownload) {
                    BlobInfo source = BlobInfo.newBuilder(BlobId.of(
                        bucket,
                        removeLeadingSlash(workingDirectory.toString()) + Path.of("/" + relativePath)
                    )).build();
                    try (var fileOutputStream = new FileOutputStream(runContext.resolve(Path.of(relativePath)).toFile());
                         var reader = storage.reader(source.getBlobId())) {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int limit;
                        while ((limit = reader.read(ByteBuffer.wrap(buffer))) >= 0) {
                            fileOutputStream.write(buffer, 0, limit);
                        }
                    }
                }
            }

            if (outputDirectoryEnabled) {
                String outputDirName = normalizeBlobDirName(outputDirectory);
                final Path outputDirPath = Path.of(outputDirName);
                Page<Blob> outputDirEntries = storage.list(bucket, Storage.BlobListOption.prefix(outputDirName));
                outputDirEntries.iterateAll().forEach(blob -> {
                    BlobId blobId = blob.getBlobId();
                    if (!blobId.getName().endsWith("/")) {
                        Path relativeBlobPathFromOutputDir = outputDirPath.relativize(Path.of(blobId.getName()));
                        Path outputFile = taskCommands.getOutputDirectory().resolve(relativeBlobPathFromOutputDir);
                        outputFile.getParent().toFile().mkdirs();
                        storage.downloadTo(blobId, outputFile);
                    }
                });
            }
        }
    }

    public void uploadFiles(RunContext runContext,
                            List<String> filesToUpload,
                            String bucket,
                            Path workingDirectory,
                            Path outputDirectory,
                            boolean outputDirectoryEnabled
                            ) throws Exception {

        try (Storage storage = storage(runContext)) {

            if (outputDirectoryEnabled) {
                var outputDirName = normalizeBlobDirName(outputDirectory);
                storage.create(BlobInfo.newBuilder(BlobId.of(bucket, outputDirName)).build());
            }

            var workingDirName = workingDirectory.toString();
            workingDirName = removeLeadingSlash(workingDirName);
            for (String relativePath : filesToUpload) {
                BlobInfo destination = BlobInfo.newBuilder(BlobId.of(bucket, workingDirName + Path.of("/" + relativePath))).build();
                Path filePath = runContext.resolve(Path.of(relativePath));

                try (var fileInputStream = new FileInputStream(filePath.toFile());
                     var writer = storage.writer(destination)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int limit;
                    while ((limit = fileInputStream.read(buffer)) >= 0) {
                        writer.write(ByteBuffer.wrap(buffer, 0, limit));
                    }
                }
            }
        }
    }

    private static String normalizeBlobDirName(final Path path) {
        var blobName = path.toString();
        blobName = removeLeadingSlash(blobName);
        if (!blobName.endsWith("/")) {
            blobName = blobName + "/";
        }
        return blobName;
    }

    private static String removeLeadingSlash(String outputDirName) {
        if (outputDirName.startsWith("/")) {
            outputDirName = outputDirName.substring(1);  // remove leading '/'
        }
        return outputDirName;
    }

    public void deleteBucket(final RunContext runContext,
                             final String bucket,
                             final String workingDirectoryToBlobPath) throws Exception {
        try (Storage storage = storage(runContext)) {
            Page<Blob> list = storage.list(bucket, Storage.BlobListOption.prefix(workingDirectoryToBlobPath));
            list.iterateAll().forEach(blob -> storage.delete(blob.getBlobId()));
            storage.delete(BlobInfo.newBuilder(BlobId.of(bucket, workingDirectoryToBlobPath)).build().getBlobId());
        }
    }

    public Storage storage(final RunContext runContext) throws IllegalVariableEvaluationException {
        return StorageOptions
            .newBuilder()
            .setCredentials(credentials)
            .setProjectId(runContext.render(projectId))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
    }

}
