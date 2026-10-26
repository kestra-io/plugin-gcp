package io.kestra.plugin.gcp.gcs;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.gcs.models.Blob;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class DownloadsUnitTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldPreserveSubpathWhenPrefixEndsWithSlash() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive/",
            "inbound/dept_a/records.csv",
            "inbound/"
        );
        assertThat(targetUri, is("gs://my-bucket/archive/dept_a/records.csv"));
    }

    @Test
    void shouldPreserveSubpathWhenPrefixHasNoTrailingSlash() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "inbound/dept_a/records.csv",
            "inbound"
        );
        assertThat(targetUri, is("gs://my-bucket/archive/inbound/dept_a/records.csv"));
    }

    @Test
    void shouldPreserveNestedSubpaths() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive/",
            "inbound/dept_a/2026/10/records.csv",
            "inbound/"
        );
        assertThat(targetUri, is("gs://my-bucket/archive/dept_a/2026/10/records.csv"));
    }

    @Test
    void shouldKeepFilenameWhenFileDirectlyUnderPrefix() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive/",
            "inbound/records.csv",
            "inbound/"
        );
        assertThat(targetUri, is("gs://my-bucket/archive/records.csv"));
    }

    @Test
    void shouldPreserveSubpathsWhenPrefixIsBucketRoot() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive/",
            "dept_a/records.csv",
            ""
        );
        assertThat(targetUri, is("gs://my-bucket/archive/dept_a/records.csv"));
    }

    @Test
    void shouldFallbackToFilenameWhenPrefixIsNull() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive/",
            "dept_a/records.csv",
            null
        );
        assertThat(targetUri, is("gs://my-bucket/archive/records.csv"));
    }

    @Test
    void shouldPreserveSubpathWhenPrefixDoesNotMatch() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive/",
            "other/records.csv",
            "inbound/"
        );
        assertThat(targetUri, is("gs://my-bucket/archive/other/records.csv"));
    }

    @Test
    void shouldPreserveSubpathWhenPrefixMatchesFolderBase() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive/",
            "incoming/2026-10/records.csv",
            "incoming/2026"
        );
        assertThat(targetUri, is("gs://my-bucket/archive/2026-10/records.csv"));
    }

    @Test
    void shouldPreserveSubpathWhenPrefixHasNoSlashAndMultipleSubfolders() {
        var targetUri1 = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "data-2025/x/r.csv",
            "data"
        );
        var targetUri2 = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "data-2026/x/r.csv",
            "data"
        );
        assertThat(targetUri1, is("gs://my-bucket/archive/data-2025/x/r.csv"));
        assertThat(targetUri2, is("gs://my-bucket/archive/data-2026/x/r.csv"));
    }

    @Test
    void shouldDifferentiateSiblingPrefixes() {
        var targetUri1 = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "incoming/2026/file.csv",
            "incoming/2026"
        );
        var targetUri2 = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "incoming/2026x/file.csv",
            "incoming/2026"
        );
        assertThat(targetUri1, is("gs://my-bucket/archive/2026/file.csv"));
        assertThat(targetUri2, is("gs://my-bucket/archive/2026x/file.csv"));
    }

    @Test
    void shouldDifferentiateSiblingPrefixesWithoutTrailingSlash() {
        var targetUri1 = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "incoming/2026/2026x/file.csv",
            "incoming/2026"
        );
        var targetUri2 = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "incoming/2026x/file.csv",
            "incoming/2026"
        );
        assertThat(targetUri1, is("gs://my-bucket/archive/2026/2026x/file.csv"));
        assertThat(targetUri2, is("gs://my-bucket/archive/2026x/file.csv"));
    }

    @Test
    void shouldStripFullPrefixWhenPrefixEndsWithSlash() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "incoming/2026/file.csv",
            "incoming/2026/"
        );
        assertThat(targetUri, is("gs://my-bucket/archive/file.csv"));
    }

    @Test
    void shouldPreserveFilenameWhenDirectlyTargeted() {
        var targetUri = Downloads.destinationUri(
            "gs://my-bucket/archive",
            "incoming/2026/file.csv",
            "incoming/2026/file.csv"
        );
        assertThat(targetUri, is("gs://my-bucket/archive/file.csv"));
    }

    @Test
    void shouldThrowIllegalStateExceptionOnCollision() {
        var blob1 = Blob.builder()
            .bucket("my-bucket")
            .name("inbound/dept_a/records.csv")
            .build();
        var blob2 = Blob.builder()
            .bucket("my-bucket")
            .name("inbound/dept_b/records.csv")
            .build();

        var task = Downloads.builder()
            .id("test")
            .type(Downloads.class.getName())
            .from(Property.ofValue("gs://my-bucket/inbound/"))
            .build();
        var runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        var ex = assertThrows(IllegalStateException.class, () ->
        {
            Downloads.performAction(
                List.of(blob1, blob2),
                ActionInterface.Action.MOVE,
                Property.ofValue("gs://my-bucket/archive/"),
                null,
                runContext,
                null,
                null,
                null
            );
        });

        assertThat(ex.getMessage(), containsString("multiple source blobs would overwrite the same destination path"));
    }

    @Test
    void shouldSkipDirectoryMarkersOnDelete() {
        var dirMarker = Blob.builder()
            .bucket("my-bucket")
            .name("inbound/dept_a/")
            .isDirectory(true)
            .build();

        var task = Downloads.builder()
            .id("test")
            .type(Downloads.class.getName())
            .from(Property.ofValue("gs://my-bucket/inbound/"))
            .build();
        var runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        assertDoesNotThrow(() ->
        {
            Downloads.performAction(
                List.of(dirMarker),
                ActionInterface.Action.DELETE,
                null,
                null,
                runContext,
                null,
                null,
                null
            );
        });
    }
}
