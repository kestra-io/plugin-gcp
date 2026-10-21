package io.kestra.plugin.gcp.gcs;

import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.gcs.models.Blob;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class AbstractGcsTest {
    @Inject
    private RunContextFactory runContextFactory;

    private RunContext runContext() {
        return runContextFactory.of(Map.of("bucket", "test-bucket"));
    }

    @Test
    void shouldEncodeHivePartitionPath() throws Exception {
        var runContext = runContext();
        var path = "gs://{{ bucket }}/hive_date=[2024-01-01]/data#1.csv";

        var uri = AbstractGcs.encode(runContext, path);

        assertThat(uri, notNullValue());
        assertThat(uri.getScheme(), is("gs"));
        assertThat(uri.getAuthority(), is("test-bucket"));
        assertThat(uri.toString(), is("gs://test-bucket/hive_date=%5B2024-01-01%5D/data%231.csv"));
        assertThat(uri.getPath(), is("/hive_date=[2024-01-01]/data#1.csv"));
        assertThat(AbstractGcs.blobPath(uri.getPath().substring(1)), is("hive_date=[2024-01-01]/data#1.csv"));
    }

    @Test
    void shouldEncodeSpacesInPath() throws Exception {
        var runContext = runContext();
        var path = "gs://test-bucket/path with spaces/file.csv";

        var uri = AbstractGcs.encode(runContext, path);

        assertThat(uri, notNullValue());
        assertThat(uri.toString(), is("gs://test-bucket/path%20with%20spaces/file.csv"));
        assertThat(uri.getPath(), is("/path with spaces/file.csv"));
        assertThat(AbstractGcs.blobPath(uri.getPath().substring(1)), is("path with spaces/file.csv"));
    }

    @Test
    void shouldNotDoubleEncodePreEncodedPath() throws Exception {
        var runContext = runContext();
        var path = "gs://test-bucket/path%20with%20encoded/data%5B0%5D.csv";

        var uri = AbstractGcs.encode(runContext, path);

        assertThat(uri, notNullValue());
        assertThat(uri.toString(), is("gs://test-bucket/path%20with%20encoded/data%5B0%5D.csv"));
        assertThat(uri.getPath(), is("/path with encoded/data[0].csv"));
        assertThat(AbstractGcs.blobPath(uri.getPath().substring(1)), is("path with encoded/data[0].csv"));
    }

    @Test
    void shouldHandleBarePercent() throws Exception {
        var runContext = runContext();
        var path = "gs://test-bucket/sales_100%_growth.csv";

        var uri = AbstractGcs.encode(runContext, path);

        assertThat(uri, notNullValue());
        assertThat(uri.toString(), is("gs://test-bucket/sales_100%25_growth.csv"));
        assertThat(uri.getPath(), is("/sales_100%_growth.csv"));
    }

    @Test
    void shouldEncodeSurrogatePairsAndEmoji() throws Exception {
        var runContext = runContext();
        var path = "gs://test-bucket/data_\uD83D\uDE80_rocket.csv";

        var uri = AbstractGcs.encode(runContext, path);

        assertThat(uri, notNullValue());
        assertThat(uri.toString(), is("gs://test-bucket/data_%F0%9F%9A%80_rocket.csv"));
        assertThat(uri.getPath(), is("/data_\uD83D\uDE80_rocket.csv"));
        assertThat(AbstractGcs.blobPath(uri.getPath().substring(1)), is("data_\uD83D\uDE80_rocket.csv"));
    }

    @Test
    void shouldEncodeSpecialCharactersAndAccents() throws Exception {
        var runContext = runContext();
        var path = "gs://test-bucket/special_{curly}_and_^caret|pipe/file_é.csv";

        var uri = AbstractGcs.encode(runContext, path);

        assertThat(uri, notNullValue());
        assertThat(uri.toString(), is("gs://test-bucket/special_%7Bcurly%7D_and_%5Ecaret%7Cpipe/file_%C3%A9.csv"));
        assertThat(uri.getPath(), is("/special_{curly}_and_^caret|pipe/file_é.csv"));
        assertThat(AbstractGcs.blobPath(uri.getPath().substring(1)), is("special_{curly}_and_^caret|pipe/file_é.csv"));
    }

    @Test
    void shouldHandleInternalStorageUri() throws Exception {
        var runContext = runContext();
        var path = "kestra:///my/internal/storage/file[1].csv";

        var uri = AbstractGcs.encode(runContext, path);

        assertThat(uri, notNullValue());
        assertThat(uri.getScheme(), is("kestra"));
        assertThat(uri.toString(), is("kestra:///my/internal/storage/file%5B1%5D.csv"));
        assertThat(uri.getPath(), is("/my/internal/storage/file[1].csv"));
    }

    @Test
    void shouldEncodeBareBlobName() {
        var blobName = "hive_date=[2024-01-01]/data#1.csv";
        var encoded = AbstractGcs.encodePath(blobName);

        assertThat(encoded, is("hive_date=%5B2024-01-01%5D/data%231.csv"));
    }

    @Test
    void shouldEncodeBareBlobNameWithSchemeLikePrefix() {
        var blobName = "http://host/a[1]";
        var encoded = AbstractGcs.encodePath(blobName);

        assertThat(encoded, is("http://host/a%5B1%5D"));
    }

    @Test
    void shouldHandleNullBlob() {
        assertThat(AbstractGcs.encode((String) null), nullValue());
        assertThat(AbstractGcs.encodePath((String) null), nullValue());
    }

    @Test
    void shouldConstructBlobUriWithoutCrash() {
        var mockBlob = Mockito.mock(com.google.cloud.storage.Blob.class);
        Mockito.when(mockBlob.getBucket()).thenReturn("my-bucket");
        Mockito.when(mockBlob.getName()).thenReturn("hive_date=[2024-01-01]/data.csv");

        var uri = Blob.uri(mockBlob);

        assertThat(uri, notNullValue());
        assertThat(uri.toString(), is("gs://my-bucket/hive_date=%5B2024-01-01%5D/data.csv"));
        assertThat(uri.getPath(), is("/hive_date=[2024-01-01]/data.csv"));
    }

    @Test
    void shouldFilterWithRawAndEncodedRegex() {
        var mockBlob = Mockito.mock(com.google.cloud.storage.Blob.class);
        Mockito.when(mockBlob.getBucket()).thenReturn("my-bucket");
        Mockito.when(mockBlob.getName()).thenReturn("hive_date=[2024-01-01]/data.csv");

        var task = List.builder()
            .from(Property.ofValue("gs://my-bucket/"))
            .build();

        assertThat(task.filter(mockBlob, ".*hive_date=\\[2024-01-01\\].*"), is(true));
        assertThat(task.filter(mockBlob, ".*hive_date=%5B2024-01-01%5D.*"), is(true));
        assertThat(task.filter(mockBlob, ".*2025.*"), is(false));
    }

    @Test
    void shouldRoundTripOutputUriWithSpaceAndBracketIntoDownstreamTasks() throws Exception {
        var runContext = runContext();
        // 1. Upstream task (Upload / Copy / Compose / List / Blob.uri) produces encoded output URI
        var originalName = "hive_date=[2024-01-01]/data with spaces.csv";
        var outputUri = URI.create("gs://test-bucket/" + AbstractGcs.encodePath(originalName));

        assertThat(outputUri.toString(), is("gs://test-bucket/hive_date=%5B2024-01-01%5D/data%20with%20spaces.csv"));

        // 2. Downstream task (Delete / Download / Copy) receives the output URI as input string: {{ outputs.upload.uri }}
        var downstreamInput = outputUri.toString();
        var resolvedUri = AbstractGcs.encode(runContext, downstreamInput);

        assertThat(resolvedUri.toString(), is("gs://test-bucket/hive_date=%5B2024-01-01%5D/data%20with%20spaces.csv"));
        assertThat(AbstractGcs.blobPath(resolvedUri.getPath().substring(1)), is(originalName));
    }
}
