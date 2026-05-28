package io.kestra.plugin.gcp.gcs;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32C;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class DownloadChecksumTest {

    @Test
    void encodesEmptyCrc32cAsAllZeroes() {
        // CRC32C of an empty byte sequence is 0 -> base64 of four zero bytes -> "AAAAAA==".
        CRC32C crc = new CRC32C();
        assertThat(Download.encodeCrc32c((int) crc.getValue()), equalTo("AAAAAA=="));
    }

    @Test
    void encodesCrc32cAsBigEndianBase64() {
        CRC32C crc = new CRC32C();
        crc.update("kestra".getBytes(StandardCharsets.UTF_8));
        String encoded = Download.encodeCrc32c((int) crc.getValue());

        // 4 raw bytes -> 8 base64 characters (with padding).
        assertThat(encoded.length(), equalTo(8));
        // Deterministic for the same input.
        assertThat(Download.encodeCrc32c((int) crc.getValue()), equalTo(encoded));
    }
}
