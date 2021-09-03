package io.kestra.plugin.gcp.gcs.models;

import com.google.cloud.storage.BucketInfo;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Logging {
    private String logBucket;
    private String logObjectPrefix;

    public static Logging of(BucketInfo.Logging item) {
        return Logging.builder()
            .logBucket(item.getLogBucket())
            .logObjectPrefix(item.getLogObjectPrefix())
            .build();
    }

    public BucketInfo.Logging convert() {
        return BucketInfo.Logging.newBuilder()
            .setLogBucket(this.logBucket)
            .setLogObjectPrefix(this.logObjectPrefix)
            .build();
    }
}
