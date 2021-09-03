package io.kestra.plugin.gcp.gcs.models;

import com.google.cloud.storage.BucketInfo;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class IamConfiguration {
    private final Boolean uniformBucketLevelAccessEnabled;
    private final BucketInfo.PublicAccessPrevention publicAccessPrevention;

    public static IamConfiguration of(BucketInfo.IamConfiguration item) {
        return IamConfiguration.builder()
            .uniformBucketLevelAccessEnabled(item.isUniformBucketLevelAccessEnabled())
            .publicAccessPrevention(item.getPublicAccessPrevention())
            .build();
    }

    public BucketInfo.IamConfiguration convert() {
        return BucketInfo.IamConfiguration.newBuilder()
            .setIsUniformBucketLevelAccessEnabled(this.uniformBucketLevelAccessEnabled)
            .setPublicAccessPrevention(this.publicAccessPrevention)
            .build();
    }
}
