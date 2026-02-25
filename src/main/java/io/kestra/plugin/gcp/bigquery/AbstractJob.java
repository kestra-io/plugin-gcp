package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.util.Map;

import com.google.cloud.bigquery.JobInfo;

import io.kestra.core.models.property.Property;

import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJob extends AbstractBigquery implements AbstractJobInterface {
    protected Property<String> destinationTable;

    protected Property<JobInfo.WriteDisposition> writeDisposition;

    protected Property<JobInfo.CreateDisposition> createDisposition;

    protected Property<Duration> jobTimeout;

    protected Property<Map<String, String>> labels;

    @Builder.Default
    protected Property<Boolean> dryRun = Property.ofValue(false);
}
