package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.JobInfo;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJob extends AbstractBigquery implements AbstractJobInterface {
    protected String destinationTable;

    protected JobInfo.WriteDisposition writeDisposition;

    protected JobInfo.CreateDisposition createDisposition;

    protected Duration jobTimeout;

    protected Map<String, String> labels;

    @Builder.Default
    protected Boolean dryRun = false;
}
