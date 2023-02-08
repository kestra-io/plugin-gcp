package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.JobInfo;
import java.time.Duration;
import java.util.Map;
import lombok.*;
import lombok.experimental.SuperBuilder;

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

    @Builder.Default protected Boolean dryRun = false;
}
