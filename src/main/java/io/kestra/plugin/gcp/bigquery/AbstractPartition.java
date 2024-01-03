package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

import static io.kestra.plugin.gcp.bigquery.AbstractPartition.PartitionType.*;
import static io.kestra.plugin.gcp.bigquery.AbstractPartition.PartitionType.YEAR;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractPartition extends AbstractTable {
    private static final Map<AbstractPartition.PartitionType, String> ADDED_DATE = Map.of(
        HOUR, "0000",
        DAY, "000000",
        MONTH, "01000000",
        YEAR, "0101000000"
    );

    @NotNull
    @Schema(
        title = "The partition type of the table"
    )
    @PluginProperty(dynamic = true)
    protected AbstractPartition.PartitionType partitionType;

    @NotNull
    @Schema(
        title = "The inclusive starting date or integer.",
        description = "If the partition :\n" +
            "- is a numeric range, must be a valid integer\n" +
            "- is a date, must a valid datetime like `{{ now() }}`"
    )
    @PluginProperty(dynamic = true)
    protected String from;

    @NotNull
    @Schema(
        title = "The inclusive ending date or integer.",
        description = "If the partition :\n" +
            "- is a numeric range, must be a valid integer\n" +
            "- is a date, must a valid datetime like `{{ now() }}`"
    )
    @PluginProperty(dynamic = true)
    protected String to;

    protected TableId tableId(RunContext runContext, String partition) throws IllegalVariableEvaluationException {
        return this.projectId != null  ?
            TableId.of(runContext.render(this.projectId), runContext.render(this.dataset), runContext.render(this.table) + "$" + partition) :
            TableId.of(runContext.render(this.dataset), runContext.render(this.table) + "$" + partition);
    }

    protected List<String> listPartitions(RunContext runContext, BigQuery connection, TableId tableId) throws IllegalVariableEvaluationException {
        List<String> partitions = connection.listPartitions(tableId);

        if (partitionType == RANGE) {
            int from = Integer.parseInt(runContext.render(this.from));
            int to = Integer.parseInt(runContext.render(this.to));

            return partitions
                .stream()
                .filter(s -> {
                    int current = Integer.parseInt(s);

                    return current >= from &&
                        current <= to;
                })
                .collect(Collectors.toList());
        } else {
            LocalDateTime from = LocalDateTime.parse(runContext.render(this.from), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]"));
            LocalDateTime to = LocalDateTime.parse(runContext.render(this.to), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm[:ss][.SSSSSS][XXX]"));
            return partitions
                .stream()
                .filter(s -> !s.equals("__NULL__"))
                .filter(s -> {
                    LocalDateTime current = LocalDateTime.parse(
                        s + ADDED_DATE.get(partitionType),
                        DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                    );

                    return current.compareTo(from) >= 0 &&
                        current.compareTo(to) <= 0;
                })
                .collect(Collectors.toList());
        }
    }

    public enum PartitionType {
        DAY,
        HOUR,
        MONTH,
        YEAR,
        RANGE
    }
}
