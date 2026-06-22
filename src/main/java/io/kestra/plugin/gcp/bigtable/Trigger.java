package io.kestra.plugin.gcp.bigtable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.GcpInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for new rows in a Google Cloud Bigtable table and trigger a flow execution.",
    description = "Polls a Bigtable table at the configured interval and triggers a downstream execution " +
        "when rows matching the configured row key range/prefix are found. As with other Kestra polling " +
        "triggers, scope the range/prefix to avoid re-triggering on the same rows across polls."
)
@Plugin(
    examples = {
        @Example(
            title = "React to new Bigtable rows every 5 minutes.",
            full = true,
            code = """
                id: bigtable_new_rows_trigger
                namespace: company.team

                triggers:
                  - id: on_new_rows
                    type: io.kestra.plugin.gcp.bigtable.Trigger
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: sensor-readings
                    rowKeyPrefix: "device-42#"
                    interval: PT5M

                tasks:
                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.rowCount }} new rows detected"
                """
        )
    }
)
public class Trigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, GcpInterface {

    @NotNull
    @Schema(title = "The GCP project ID.")
    @PluginProperty(group = "connection")
    private Property<String> projectId;

    @Schema(title = "The GCP service account.")
    @PluginProperty(secret = true, group = "connection")
    private Property<String> serviceAccount;

    @Schema(title = "The GCP service account to impersonate.")
    @PluginProperty(secret = true, group = "advanced")
    private Property<String> impersonatedServiceAccount;

    @Schema(title = "The GCP scopes to be used.")
    @PluginProperty(group = "advanced")
    private Property<List<String>> scopes;

    @NotNull
    @Schema(
        title = "The Bigtable instance ID.",
        description = "An instance is a container for your tables within a given GCP project."
    )
    @PluginProperty(group = "connection")
    private Property<String> instanceId;

    @NotNull
    @Schema(title = "The Bigtable table ID to poll.")
    @PluginProperty(group = "source")
    private Property<String> tableId;

    @Schema(
        title = "Row key prefix to scope the poll to.",
        description = "Mutually exclusive with `rowKeyStart`/`rowKeyEnd`. Scope this narrowly enough " +
            "(e.g. to a time-bucketed key segment) to avoid matching rows already seen on a previous poll."
    )
    @PluginProperty(group = "source")
    private Property<String> rowKeyPrefix;

    @Schema(title = "Inclusive start of the row key range to scan.")
    @PluginProperty(group = "source")
    private Property<String> rowKeyStart;

    @Schema(title = "Exclusive end of the row key range to scan.")
    @PluginProperty(group = "source")
    private Property<String> rowKeyEnd;

    @Schema(
        title = "Only consider cells in this column family newer than the lookback window.",
        description = "When set, only rows with at least one cell in this column family timestamped " +
            "within the lookback window are returned. Requires `lookbackSeconds` to also be set."
    )
    @PluginProperty(group = "processing")
    private Property<String> columnFamily;

    @Schema(
        title = "Lookback window, in seconds, applied to cell timestamps.",
        description = "When set together with `columnFamily`, only rows with a cell newer than " +
            "(now - lookbackSeconds) are returned. This re-fetches anything written within the window " +
            "on every poll, so keep the window close to the polling `interval` to avoid duplicate executions."
    )
    @PluginProperty(group = "processing")
    private Property<Long> lookbackSeconds;

    @Builder.Default
    @Schema(
        title = "The maximum number of rows to return on a single poll.",
        description = "Caps the size of matched rows returned in a single execution to prevent OOM errors."
    )
    @PluginProperty(group = "processing")
    private Property<Integer> maxRows = Property.ofValue(1000);

    @Schema(
        title = "Bigtable emulator host (`host:port`), for local testing only.",
        description = "When set, the trigger connects to a local Bigtable emulator instead of the real Bigtable service."
    )
    @PluginProperty(group = "advanced")
    private Property<String> emulatorHost;

    @Builder.Default
    @Schema(title = "The interval between polls.")
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofSeconds(60);

    @Override
    public Duration getInterval() {
        return this.interval;
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        String rTableId = runContext.render(this.tableId).as(String.class).orElseThrow();
        Optional<String> rPrefix = runContext.render(this.rowKeyPrefix).as(String.class);
        Optional<String> rStart = runContext.render(this.rowKeyStart).as(String.class);
        Optional<String> rEnd = runContext.render(this.rowKeyEnd).as(String.class);
        Optional<String> rColumnFamily = runContext.render(this.columnFamily).as(String.class);
        Optional<Long> rLookbackSeconds = runContext.render(this.lookbackSeconds).as(Long.class);
        Integer rMaxRows = runContext.render(this.maxRows).as(Integer.class).orElse(1000);

        BigtableClientFactory clientFactory = BigtableClientFactory.builder()
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .impersonatedServiceAccount(this.impersonatedServiceAccount)
            .scopes(this.scopes)
            .instanceId(this.instanceId)
            .emulatorHost(this.emulatorHost)
            .build();

        List<Row> matchedRows = new ArrayList<>();

        try (BigtableDataClient client = clientFactory.dataClient(runContext)) {
            Query query = Query.create(rTableId).limit(rMaxRows);

            if (rPrefix.isPresent()) {
                query = query.prefix(rPrefix.get());
            } else if (rStart.isPresent() || rEnd.isPresent()) {
                var range = com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange.unbounded();
                if (rStart.isPresent()) {
                    range = range.startClosed(rStart.get());
                }
                if (rEnd.isPresent()) {
                    range = range.endOpen(rEnd.get());
                }
                query = query.range(range);
            }

            if (rColumnFamily.isPresent() && rLookbackSeconds.isPresent()) {
                long sinceMicros = (System.currentTimeMillis() - (rLookbackSeconds.get() * 1000L)) * 1000L;
                query = query.filter(
                    Filters.FILTERS.chain()
                        .filter(Filters.FILTERS.family().exactMatch(rColumnFamily.get()))
                        .filter(Filters.FILTERS.timestamp().range().startClosed(sinceMicros))
                );
            }

            for (Row row : client.readRows(query)) {
                matchedRows.add(row);
            }
        }

        if (matchedRows.isEmpty()) {
            logger.debug("No new rows found in Bigtable table '{}'", rTableId);
            return Optional.empty();
        }

        logger.info("Found {} new row(s) in Bigtable table '{}'", matchedRows.size(), rTableId);

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Row row : matchedRows) {
            Map<String, Object> rowMap = new LinkedHashMap<>();
            rowMap.put("rowKey", row.getKey().toStringUtf8());
            List<Map<String, Object>> cells = new ArrayList<>();
            for (RowCell cell : row.getCells()) {
                Map<String, Object> cellMap = new LinkedHashMap<>();
                cellMap.put("family", cell.getFamily());
                cellMap.put("qualifier", cell.getQualifier().toStringUtf8());
                cellMap.put("value", cell.getValue().toStringUtf8());
                cellMap.put("timestamp", cell.getTimestamp());
                cells.add(cellMap);
            }
            rowMap.put("cells", cells);
            rows.add(rowMap);
        }

        Output output = Output.builder()
            .rowCount((long) matchedRows.size())
            .rows(rows)
            .build();

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);

        return Optional.of(execution);
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class BigtableClientFactory extends AbstractBigtable {
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of rows matched on this poll.")
        private final Long rowCount;

        @Schema(title = "The matched rows, each with its row key and cells.")
        private final List<Map<String, Object>> rows;
    }
}
