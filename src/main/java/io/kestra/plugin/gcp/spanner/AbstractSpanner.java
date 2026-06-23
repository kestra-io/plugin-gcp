package io.kestra.plugin.gcp.spanner;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.stream.Collectors;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Type;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.CredentialService;
import io.kestra.plugin.gcp.GcpInterface;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSpanner extends Task implements GcpInterface {

    @NotNull
    @Schema(title = "The GCP project ID")
    @PluginProperty(group = "connection")
    protected Property<String> projectId;

    @Schema(title = "The GCP service account")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> serviceAccount;

    @Schema(title = "The GCP service account to impersonate")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> impersonatedServiceAccount;

    @Schema(title = "The GCP scopes to be used")
    @PluginProperty(group = "advanced")
    protected Property<List<String>> scopes;

    @NotNull
    @Schema(title = "The Spanner instance ID")
    @PluginProperty(group = "connection")
    protected Property<String> instanceId;

    @NotNull
    @Schema(title = "The Spanner database ID")
    @PluginProperty(group = "connection")
    protected Property<String> databaseId;

    @Schema(
        title = "Spanner emulator host (`host:port`), for local testing only.",
        description = "When set, the task connects to a local Spanner emulator instead of the real Spanner service."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> emulatorHost;

    protected Spanner spannerClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        String rProjectId = runContext.render(this.projectId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required projectId"));
        Optional<String> rEmulatorHost = runContext.render(this.emulatorHost).as(String.class);

        SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder()
            .setProjectId(rProjectId);

        if (rEmulatorHost.isPresent()) {
            optionsBuilder.setEmulatorHost(rEmulatorHost.get());
            optionsBuilder.setCredentials(NoCredentials.getInstance());
        } else {
            optionsBuilder.setCredentials(CredentialService.credentials(runContext, this));
        }

        return optionsBuilder.build().getService();
    }

    protected DatabaseId databaseId(RunContext runContext) throws IllegalVariableEvaluationException {
        String rProjectId = runContext.render(this.projectId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required projectId"));
        String rInstanceId = runContext.render(this.instanceId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required instanceId"));
        String rDatabaseId = runContext.render(this.databaseId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required databaseId"));

        return DatabaseId.of(rProjectId, rInstanceId, rDatabaseId);
    }

    protected void bindParameter(Statement.Builder builder, String name, Object value) {
        ValueBinder<Statement.Builder> binder = builder.bind(name);
        if (value == null) {
            binder.to(Value.string(null));
            return;
        }

        if (value instanceof String) {
            binder.to(Value.string((String) value));
        } else if (value instanceof Boolean) {
            binder.to(Value.bool((Boolean) value));
        } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            binder.to(Value.int64(((Number) value).longValue()));
        } else if (value instanceof Double || value instanceof Float) {
            binder.to(Value.float64(((Number) value).doubleValue()));
        } else if (value instanceof java.math.BigDecimal) {
            binder.to(Value.numeric((java.math.BigDecimal) value));
        } else if (value instanceof com.google.cloud.Timestamp) {
            binder.to((com.google.cloud.Timestamp) value);
        } else if (value instanceof com.google.cloud.Date) {
            binder.to((com.google.cloud.Date) value);
        } else if (value instanceof java.time.Instant) {
            binder.to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(((java.time.Instant) value).getEpochSecond(), ((java.time.Instant) value).getNano()));
        } else if (value instanceof java.time.LocalDate) {
            java.time.LocalDate ld = (java.time.LocalDate) value;
            binder.to(com.google.cloud.Date.fromYearMonthDay(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth()));
        } else if (value instanceof byte[]) {
            binder.to(Value.bytes(com.google.cloud.ByteArray.copyFrom((byte[]) value)));
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                binder.to(Value.stringArray(null));
            } else {
                Object first = list.get(0);
                if (first instanceof String) {
                    binder.to(Value.stringArray(list.stream().map(o -> (String) o).collect(Collectors.toList())));
                } else if (first instanceof Boolean) {
                    binder.to(Value.boolArray(list.stream().map(o -> (Boolean) o).collect(Collectors.toList())));
                } else if (first instanceof Long || first instanceof Integer) {
                    binder.to(Value.int64Array(list.stream().mapToLong(o -> ((Number) o).longValue()).toArray()));
                } else if (first instanceof Double || first instanceof Float) {
                    binder.to(Value.float64Array(list.stream().mapToDouble(o -> ((Number) o).doubleValue()).toArray()));
                } else {
                    binder.to(Value.stringArray(list.stream().map(Object::toString).collect(Collectors.toList())));
                }
            }
        } else {
            try {
                String json = JacksonMapper.ofJson().writeValueAsString(value);
                binder.to(Value.json(json));
            } catch (Exception e) {
                binder.to(Value.string(value.toString()));
            }
        }
    }

    protected Map<String, Object> rowToMap(Struct row) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Type.StructField field : row.getType().getStructFields()) {
            String name = field.getName();
            map.put(name, getValue(row, name, field.getType()));
        }
        return map;
    }

    private Object getValue(Struct row, String columnName, Type type) {
        if (row.isNull(columnName)) {
            return null;
        }

        Type.Code code = type.getCode();
        switch (code) {
            case BOOL:
                return row.getBoolean(columnName);
            case INT64:
                return row.getLong(columnName);
            case FLOAT64:
                return row.getDouble(columnName);
            case NUMERIC:
                return row.getBigDecimal(columnName);
            case STRING:
                return row.getString(columnName);
            case JSON:
                String jsonVal = row.getJson(columnName);
                try {
                    return JacksonMapper.toMap(jsonVal);
                } catch (Exception e) {
                    return jsonVal;
                }
            case BYTES:
                return row.getBytes(columnName).toByteArray();
            case TIMESTAMP:
                return row.getTimestamp(columnName).toSqlTimestamp().toInstant();
            case DATE:
                com.google.cloud.Date date = row.getDate(columnName);
                return java.time.LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
            case ARRAY:
                return getArrayValue(row, columnName, type.getArrayElementType());
            case STRUCT:
                return rowToMap(row.getStruct(columnName));
            default:
                return row.getValue(columnName).toString();
        }
    }

    private Object getArrayValue(Struct row, String columnName, Type elementType) {
        Type.Code elementCode = elementType.getCode();
        switch (elementCode) {
            case BOOL:
                return row.getBooleanList(columnName);
            case INT64:
                return row.getLongList(columnName);
            case FLOAT64:
                return row.getDoubleList(columnName);
            case NUMERIC:
                return row.getBigDecimalList(columnName);
            case STRING:
                return row.getStringList(columnName);
            case JSON:
                return row.getJsonList(columnName);
            case TIMESTAMP:
                return row.getTimestampList(columnName).stream()
                    .map(t -> t.toSqlTimestamp().toInstant())
                    .collect(Collectors.toList());
            case DATE:
                return row.getDateList(columnName).stream()
                    .map(date -> java.time.LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()))
                    .collect(Collectors.toList());
            case STRUCT:
                return row.getStructList(columnName).stream()
                    .map(this::rowToMap)
                    .collect(Collectors.toList());
            default:
                return row.getValue(columnName).toString();
        }
    }
}
