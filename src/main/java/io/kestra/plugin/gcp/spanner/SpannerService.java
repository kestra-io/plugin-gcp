package io.kestra.plugin.gcp.spanner;

import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.math.BigDecimal;

import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.Date;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Type;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.CredentialService;
import io.kestra.core.serializers.JacksonMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class SpannerService {
    public static Spanner spannerClient(RunContext runContext, SpannerConnectionInterface connection) throws IllegalVariableEvaluationException, IOException {
        String rProjectId = runContext.render(connection.getProjectId()).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required projectId"));
        Optional<String> rEmulatorHost = runContext.render(connection.getEmulatorHost()).as(String.class);

        SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder()
            .setProjectId(rProjectId);

        if (rEmulatorHost.isPresent()) {
            optionsBuilder.setEmulatorHost(rEmulatorHost.get());
            optionsBuilder.setCredentials(NoCredentials.getInstance());
        } else {
            optionsBuilder.setCredentials(CredentialService.credentials(runContext, connection));
        }

        return optionsBuilder.build().getService();
    }

    public static DatabaseId databaseId(RunContext runContext, SpannerConnectionInterface connection) throws IllegalVariableEvaluationException {
        String rProjectId = runContext.render(connection.getProjectId()).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required projectId"));
        String rInstanceId = runContext.render(connection.getInstanceId()).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required instanceId"));
        String rDatabaseId = runContext.render(connection.getDatabaseId()).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required databaseId"));

        return DatabaseId.of(rProjectId, rInstanceId, rDatabaseId);
    }

    public static void bindParameter(Statement.Builder builder, String name, Object value) {
        ValueBinder<Statement.Builder> binder = builder.bind(name);
        switch (value) {
            case null -> binder.to(Value.string(null));
            case String s -> binder.to(Value.string(s));
            case Boolean b -> binder.to(Value.bool(b));
            case Long l -> binder.to(Value.int64(l));
            case Integer i -> binder.to(Value.int64(i.longValue()));
            case Short sh -> binder.to(Value.int64(sh.longValue()));
            case Byte by -> binder.to(Value.int64(by.longValue()));
            case Double d -> binder.to(Value.float64(d));
            case Float f -> binder.to(Value.float64(f.doubleValue()));
            case BigDecimal bd -> binder.to(Value.numeric(bd));
            case Timestamp ts -> binder.to(ts);
            case Date dt -> binder.to(dt);
            case Instant inst -> binder.to(Timestamp.ofTimeSecondsAndNanos(inst.getEpochSecond(), inst.getNano()));
            case LocalDate ld -> binder.to(Date.fromYearMonthDay(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth()));
            case byte[] bytes -> binder.to(Value.bytes(ByteArray.copyFrom(bytes)));
            case List<?> list -> bindList(binder, name, list);
            default -> {
                try {
                    String json = JacksonMapper.ofJson().writeValueAsString(value);
                    binder.to(Value.json(json));
                } catch (JsonProcessingException e) {
                    binder.to(Value.string(value.toString()));
                }
            }
        }
    }

    private static void bindList(ValueBinder<Statement.Builder> binder, String name, List<?> list) {
        if (list.isEmpty()) {
            throw new IllegalArgumentException(
                "Cannot bind an empty list for parameter '" + name + "': "
                + "Spanner arrays are typed and the element type cannot be inferred from an empty list. "
                + "Omit the parameter or pass a typed/non-empty value.");
        }
        for (var obj : list) {
            if (obj == null) {
                throw new IllegalArgumentException("Null elements are not supported in array parameter '" + name + "'");
            }
        }
        var first = list.get(0);
        if (first instanceof String) {
            binder.to(Value.stringArray(castList(list, String.class, name)));
        } else if (first instanceof Boolean) {
            binder.to(Value.boolArray(castList(list, Boolean.class, name)));
        } else if (first instanceof Long || first instanceof Integer || first instanceof Short || first instanceof Byte) {
            binder.to(Value.int64Array(toLongArray(list, name)));
        } else if (first instanceof Double || first instanceof Float) {
            binder.to(Value.float64Array(toDoubleArray(list, name)));
        } else {
            binder.to(Value.stringArray(list.stream().map(Object::toString).toList()));
        }
    }

    private static <T> List<T> castList(List<?> list, Class<T> clazz, String name) {
        var result = new ArrayList<T>(list.size());
        for (var obj : list) {
            if (!clazz.isInstance(obj)) {
                throw new IllegalArgumentException("Mixed types in list parameter '" + name + "': expected " + clazz.getSimpleName() + " but got " + obj.getClass().getSimpleName());
            }
            result.add(clazz.cast(obj));
        }
        return result;
    }

    private static long[] toLongArray(List<?> list, String name) {
        var result = new long[list.size()];
        for (var i = 0; i < list.size(); i++) {
            var obj = list.get(i);
            if (obj instanceof Long || obj instanceof Integer || obj instanceof Short || obj instanceof Byte) {
                result[i] = ((Number) obj).longValue();
            } else {
                throw new IllegalArgumentException("Mixed types in list parameter '" + name + "': expected integer element but got " + obj.getClass().getSimpleName());
            }
        }
        return result;
    }

    private static double[] toDoubleArray(List<?> list, String name) {
        var result = new double[list.size()];
        for (var i = 0; i < list.size(); i++) {
            var obj = list.get(i);
            if (obj instanceof Double || obj instanceof Float) {
                result[i] = ((Number) obj).doubleValue();
            } else {
                throw new IllegalArgumentException("Mixed types in list parameter '" + name + "': expected floating-point element but got " + obj.getClass().getSimpleName());
            }
        }
        return result;
    }

    public static Map<String, Object> rowToMap(Struct row) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Type.StructField field : row.getType().getStructFields()) {
            String name = field.getName();
            map.put(name, getValue(row, name, field.getType()));
        }
        return map;
    }

    private static Object getValue(Struct row, String columnName, Type type) {
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

    private static Object getArrayValue(Struct row, String columnName, Type elementType) {
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
                    .toList();
            case DATE:
                return row.getDateList(columnName).stream()
                    .map(date -> java.time.LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()))
                    .toList();
            case STRUCT:
                return row.getStructList(columnName).stream()
                    .map(SpannerService::rowToMap)
                    .toList();
            default:
                return row.getValue(columnName).toString();
        }
    }
}
