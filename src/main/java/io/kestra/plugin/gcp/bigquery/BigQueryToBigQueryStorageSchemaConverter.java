package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.common.collect.ImmutableMap;

/**
 * https://github.com/googleapis/java-bigquerystorage/blob/main/samples/snippets/src/main/java/com/example/bigquerystorage/BqToBqStorageSchemaConverter.java
 */
public class BigQueryToBigQueryStorageSchemaConverter {
    private static final ImmutableMap<Field.Mode, TableFieldSchema.Mode> BQ_TABLE_SCHEMA_MODE_MAP =
        ImmutableMap.of(
            Field.Mode.NULLABLE, TableFieldSchema.Mode.NULLABLE,
            Field.Mode.REPEATED, TableFieldSchema.Mode.REPEATED,
            Field.Mode.REQUIRED, TableFieldSchema.Mode.REQUIRED
        );

    private static ImmutableMap<StandardSQLTypeName, TableFieldSchema.Type> BQ_TABLE_SCHEMA_TYPE_MAP =
        new ImmutableMap.Builder<StandardSQLTypeName, TableFieldSchema.Type>()
            .put(StandardSQLTypeName.BOOL, TableFieldSchema.Type.BOOL)
            .put(StandardSQLTypeName.BYTES, TableFieldSchema.Type.BYTES)
            .put(StandardSQLTypeName.DATE, TableFieldSchema.Type.DATE)
            .put(StandardSQLTypeName.DATETIME, TableFieldSchema.Type.DATETIME)
            .put(StandardSQLTypeName.FLOAT64, TableFieldSchema.Type.DOUBLE)
            .put(StandardSQLTypeName.GEOGRAPHY, TableFieldSchema.Type.GEOGRAPHY)
            .put(StandardSQLTypeName.INT64, TableFieldSchema.Type.INT64)
            .put(StandardSQLTypeName.NUMERIC, TableFieldSchema.Type.NUMERIC)
            .put(StandardSQLTypeName.STRING, TableFieldSchema.Type.STRING)
            .put(StandardSQLTypeName.STRUCT, TableFieldSchema.Type.STRUCT)
            .put(StandardSQLTypeName.TIME, TableFieldSchema.Type.TIME)
            .put(StandardSQLTypeName.TIMESTAMP, TableFieldSchema.Type.TIMESTAMP)
            .put(StandardSQLTypeName.BIGNUMERIC, TableFieldSchema.Type.BIGNUMERIC)
            .put(StandardSQLTypeName.JSON, TableFieldSchema.Type.JSON)
            .put(StandardSQLTypeName.INTERVAL, TableFieldSchema.Type.INTERVAL)
            .build();

    /**
     * Converts from BigQuery client Table Schema to bigquery storage API Table Schema.
     *
     * @param schema the BigQuery client Table Schema
     * @return the bigquery storage API Table Schema
     */
    public static TableSchema convertTableSchema(Schema schema) {
        TableSchema.Builder result = TableSchema.newBuilder();
        for (int i = 0; i < schema.getFields().size(); i++) {
            result.addFields(i, convertFieldSchema(schema.getFields().get(i)));
        }
        return result.build();
    }

    /**
     * Converts from bigquery v2 Field Schema to bigquery storage API Field Schema.
     *
     * @param field the BigQuery client Field Schema
     * @return the bigquery storage API Field Schema
     */
    public static TableFieldSchema convertFieldSchema(Field field) {
        TableFieldSchema.Builder result = TableFieldSchema.newBuilder();
        if (field.getMode() == null) {
            field = field.toBuilder().setMode(Field.Mode.NULLABLE).build();
        }
        result.setMode(BQ_TABLE_SCHEMA_MODE_MAP.get(field.getMode()));
        result.setName(field.getName());
        result.setType(BQ_TABLE_SCHEMA_TYPE_MAP.get(field.getType().getStandardType()));
        if (field.getDescription() != null) {
            result.setDescription(field.getDescription());
        }
        if (field.getSubFields() != null) {
            for (int i = 0; i < field.getSubFields().size(); i++) {
                result.addFields(i, convertFieldSchema(field.getSubFields().get(i)));
            }
        }
        return result.build();
    }
}
