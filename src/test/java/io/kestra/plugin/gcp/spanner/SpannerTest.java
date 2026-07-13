package io.kestra.plugin.gcp.spanner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.math.BigDecimal;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.*;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Await;
import static io.kestra.core.utils.Rethrow.throwSupplier;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Execution(ExecutionMode.SAME_THREAD)
class SpannerTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static final String PROJECT_ID = "test-project";
    private static final String INSTANCE_ID = "test-instance";
    private static final String DATABASE_ID = "test-database";

    protected static final GenericContainer<?> SPANNER_EMULATOR = new GenericContainer<>("gcr.io/cloud-spanner-emulator/emulator:latest")
        .withExposedPorts(9010)
        .waitingFor(Wait.forListeningPort());

    private static Spanner spanner;

    protected static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable e) {
            return false;
        }
    }

    @BeforeAll
    static void setUpEmulator() throws Exception {
        Assumptions.assumeTrue(isDockerAvailable(), "Docker is not available, skipping Spanner tests");
        if (!SPANNER_EMULATOR.isRunning()) {
            SPANNER_EMULATOR.start();
        }

        var emulatorHost = SPANNER_EMULATOR.getHost() + ":" + SPANNER_EMULATOR.getMappedPort(9010);
        var options = SpannerOptions.newBuilder()
            .setProjectId(PROJECT_ID)
            .setEmulatorHost(emulatorHost)
            .setCredentials(NoCredentials.getInstance())
            .build();
        spanner = options.getService();

        var instanceAdminClient = spanner.getInstanceAdminClient();
        var instanceConfig = InstanceConfigId.of(PROJECT_ID, "emulator-config");
        var instanceInfo = InstanceInfo.newBuilder(InstanceId.of(PROJECT_ID, INSTANCE_ID))
            .setInstanceConfigId(instanceConfig)
            .setDisplayName("Test Instance")
            .setNodeCount(1)
            .build();
        retryOnUnavailable(() -> {
            try {
                instanceAdminClient.createInstance(instanceInfo).get();
            } catch (Exception e) {
                var cause = e.getCause();
                if (!(cause instanceof SpannerException && ((SpannerException) cause).getErrorCode() == ErrorCode.ALREADY_EXISTS)) {
                    throw e;
                }
            }
        });

        var databaseAdminClient = spanner.getDatabaseAdminClient();
        retryOnUnavailable(() -> {
            try {
                databaseAdminClient.createDatabase(INSTANCE_ID, DATABASE_ID, List.of(
                    "CREATE TABLE users (\n" +
                    "  id INT64 NOT NULL,\n" +
                    "  name STRING(256),\n" +
                    "  age INT64\n" +
                    ") PRIMARY KEY (id)",
                    "CREATE CHANGE STREAM users_stream FOR users"
                )).get();
            } catch (Exception e) {
                var cause = e.getCause();
                if (!(cause instanceof SpannerException && ((SpannerException) cause).getErrorCode() == ErrorCode.ALREADY_EXISTS)) {
                    throw e;
                }
            }
        });
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    // The emulator's TCP port can accept connections before its gRPC service is ready to serve admin
    // requests, so the first create-instance/create-database call can transiently fail with UNAVAILABLE.
    private static void retryOnUnavailable(ThrowingRunnable action) throws Exception {
        var maxAttempts = 5;
        for (var attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                action.run();
                return;
            } catch (Exception e) {
                var cause = e.getCause();
                var isUnavailable = cause instanceof SpannerException && ((SpannerException) cause).getErrorCode() == ErrorCode.UNAVAILABLE;
                if (!isUnavailable || attempt == maxAttempts) {
                    throw e;
                }
                Thread.sleep(500);
            }
        }
    }

    @AfterAll
    static void tearDownEmulator() {
        if (spanner != null) {
            spanner.close();
        }
        if (SPANNER_EMULATOR.isRunning()) {
            SPANNER_EMULATOR.stop();
        }
    }

    private String getEmulatorHost() {
        return SPANNER_EMULATOR.getHost() + ":" + SPANNER_EMULATOR.getMappedPort(9010);
    }

    @BeforeEach
    void cleanTable() {
        var dbClient = spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));
        dbClient.readWriteTransaction().run(transaction -> {
            transaction.executeUpdate(Statement.of("DELETE FROM users WHERE true"));
            return null;
        });
    }

    @Test
    void executeAndQueryTask() throws Exception {
        var runContext = runContextFactory.of();

        var executeTask = Execute.builder()
            .id("execute")
            .type(Execute.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("INSERT INTO users (id, name, age) VALUES (@id, @name, @age)"))
            .parameters(Property.ofValue(Map.of("id", 1L, "name", "John Doe", "age", 30L)))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var executeOutput = executeTask.run(runContext);
        assertThat(executeOutput.getAffectedRows(), is(1L));

        var queryTask = Query.builder()
            .id("query")
            .type(Query.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("SELECT * FROM users WHERE id = @id"))
            .parameters(Property.ofValue(Map.of("id", 1L)))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var queryOutput = queryTask.run(runContext);
        assertThat(queryOutput.getSize(), is(1L));
        assertThat(queryOutput.getRow(), is(notNullValue()));
        assertThat(queryOutput.getRow().get("name"), is("John Doe"));
        assertThat(queryOutput.getRow().get("age"), is(30L));

        var queryFetchTask = Query.builder()
            .id("query-fetch")
            .type(Query.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("SELECT * FROM users WHERE id = @id"))
            .parameters(Property.ofValue(Map.of("id", 1L)))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var queryFetchOutput = queryFetchTask.run(runContext);
        assertThat(queryFetchOutput.getSize(), is(1L));
        assertThat(queryFetchOutput.getRows(), is(notNullValue()));
        assertThat(queryFetchOutput.getRows().size(), is(1));
        assertThat(queryFetchOutput.getRows().get(0).get("name"), is("John Doe"));
        assertThat(queryFetchOutput.getRows().get(0).get("age"), is(30L));

        var queryStoreTask = Query.builder()
            .id("query-store")
            .type(Query.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("SELECT * FROM users WHERE id = @id"))
            .parameters(Property.ofValue(Map.of("id", 1L)))
            .fetchType(Property.ofValue(FetchType.STORE))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var queryStoreOutput = queryStoreTask.run(runContext);
        assertThat(queryStoreOutput.getSize(), is(1L));
        assertThat(queryStoreOutput.getUri(), is(notNullValue()));

        var storedRows = new ArrayList<Map<String, Object>>();
        try (var is = runContext.storage().getFile(queryStoreOutput.getUri())) {
            FileSerde.readAll(is)
                .doOnNext(row -> storedRows.add((Map<String, Object>) row))
                .then()
                .block();
        }
        assertThat(storedRows.size(), is(1));
        assertThat(storedRows.get(0).get("name"), is("John Doe"));
        assertThat(((Number) storedRows.get(0).get("age")).longValue(), is(30L));
    }

    @Test
    void executeDdlTask() throws Exception {
        var runContext = runContextFactory.of();

        var ddlTask = Execute.builder()
            .id("execute-ddl")
            .type(Execute.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("CREATE TABLE ddl_test_table (id INT64 NOT NULL) PRIMARY KEY (id)"))
            .isDdl(Property.ofValue(true))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var ddlOutput = ddlTask.run(runContext);
        assertThat(ddlOutput, is(notNullValue()));

        var dropDdlTask = Execute.builder()
            .id("execute-drop-ddl")
            .type(Execute.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("DROP TABLE ddl_test_table"))
            .isDdl(Property.ofValue(true))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();
        dropDdlTask.run(runContext);
    }

    @Test
    void batchDmlTask() throws Exception {
        var runContext = runContextFactory.of();

        var batchTask = BatchDml.builder()
            .id("batch")
            .type(BatchDml.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .statements(Property.ofValue(List.of(
                "INSERT INTO users (id, name, age) VALUES (10, 'Alice', 25)",
                "INSERT INTO users (id, name, age) VALUES (11, 'Bob', 35)"
            )))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var output = batchTask.run(runContext);
        assertThat(output.getTotalAffectedRows(), is(2L));
        assertThat(output.getAffectedRows(), contains(1L, 1L));
    }

    @Test
    void createAndDeleteDatabaseTask() throws Exception {
        var runContext = runContextFactory.of();
        var tempDbId = "temp_db_test";

        try {
            var deleteDbTask = DeleteDatabase.builder()
                .id("delete-db-pre")
                .type(DeleteDatabase.class.getName())
                .projectId(Property.ofValue(PROJECT_ID))
                .instanceId(Property.ofValue(INSTANCE_ID))
                .databaseId(Property.ofValue(tempDbId))
                .emulatorHost(Property.ofValue(getEmulatorHost()))
                .build();
            deleteDbTask.run(runContext);
        } catch (Exception e) {
        }

        try {
            var createDbTask = CreateDatabase.builder()
                .id("create-db")
                .type(CreateDatabase.class.getName())
                .projectId(Property.ofValue(PROJECT_ID))
                .instanceId(Property.ofValue(INSTANCE_ID))
                .databaseId(Property.ofValue(tempDbId))
                .extraDdl(Property.ofValue(List.of(
                    "CREATE TABLE temp_table (id INT64 NOT NULL) PRIMARY KEY (id)"
                )))
                .emulatorHost(Property.ofValue(getEmulatorHost()))
                .build();

            var createOutput = createDbTask.run(runContext);
            assertThat(createOutput.getDatabase(), is(tempDbId));
        } finally {
            var deleteDbTask = DeleteDatabase.builder()
                .id("delete-db")
                .type(DeleteDatabase.class.getName())
                .projectId(Property.ofValue(PROJECT_ID))
                .instanceId(Property.ofValue(INSTANCE_ID))
                .databaseId(Property.ofValue(tempDbId))
                .emulatorHost(Property.ofValue(getEmulatorHost()))
                .build();

            var deleteOutput = deleteDbTask.run(runContext);
            assertThat(deleteOutput.getDatabase(), is(tempDbId));
        }
    }

    @Test
    void triggerTask() throws Exception {
        var dbClient = spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));
        dbClient.readWriteTransaction().run(transaction -> {
            transaction.executeUpdate(Statement.of("INSERT INTO users (id, name, age) VALUES (99, 'Trigger User', 40)"));
            return null;
        });

        var trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .changeStreamName(Property.ofValue("users_stream"))
            .lookback(Property.ofValue(java.time.Duration.ofSeconds(10)))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var triggerContext = io.kestra.core.utils.TestsUtils
            .mockTrigger(runContextFactory, trigger);

        var execution = Optional.ofNullable(
            Await.until(
                throwSupplier(() -> trigger.evaluate(triggerContext.getKey(), triggerContext.getValue()).orElse(null)),
                java.time.Duration.ofMillis(500),
                java.time.Duration.ofSeconds(15)
            )
        );
        assertThat(execution.isPresent(), is(true));

        var variables = execution.get().getTrigger().getVariables();
        assertThat(variables.get("changeCount"), is(notNullValue()));
        assertThat(((Number) variables.get("changeCount")).longValue(), is(greaterThanOrEqualTo(1L)));

        var uriString = (String) variables.get("uri");
        assertThat(uriString, is(notNullValue()));
        var uri = URI.create(uriString);
        assertThat(uri.getScheme(), is("kestra"));
    }

    @Test
    void triggerTaskInvalidChangeStreamName() {
        var trigger = Trigger.builder()
            .id("trigger-invalid")
            .type(Trigger.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .changeStreamName(Property.ofValue("users stream; DROP TABLE users;"))
            .lookback(Property.ofValue(java.time.Duration.ofSeconds(10)))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var triggerContext = io.kestra.core.utils.TestsUtils.mockTrigger(runContextFactory, trigger);

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
            trigger.evaluate(triggerContext.getKey(), triggerContext.getValue());
        });
    }

    @Test
    void executeWithArrayParameters() throws Exception {
        var runContext = runContextFactory.of();

        var executeTask = Execute.builder()
            .id("execute-array")
            .type(Execute.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("INSERT INTO users (id, name, age) VALUES (100, 'User 100', 20), (101, 'User 101', 25)"))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();
        executeTask.run(runContext);

        var queryTask = Query.builder()
            .id("query-array")
            .type(Query.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("SELECT * FROM users WHERE id IN UNNEST(@ids) ORDER BY id"))
            .parameters(Property.ofValue(Map.of("ids", List.of(100L, 101L))))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var queryOutput = queryTask.run(runContext);
        assertThat(queryOutput.getSize(), is(2L));
        assertThat(queryOutput.getRows().get(0).get("name"), is("User 100"));
        assertThat(queryOutput.getRows().get(1).get("name"), is("User 101"));

        var queryEmptyTask = Query.builder()
            .id("query-empty-array")
            .type(Query.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("SELECT * FROM users WHERE id IN UNNEST(@ids)"))
            .parameters(Property.ofValue(Map.of("ids", List.of())))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var emptyException = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
            queryEmptyTask.run(runContext);
        });
        assertThat(emptyException.getMessage(), containsString("Cannot bind an empty list"));

        var queryMixedTask = Query.builder()
            .id("query-mixed-array")
            .type(Query.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("SELECT * FROM users WHERE id IN UNNEST(@ids)"))
            .parameters(Property.ofValue(Map.of("ids", List.of(100L, "hello"))))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var mixedException = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
            queryMixedTask.run(runContext);
        });
        assertThat(mixedException.getMessage(), containsString("Mixed types in list parameter"));
    }

    @Test
    void executeWithArrayParametersEdgeCases() throws Exception {
        var runContext = runContextFactory.of();

        var queryShortTask = Query.builder()
            .id("query-short-array")
            .type(Query.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("SELECT * FROM users WHERE id IN UNNEST(@ids)"))
            .parameters(Property.ofValue(Map.of("ids", List.of((short) 1, (short) 2))))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();
        queryShortTask.run(runContext);

        var queryNullTask = Query.builder()
            .id("query-null-array")
            .type(Query.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("SELECT * FROM users WHERE id IN UNNEST(@ids)"))
            .parameters(Property.ofValue(Map.of("ids", Arrays.asList(1L, null))))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        var nullException = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
            queryNullTask.run(runContext);
        });
        assertThat(nullException.getMessage(), containsString("Null elements are not supported in array parameter"));
    }

    @Test
    void bindParameterTypesRoundTrip() throws Exception {
        var runContext = runContextFactory.of();

        var createTableTask = Execute.builder()
            .id("create-types-table")
            .type(Execute.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue(
                "CREATE TABLE types_table (\n" +
                "  id INT64 NOT NULL,\n" +
                "  bool_col BOOL,\n" +
                "  double_col FLOAT64,\n" +
                "  numeric_col NUMERIC,\n" +
                "  bytes_col BYTES(MAX),\n" +
                "  timestamp_col TIMESTAMP,\n" +
                "  date_col DATE,\n" +
                "  json_col JSON\n" +
                ") PRIMARY KEY (id)"
            ))
            .isDdl(Property.ofValue(true))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();
        createTableTask.run(runContext);

        try {
            var bytes = new byte[]{1, 2, 3};
            var instant = Instant.parse("2026-06-24T12:00:00Z");
            var localDate = LocalDate.parse("2026-06-24");
            var bigDecimal = new BigDecimal("123.45");
            var jsonMap = Map.of("key", "value");

            var insertTask = Execute.builder()
                .id("insert-types")
                .type(Execute.class.getName())
                .projectId(Property.ofValue(PROJECT_ID))
                .instanceId(Property.ofValue(INSTANCE_ID))
                .databaseId(Property.ofValue(DATABASE_ID))
                .sql(Property.ofValue(
                    "INSERT INTO types_table (id, bool_col, double_col, numeric_col, bytes_col, timestamp_col, date_col, json_col) " +
                    "VALUES (@id, @bool_col, @double_col, @numeric_col, @bytes_col, @timestamp_col, @date_col, @json_col)"
                ))
                .parameters(Property.ofValue(Map.of(
                    "id", 1L,
                    "bool_col", true,
                    "double_col", 3.14,
                    "numeric_col", bigDecimal,
                    "bytes_col", bytes,
                    "timestamp_col", instant,
                    "date_col", localDate,
                    "json_col", jsonMap
                )))
                .emulatorHost(Property.ofValue(getEmulatorHost()))
                .build();
            insertTask.run(runContext);

            var queryTask = Query.builder()
                .id("query-types")
                .type(Query.class.getName())
                .projectId(Property.ofValue(PROJECT_ID))
                .instanceId(Property.ofValue(INSTANCE_ID))
                .databaseId(Property.ofValue(DATABASE_ID))
                .sql(Property.ofValue("SELECT * FROM types_table WHERE id = 1"))
                .fetchType(Property.ofValue(FetchType.FETCH_ONE))
                .emulatorHost(Property.ofValue(getEmulatorHost()))
                .build();

            var queryOutput = queryTask.run(runContext);
            var row = queryOutput.getRow();

            assertThat(row.get("bool_col"), is(true));
            assertThat(row.get("double_col"), is(3.14));
            assertThat(row.get("numeric_col"), is(bigDecimal));
            assertThat((byte[]) row.get("bytes_col"), is(bytes));
            assertThat((Instant) row.get("timestamp_col"), is(instant));
            assertThat((LocalDate) row.get("date_col"), is(localDate));
            assertThat((Map<?, ?>) row.get("json_col"), is(jsonMap));

        } finally {
            var dropTableTask = Execute.builder()
                .id("drop-types-table")
                .type(Execute.class.getName())
                .projectId(Property.ofValue(PROJECT_ID))
                .instanceId(Property.ofValue(INSTANCE_ID))
                .databaseId(Property.ofValue(DATABASE_ID))
                .sql(Property.ofValue("DROP TABLE types_table"))
                .isDdl(Property.ofValue(true))
                .emulatorHost(Property.ofValue(getEmulatorHost()))
                .build();
            dropTableTask.run(runContext);
        }
    }
}
