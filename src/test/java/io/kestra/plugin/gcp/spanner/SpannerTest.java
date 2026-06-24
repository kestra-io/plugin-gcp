package io.kestra.plugin.gcp.spanner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

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
        .withExposedPorts(9010);

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

        String emulatorHost = SPANNER_EMULATOR.getHost() + ":" + SPANNER_EMULATOR.getMappedPort(9010);
        SpannerOptions options = SpannerOptions.newBuilder()
            .setProjectId(PROJECT_ID)
            .setEmulatorHost(emulatorHost)
            .setCredentials(NoCredentials.getInstance())
            .build();
        spanner = options.getService();

        InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
        InstanceConfigId instanceConfig = InstanceConfigId.of(PROJECT_ID, "emulator-config");
        InstanceInfo instanceInfo = InstanceInfo.newBuilder(InstanceId.of(PROJECT_ID, INSTANCE_ID))
            .setInstanceConfigId(instanceConfig)
            .setDisplayName("Test Instance")
            .setNodeCount(1)
            .build();
        try {
            instanceAdminClient.createInstance(instanceInfo).get();
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof SpannerException && ((SpannerException) cause).getErrorCode() == ErrorCode.ALREADY_EXISTS)) {
                throw e;
            }
        }

        DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
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
            Throwable cause = e.getCause();
            if (!(cause instanceof SpannerException && ((SpannerException) cause).getErrorCode() == ErrorCode.ALREADY_EXISTS)) {
                throw e;
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
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));
        dbClient.readWriteTransaction().run(transaction -> {
            transaction.executeUpdate(Statement.of("DELETE FROM users WHERE true"));
            return null;
        });
    }

    @Test
    void executeAndQueryTask() throws Exception {
        RunContext runContext = runContextFactory.of();

        Execute executeTask = Execute.builder()
            .id("execute")
            .type(Execute.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("INSERT INTO users (id, name, age) VALUES (@id, @name, @age)"))
            .parameters(Property.ofValue(Map.of("id", 1L, "name", "John Doe", "age", 30L)))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        Execute.Output executeOutput = executeTask.run(runContext);
        assertThat(executeOutput.getAffectedRows(), is(1L));

        Query queryTask = Query.builder()
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

        Query.Output queryOutput = queryTask.run(runContext);
        assertThat(queryOutput.getSize(), is(1L));
        assertThat(queryOutput.getRow(), is(notNullValue()));
        assertThat(queryOutput.getRow().get("name"), is("John Doe"));
        assertThat(queryOutput.getRow().get("age"), is(30L));

        Query queryFetchTask = Query.builder()
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

        Query.Output queryFetchOutput = queryFetchTask.run(runContext);
        assertThat(queryFetchOutput.getSize(), is(1L));
        assertThat(queryFetchOutput.getRows(), is(notNullValue()));
        assertThat(queryFetchOutput.getRows().size(), is(1));
        assertThat(queryFetchOutput.getRows().get(0).get("name"), is("John Doe"));
        assertThat(queryFetchOutput.getRows().get(0).get("age"), is(30L));

        Query queryStoreTask = Query.builder()
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

        Query.Output queryStoreOutput = queryStoreTask.run(runContext);
        assertThat(queryStoreOutput.getSize(), is(1L));
        assertThat(queryStoreOutput.getUri(), is(notNullValue()));

        List<Map<String, Object>> storedRows = new ArrayList<>();
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
        RunContext runContext = runContextFactory.of();

        Execute ddlTask = Execute.builder()
            .id("execute-ddl")
            .type(Execute.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .sql(Property.ofValue("CREATE TABLE ddl_test_table (id INT64 NOT NULL) PRIMARY KEY (id)"))
            .isDdl(Property.ofValue(true))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        Execute.Output ddlOutput = ddlTask.run(runContext);
        assertThat(ddlOutput, is(notNullValue()));

        Execute dropDdlTask = Execute.builder()
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
        RunContext runContext = runContextFactory.of();

        BatchDml batchTask = BatchDml.builder()
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

        BatchDml.Output output = batchTask.run(runContext);
        assertThat(output.getTotalAffectedRows(), is(2L));
        assertThat(output.getAffectedRows(), contains(1L, 1L));
    }

    @Test
    void createAndDeleteDatabaseTask() throws Exception {
        RunContext runContext = runContextFactory.of();
        String tempDbId = "temp-db-" + System.currentTimeMillis();

        CreateDatabase createDbTask = CreateDatabase.builder()
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

        CreateDatabase.Output createOutput = createDbTask.run(runContext);
        assertThat(createOutput.getDatabase(), is(tempDbId));

        DeleteDatabase deleteDbTask = DeleteDatabase.builder()
            .id("delete-db")
            .type(DeleteDatabase.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(tempDbId))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        DeleteDatabase.Output deleteOutput = deleteDbTask.run(runContext);
        assertThat(deleteOutput.getDatabase(), is(tempDbId));
    }

    @Test
    void triggerTask() throws Exception {
        DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));
        dbClient.readWriteTransaction().run(transaction -> {
            transaction.executeUpdate(Statement.of("INSERT INTO users (id, name, age) VALUES (99, 'Trigger User', 40)"));
            return null;
        });

        Trigger trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .projectId(Property.ofValue(PROJECT_ID))
            .instanceId(Property.ofValue(INSTANCE_ID))
            .databaseId(Property.ofValue(DATABASE_ID))
            .changeStreamName(Property.ofValue("users_stream"))
            .lookback(Property.ofValue(java.time.Duration.ofSeconds(10)))
            .emulatorHost(Property.ofValue(getEmulatorHost()))
            .build();

        java.util.Map.Entry<io.kestra.core.models.conditions.ConditionContext, io.kestra.core.models.triggers.Trigger> triggerContext = io.kestra.core.utils.TestsUtils
            .mockTrigger(runContextFactory, trigger);

        Optional<io.kestra.core.models.executions.Execution> execution = Optional.ofNullable(
            Await.until(
                throwSupplier(() -> trigger.evaluate(triggerContext.getKey(), triggerContext.getValue()).orElse(null)),
                java.time.Duration.ofMillis(500),
                java.time.Duration.ofSeconds(15)
            )
        );
        assertThat(execution.isPresent(), is(true));

        Map<String, Object> variables = execution.get().getTrigger().getVariables();
        assertThat(variables.get("changeCount"), is(notNullValue()));
        assertThat(((Number) variables.get("changeCount")).longValue(), is(greaterThanOrEqualTo(1L)));

        String uriString = (String) variables.get("uri");
        assertThat(uriString, is(notNullValue()));
        URI uri = URI.create(uriString);
        assertThat(uri.getScheme(), is("kestra"));
    }
}
