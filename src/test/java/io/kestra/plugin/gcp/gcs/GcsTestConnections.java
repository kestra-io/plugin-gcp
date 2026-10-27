package io.kestra.plugin.gcp.gcs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import org.mockito.AdditionalAnswers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;

/**
 * Supplies Floci clients to real GCS task spies and nested tasks.
 *
 * Mockito construction scopes are thread-local, so callers must open the scope on the same thread that
 * constructs nested tasks. Each intercepted nested task delegates to a spy of a real task rebuilt from
 * the original Lombok builder, preserving configured state and real behavior while replacing only its
 * GCS connection.
 */
final class GcsTestConnections {
    private GcsTestConnections() {
    }

    static Scope useFloci() {
        return useFloci(AbstractFlociGcpTest.flociEndpoint());
    }

    @SafeVarargs
    static Scope useFloci(String endpoint, Class<? extends AbstractGcs>... nestedTaskTypes) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("Floci endpoint must not be blank");
        }

        return new Scope(endpoint, nestedTaskTypes);
    }

    static final class Scope implements AutoCloseable {
        private final String endpoint;
        private final java.util.List<MockedConstruction<?>> constructions = new ArrayList<>();
        private final Queue<Storage> clients = new ConcurrentLinkedQueue<>();
        private boolean closed;

        private Scope(String endpoint, Class<? extends AbstractGcs>[] nestedTaskTypes) {
            this.endpoint = endpoint;

            try {
                for (Class<? extends AbstractGcs> taskType : nestedTaskTypes) {
                    constructions.add(mockTaskConstruction(taskType));
                }
            } catch (RuntimeException | Error e) {
                try {
                    close();
                } catch (RuntimeException closeError) {
                    e.addSuppressed(closeError);
                }
                throw e;
            }
        }

        <T extends AbstractGcs> T spy(T task) {
            if (closed) {
                throw new IllegalStateException("Floci test connection scope is closed");
            }

            T spy = Mockito.spy(task);
            stubConnection(spy);
            return spy;
        }

        private MockedConstruction<?> mockTaskConstruction(Class<? extends AbstractGcs> taskType) {
            // Construct a subtype from the intercepted builder; builder.build() would re-enter this same mock.
            if (taskType == List.class) {
                return mockNestedTask(
                    List.class,
                    context -> new FlociList((List.ListBuilder<?, ?>) context.arguments().get(0))
                );
            }
            if (taskType == Copy.class) {
                return mockNestedTask(
                    Copy.class,
                    context -> new FlociCopy((Copy.CopyBuilder<?, ?>) context.arguments().get(0))
                );
            }
            if (taskType == Delete.class) {
                return mockNestedTask(
                    Delete.class,
                    context -> new FlociDelete((Delete.DeleteBuilder<?, ?>) context.arguments().get(0))
                );
            }

            throw new IllegalArgumentException("Unsupported nested GCS task: " + taskType.getName());
        }

        private <T extends AbstractGcs> MockedConstruction<T> mockNestedTask(
            Class<T> taskType,
            Function<MockedConstruction.Context, T> buildTask) {
            return Mockito.mockConstruction(
                taskType,
                context ->
                {
                    T nestedTask = Mockito.spy(buildTask.apply(context));
                    stubConnection(nestedTask);

                    return Mockito.withSettings().defaultAnswer(AdditionalAnswers.delegatesTo(nestedTask));
                }
            );
        }

        private void stubConnection(AbstractGcs task) {
            try {
                Mockito.doAnswer(invocation -> connection(task, invocation.getArgument(0)))
                    .when(task)
                    .connection(Mockito.any(RunContext.class));
            } catch (IOException | IllegalVariableEvaluationException e) {
                throw new IllegalStateException("Could not stub the Floci test connection", e);
            }
        }

        private Storage connection(AbstractGcs task, RunContext runContext)
            throws IOException, IllegalVariableEvaluationException {
            Storage storage = StorageOptions.newBuilder()
                .setHost(endpoint)
                .setCredentials(NoCredentials.getInstance())
                .setProjectId(runContext.render(task.getProjectId()).as(String.class).orElse(null))
                .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
                .build()
                .getService();

            clients.add(storage);
            return storage;
        }

        private class FlociList extends List {
            private FlociList(List.ListBuilder<?, ?> builder) {
                super(builder);
            }
        }

        private class FlociCopy extends Copy {
            private FlociCopy(Copy.CopyBuilder<?, ?> builder) {
                super(builder);
            }
        }

        private class FlociDelete extends Delete {
            private FlociDelete(Delete.DeleteBuilder<?, ?> builder) {
                super(builder);
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;

            RuntimeException failure = null;
            for (int i = constructions.size() - 1; i >= 0; i--) {
                try {
                    constructions.get(i).close();
                } catch (RuntimeException e) {
                    failure = addFailure(failure, e);
                }
            }
            constructions.clear();

            for (Storage client : clients) {
                try {
                    client.close();
                } catch (Exception e) {
                    failure = addFailure(failure, new IllegalStateException("Could not close Floci GCS client", e));
                }
            }
            clients.clear();

            if (failure != null) {
                throw failure;
            }
        }

        private RuntimeException addFailure(RuntimeException failure, RuntimeException next) {
            if (failure == null) {
                return next;
            }

            failure.addSuppressed(next);
            return failure;
        }
    }
}
