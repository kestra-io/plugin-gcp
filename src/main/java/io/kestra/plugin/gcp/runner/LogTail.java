package io.kestra.plugin.gcp.runner;

import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.LogEntryServerStream;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class LogTail implements AutoCloseable {
    private final LogEntryServerStream stream;
    private final ExecutorService executorService;
    private final Duration waitForLogInterval;

    LogTail(LogEntryServerStream stream, AbstractLogConsumer logConsumer, Duration waitForLogInterval) {
        this.stream = stream;
        this.waitForLogInterval = waitForLogInterval;
        this.executorService = Executors.newSingleThreadExecutor();

        this.executorService.submit(
            () -> {
                for (LogEntry entry : this.stream) {
                    logConsumer.accept(entry.<Payload.StringPayload>getPayload().getData(), isError(entry.getSeverity()));
                }
            }
        );
    }

    private boolean isError(Severity severity) {
        return severity == Severity.ERROR || severity == Severity.CRITICAL || severity == Severity.EMERGENCY;
    }


    @Override
    public void close() {
        this.executorService.shutdown();

        // sleep 1s before cancelling the stream to wait for late logs
        try {
            Thread.sleep(waitForLogInterval.toMillis());
        } catch (InterruptedException e) {
            // if we are interrupted, do nothing.
        }

        this.stream.cancel();
    }
}
