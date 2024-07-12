package io.kestra.plugin.gcp.runner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.LogEntryServerStream;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.serializers.JacksonMapper;

import java.time.Duration;
import java.util.concurrent.CancellationException;

class LogTail implements AutoCloseable {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    private final LogEntryServerStream stream;
    private final Duration waitForLogInterval;
    private final Thread logThread;

    LogTail(LogEntryServerStream stream, AbstractLogConsumer logConsumer, Duration waitForLogInterval) {
        this.stream = stream;
        this.waitForLogInterval = waitForLogInterval;

        logThread = Thread.ofVirtual().start(
            () -> {
                try {
                    for (LogEntry entry : this.stream) {
                        if (entry.getPayload() instanceof Payload.StringPayload str) {
                            logConsumer.accept(str.getData(), isError(entry.getSeverity()));
                        } else if (entry.getPayload() instanceof Payload.JsonPayload json) {
                            try {
                                String strPayload = MAPPER.writeValueAsString(json.getDataAsMap());
                                logConsumer.accept(strPayload, isError(entry.getSeverity()));
                            } catch (JsonProcessingException e) {
                                logConsumer.accept("Unable to parse JSON log message: " + e.getMessage(), true);
                            }
                        } else {
                            logConsumer.accept("Unable to process a log payload of type: " + entry.getPayload().getClass().getName(), true);
                        }
                    }
                } catch (CancellationException e) {
                    // when the stream is canceled, a cancellation exception is throws
                    // we ignore it as ... well ... we cancel the stream, so we cause this
                }
            }
        );
    }

    private boolean isError(Severity severity) {
        return severity == Severity.ERROR || severity == Severity.CRITICAL || severity == Severity.EMERGENCY;
    }


    @Override
    public void close() throws InterruptedException {
        // sleep 1s before cancelling the stream to wait for late logs
        try {
            Thread.sleep(waitForLogInterval.toMillis());
        } catch (InterruptedException e) {
            // if we are interrupted, do nothing.
        }

        this.stream.cancel();
        this.logThread.join();
    }
}
