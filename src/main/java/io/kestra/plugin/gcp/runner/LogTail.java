package io.kestra.plugin.gcp.runner;

import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.LogEntryServerStream;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import io.kestra.core.models.script.AbstractLogConsumer;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class LogTail implements AutoCloseable {
    private final LogEntryServerStream stream;
    private final ExecutorService executorService;

    private volatile boolean stopped = false;

    LogTail(LogEntryServerStream stream, AbstractLogConsumer logConsumer) {
        this.stream = stream;
        this.executorService = Executors.newSingleThreadExecutor();

        this.executorService.submit(
            () -> {
                Iterator<LogEntry> it = this.stream.iterator();
                while (it.hasNext() && !stopped) {
                    LogEntry entry = it.next();
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
        this.stopped = true;
        this.stream.cancel();
        this.executorService.shutdown();
    }
}
