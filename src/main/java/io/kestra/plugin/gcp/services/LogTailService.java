package io.kestra.plugin.gcp.services;

import com.google.auth.Credentials;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.LogEntryServerStream;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.kestra.core.utils.Rethrow.throwRunnable;

public class LogTailService {
    public static Thread tail(Logger logger, String projectId, Credentials credential, String filter, AtomicBoolean stopSignal) throws Exception {
        LoggingOptions options = LoggingOptions.newBuilder()
            .setCredentials(credential)
            .setProjectId(projectId)
            .build();

        Thread thread = new Thread(
            throwRunnable(() -> {
                try (Logging logging = options.getService()) {
                    LogEntryServerStream stream = logging.tailLogEntries(
                        Logging.TailOption.project(projectId),
                        Logging.TailOption.filter(filter)
                    );

                    for (LogEntry logEntry : stream) {
                        LogTailService.log(logger, logEntry);

                        if (stopSignal.get()) {
                            if (stream.isReceiveReady()) {
                                stream.iterator().forEachRemaining(l -> LogTailService.log(logger, l));
                            }

                            stream.cancel();
                        }
                    }
                }
            }),
            "gcp-log-tail"
        );

        thread.start();
        thread.setUncaughtExceptionHandler((t, e) -> {
            if (!(e instanceof InterruptedException)) {
                logger.error("Failed to capture log", e);
            }
        });

        return thread;
    }

    private static void log(Logger logger, LogEntry logEntry) {
        switch (logEntry.getSeverity()) {
            case DEBUG:
                logger.debug("{}", logEntry.toStructuredJsonString());
            case WARNING:
                logger.warn("{}", logEntry.toStructuredJsonString());
            case ERROR:
            case CRITICAL:
            case ALERT:
            case EMERGENCY:
                logger.error("{}", logEntry.toStructuredJsonString());
                break;
            default:
                logger.info("{}", logEntry.toStructuredJsonString());
        }
    }
}
