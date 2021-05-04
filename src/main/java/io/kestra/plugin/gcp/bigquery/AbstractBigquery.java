package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.*;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import net.jodah.failsafe.Failsafe;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.AbstractRetry;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import javax.validation.Valid;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBigquery extends AbstractTask {
    @Schema(
        title = "The geographic location where the dataset should reside",
        description = "This property is experimental" +
            " and might be subject to change or removed.\n" +
            " \n" +
            " See <a href=\"https://cloud.google.com/bigquery/docs/reference/v2/datasets#location\">Dataset Location</a>"
    )
    @PluginProperty(dynamic = true)
    protected String location;

    @Schema(
        title = "Automatic retry for retryable bigquery exceptions",
        description = "Some exceptions (espacially rate limit) are not retried by default by BigQuery client, we use by " +
            "default a transparent retry (not the kestra one) to handle this case.\n" +
            "The default values are Exponential of 5 seconds for max 15 minutes and 10 attempts"
    )
    @PluginProperty(dynamic = true)
    protected AbstractRetry retryAuto;

    @Builder.Default
    @Schema(
        title = "The reason that are valid for a automatic retry."
    )
    @PluginProperty(dynamic = true)
    protected List<String> retryReasons = Arrays.asList(
        "rateLimitExceeded",
        "jobBackendError"
    );

    @Builder.Default
    @Schema(
        title = "The message that are valid for a automatic retry.",
        description = "Message is tested as a substring of the full message and case insensitive"
    )
    @PluginProperty(dynamic = true)
    protected List<String> retryMessages = Collections.singletonList(
        "due to concurrent update"
    );

    BigQuery connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return BigQueryOptions
            .newBuilder()
            .setCredentials(this.credentials(runContext))
            .setProjectId(runContext.render(this.projectId))
            .setLocation(runContext.render(this.location))
            .build()
            .getService();
    }

    protected Job waitForJob(Logger logger, Callable<Job> createJob) {
        return Failsafe
            .with(AbstractRetry.<Job>retryPolicy(this.getRetryAuto() != null ? this.getRetry() : Exponential.builder()
                    .type("exponential")
                    .interval(Duration.ofSeconds(5))
                    .maxInterval(Duration.ofMinutes(60))
                    .maxDuration(Duration.ofMinutes(15))
                    .maxAttempt(10)
                    .build()
                )
                .handleIf(this::shouldRetry)
                .onFailure(event -> logger.error(
                    "Stop retry, attempts {} elapsed {} seconds",
                    event.getAttemptCount(),
                    event.getElapsedTime().getSeconds(),
                    event.getFailure()
                ))
                .onRetry(event -> {
                    logger.warn(
                        "Retrying, attempts {} elapsed {} seconds",
                        event.getAttemptCount(),
                        event.getElapsedTime().getSeconds()
                    );
                })
            )
            .get(() -> {
                Job job = createJob.call();

                BigQueryService.handleErrors(job, logger);

                try {
                    job = job.waitFor();
                } catch (Exception exception) {
                    if (exception instanceof com.google.cloud.bigquery.BigQueryException) {
                        com.google.cloud.bigquery.BigQueryException bqException = (com.google.cloud.bigquery.BigQueryException) exception;

                        logger.warn(
                            "Error query on job '{}' with error [\n - {}\n]",
                            job.getJobId().getJob(),
                            bqException.toString()
                        );

                        throw new BigQueryException(bqException.getError());
                    } else if (exception instanceof JobException) {
                        JobException bqException = (JobException) exception;

                        logger.warn(
                            "Error query on job '{}' with error [\n - {}\n]",
                            job.getJobId().getJob(),
                            bqException.toString()
                        );
                    }

                    throw exception;
                }

                BigQueryService.handleErrors(job, logger);

                return job;
            });
    }

    private boolean shouldRetry(Throwable failure) {
        if (!(failure instanceof BigQueryException)) {
            return false;
        }

        BigQueryError error = ((BigQueryException) failure).getError();

        if (this.retryReasons != null && this.retryReasons.contains(error.getReason())) {
            return true;
        }

        if (this.retryMessages != null) {
            for (String message: this.retryMessages) {
                if (error.getMessage().toLowerCase().contains(message.toLowerCase())) {
                    return true;
                }
            }
        }

        return false;
    }
}
