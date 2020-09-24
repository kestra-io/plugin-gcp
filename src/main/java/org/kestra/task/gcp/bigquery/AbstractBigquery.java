package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import lombok.*;
import lombok.experimental.SuperBuilder;
import net.jodah.failsafe.Failsafe;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.models.tasks.retrys.AbstractRetry;
import org.kestra.core.models.tasks.retrys.Exponential;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import javax.validation.Valid;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBigquery extends Task {
    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    protected String projectId;

    @InputProperty(
        description = "The geographic location where the dataset should reside",
        body = "This property is experimental\n" +
            " and might be subject to change or removed.\n" +
            " \n" +
            " See <a href=\"https://cloud.google.com/bigquery/docs/reference/v2/datasets#location\">Dataset Location</a>",
        dynamic = true
    )
    protected String location;

    @Valid
    @Builder.Default
    @InputProperty(
        description = "Automatic retry for retryable bigquery exceptions",
        body = {"Some exceptions (espacially rate limit) are not retried by default by BigQuery client, we use by " +
            "default a transparent retry (not the kestra one) to handle this case.",
            "The default values are Exponential of 5 seconds for max 15 minutes and 10 attempts"},
        dynamic = false
    )
    protected AbstractRetry retryAuto = Exponential.builder()
        .type("exponential")
        .interval(Duration.ofSeconds(5))
        .maxDuration(Duration.ofMinutes(15))
        .maxAttempt(10)
        .build();

    @Valid
    @Builder.Default
    @InputProperty(
        description = "The reason valid for a automatic retry.",
        dynamic = false
    )
    protected List<String> retryReasons = Arrays.asList(
        "rateLimitExceeded",
        "jobBackendError"
    );

    protected BigQuery connection(RunContext runContext) throws IllegalVariableEvaluationException {
        return new BigQueryService().of(
            runContext.render(this.projectId),
            runContext.render(this.location)
        );
    }

    public Job waitForJob(Logger logger, Callable<Job> createJob) {
        return Failsafe
            .with(AbstractRetry.<Job>retryPolicy(this.getRetryAuto())
                .handleIf(failure -> {
                    if (this.retryReasons == null || this.retryReasons.size() == 0) {
                        return false;
                    }

                    if (!(failure instanceof BigQueryException)) {
                        return false;
                    }

                    BigQueryError error = ((BigQueryException) failure).getError();

                    return this.retryReasons.contains(error.getReason());
                })
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
                    }

                    throw exception;
                }

                BigQueryService.handleErrors(job, logger);

                return job;
            });
    }
}
