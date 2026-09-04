package io.kestra.plugin.gcp.bigquery;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.TableId;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

public class BigQueryService {
    private static final String UNKNOWN_REASON = "unknown";

    // BigQuery reserves a caller-supplied job id, so deriving it from the taskrun makes a worker-loss
    // resubmit collide with the job the lost worker started instead of running the same work twice.
    public static JobId jobId(RunContext runContext, AbstractBigquery abstractBigquery) throws IllegalVariableEvaluationException {
        var taskRun = runContext.taskRunInfo();

        return JobId.newBuilder()
            .setProject(runContext.render(abstractBigquery.getProjectId()).as(String.class).orElse(null))
            .setLocation(runContext.render(abstractBigquery.getLocation()).as(String.class).orElse(null))
            .setJob("kestra_" + taskRun.executionId() + "_" + taskRun.taskRunId())
            .build();
    }

    // Same project and location with no job id, so BigQuery assigns a random one as it did before.
    private static JobId randomJobId(JobId jobId) {
        return JobId.newBuilder()
            .setProject(jobId.getProject())
            .setLocation(jobId.getLocation())
            .build();
    }

    /**
     * Submits the job, adopting the existing one when its deterministic id is already taken. A job that
     * already failed has burnt its id for good, so that case falls back to a fresh random id and lets the
     * retry make progress rather than re-reporting the old failure.
     */
    public static Job createOrAdoptJob(BigQuery connection, JobInfo jobInfo, Logger logger) {
        try {
            return connection.create(jobInfo);
        } catch (com.google.cloud.bigquery.BigQueryException e) {
            if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
                throw e;
            }

            var existing = connection.getJob(jobInfo.getJobId());
            if (existing == null) {
                throw e;
            }

            var status = existing.getStatus();
            if (status != null && status.getState() == JobStatus.State.DONE && status.getError() != null) {
                logger.info("Job '{}' already ran and failed, starting a new one", jobInfo.getJobId().getJob());

                return connection.create(
                    JobInfo.newBuilder(jobInfo.getConfiguration())
                        .setJobId(randomJobId(jobInfo.getJobId()))
                        .build()
                );
            }

            logger.info("Adopting job '{}' already started by this taskrun instead of submitting a duplicate", jobInfo.getJobId().getJob());

            return existing;
        }
    }

    public static TableId tableId(String table) {
        String[] split = table.split("\\.");
        if (split.length == 2) {
            return TableId.of(split[0], split[1]);
        } else if (split.length == 3) {
            return TableId.of(split[0], split[1], split[2]);
        } else {
            throw new IllegalArgumentException("Invalid table name '" + table + "'");
        }
    }

    public static void handleErrors(Job job, Logger logger) throws BigQueryException {
        if (job == null) {
            throw new IllegalArgumentException("Job no longer exists");
        } else if (job.getStatus().getError() != null) {
            ArrayList<BigQueryError> errors = new ArrayList<>();
            if (job.getStatus().getError() != null) {
                errors.add(job.getStatus().getError());
            }

            if (job.getStatus().getExecutionErrors() != null) {
                errors.addAll(job.getStatus().getExecutionErrors());
            }

            if (errors.size() > 0) {
                logger.warn(
                    "Error query on job '{}' with errors:\n[\n - {}\n]",
                    "job '" + job.getJobId().getJob() + "'",
                    String.join("\n - ", errors.stream().map(BigQueryError::toString).toArray(String[]::new))
                );

                throw new BigQueryException(errors);
            }
        }
    }

    /**
     * BigQuery fills the error list only for job-level failures. A transport failure (bare 5xx, socket
     * error, interrupted poll) arrives with it null, so carry the exception's own reason and message as
     * a single error: otherwise the failure reads as "Bigquery Errors [ - ]" with nothing to diagnose.
     * Whether such a failure is worth retrying is the client's call, not a reason string's.
     */
    public static List<BigQueryError> errorsOf(com.google.cloud.bigquery.BigQueryException exception) {
        return errorsOrSynthetic(exception.getErrors(), exception.getReason(), exception.getLocation(), exception);
    }

    /** {@link Job#waitFor} raises a JobException, which carries neither reason nor location. */
    public static List<BigQueryError> errorsOf(JobException exception) {
        return errorsOrSynthetic(exception.getErrors(), null, null, exception);
    }

    private static List<BigQueryError> errorsOrSynthetic(List<BigQueryError> errors, String reason, String location, Throwable exception) {
        if (errors != null && !errors.isEmpty()) {
            return errors;
        }

        return List.of(new BigQueryError(Objects.requireNonNullElse(reason, UNKNOWN_REASON), location, messageOf(exception)));
    }

    private static String messageOf(Throwable exception) {
        if (exception.getMessage() != null && !exception.getMessage().isBlank()) {
            return exception.getMessage();
        }

        return exception.getCause() != null ? exception.getCause().toString() : exception.toString();
    }

    public static Map<String, String> labels(RunContext runContext) {
        var flowProperties = (Map<String, Object>) runContext.getVariables().get("flow");
        var executionProperties = (Map<String, Object>) runContext.getVariables().get("execution");
        var taskProperties = (Map<String, Object>) runContext.getVariables().get("task");
        var triggerProperties = (Map<String, Object>) runContext.getVariables().get("trigger");

        Map<String, String> labels = new HashMap<>();
        labels.put("kestra_namespace", sanitizeLabel((String) flowProperties.get("namespace")));
        labels.put("kestra_flow_id", sanitizeLabel((String) flowProperties.get("id")));
        if (executionProperties != null && executionProperties.containsKey("id")) {
            labels.put("kestra_execution_id", sanitizeLabel((String) executionProperties.get("id")));
        }
        if (taskProperties != null && taskProperties.containsKey("id")) {
            labels.put("kestra_task_id", sanitizeLabel((String) taskProperties.get("id")));
        }
        if (triggerProperties != null && triggerProperties.containsKey("id")) {
            labels.put("kestra_trigger_id", sanitizeLabel((String) triggerProperties.get("id")));
        }

        return labels;
    }

    private static String sanitizeLabel(String label) {
        // From BigQuery documentation :
        // Label keys and values can be no longer than 63 characters, can only contain lowercase letters, numeric characters, underscores and dashes.
        var replaced = label.replace('.', '_').toLowerCase();
        return replaced.length() > 63 ? replaced.substring(0, 63) : replaced;
    }
}
