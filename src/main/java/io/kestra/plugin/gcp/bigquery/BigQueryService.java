package io.kestra.plugin.gcp.bigquery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.TableId;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

public class BigQueryService {
    private static final String UNKNOWN_REASON = "unknown";

    public static JobId jobId(RunContext runContext, AbstractBigquery abstractBigquery) throws IllegalVariableEvaluationException {
        return JobId.newBuilder()
            .setProject(runContext.render(abstractBigquery.getProjectId()).as(String.class).orElse(null))
            .setLocation(runContext.render(abstractBigquery.getLocation()).as(String.class).orElse(null))
            .build();
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
     * error, interrupted poll) arrives with it null, so fold the exception's own reason and message into
     * one error: otherwise the failure reads as "Bigquery Errors [ - ]" and retryReasons has no match.
     *
     * @param inferRetryableReason derive a reason from the HTTP status when BigQuery returned none.
     *        Only pass true where a retry can be deduplicated.
     */
    public static List<BigQueryError> errorsOf(com.google.cloud.bigquery.BigQueryException exception, boolean inferRetryableReason) {
        return errorsOrSynthetic(
            exception.getErrors(),
            reasonOf(exception, inferRetryableReason),
            exception.getLocation(),
            exception
        );
    }

    /** {@link Job#waitFor} raises a JobException, which carries no HTTP status to derive a reason from. */
    public static List<BigQueryError> errorsOf(JobException exception) {
        return errorsOrSynthetic(exception.getErrors(), UNKNOWN_REASON, null, exception);
    }

    private static List<BigQueryError> errorsOrSynthetic(List<BigQueryError> errors, String reason, String location, Throwable exception) {
        if (errors != null && !errors.isEmpty()) {
            return errors;
        }

        return List.of(new BigQueryError(reason, location, messageOf(exception)));
    }

    private static String reasonOf(com.google.cloud.bigquery.BigQueryException exception, boolean inferRetryableReason) {
        if (exception.getReason() != null) {
            return exception.getReason();
        }

        if (!inferRetryableReason) {
            return UNKNOWN_REASON;
        }

        // No structured payload: derive the reason from the status so retryReasons still match.
        // A bare 403 stays unknown on purpose: it is far more often a permission denial than a quota.
        return switch (exception.getCode()) {
            case 429 -> "rateLimitExceeded";
            case 500 -> "internalError";
            case 502, 503, 504 -> "backendError";
            default -> UNKNOWN_REASON;
        };
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
