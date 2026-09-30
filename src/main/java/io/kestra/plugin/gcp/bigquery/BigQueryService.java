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
        var builder = JobId.newBuilder()
            .setProject(runContext.render(abstractBigquery.getProjectId()).as(String.class).orElse(null))
            .setLocation(runContext.render(abstractBigquery.getLocation()).as(String.class).orElse(null));

        // A trigger evaluates outside any execution, so there is no taskrun to key on and no resubmit to
        // deduplicate. Deriving an id there would give every poll of every trigger the same one.
        var taskRunId = runContext.taskRunInfo().taskRunId();
        if (taskRunId != null) {
            builder.setJob("kestra_" + taskRunId);
        }

        return builder.build();
    }

    // Same project and location with no job id, so BigQuery assigns a random one as it did before.
    private static JobId randomJobId(JobId jobId) {
        return JobId.newBuilder()
            .setProject(jobId.getProject())
            .setLocation(jobId.getLocation())
            .build();
    }

    /**
     * Submits the job, adopting the existing one when its deterministic id is already taken.
     *
     * <p>
     * A failed job burns its id for good, so a retry walks {@code kestra_<taskRunId>}, {@code _1}, {@code _2}, ...
     * and adopts the first job that has not failed, or submits under the first free id. The chain is deterministic so
     * a worker-loss resubmit during a retry still adopts that retry's job rather than duplicating it.
     */
    public static Job createOrAdoptJob(BigQuery connection, JobInfo jobInfo, Logger logger) {
        var base = jobInfo.getJobId();
        if (base == null || base.getJob() == null) {
            return connection.create(jobInfo);
        }

        for (int n = 0; n <= MAX_RESUBMITS; n++) {
            var candidate = n == 0 ? jobInfo : withJob(jobInfo, base.getJob() + "_" + n);
            var submittedAt = System.currentTimeMillis();
            Job job;

            try {
                job = connection.create(candidate);
            } catch (com.google.cloud.bigquery.BigQueryException e) {
                if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
                    throw e;
                }

                // BigQuery says the id is taken but will not hand the job over, so there is nothing to adopt
                // and nothing safe to resubmit under. Failing here duplicates no work, which is the point.
                var existing = connection.getJob(candidate.getJobId());
                if (existing == null) {
                    throw e;
                }
                if (!isFailed(existing)) {
                    return adopt(existing, candidate, logger);
                }
                logger.warn("Job '{}' already ran and failed, trying the next id", candidate.getJobId().getJob());
                continue;
            }

            // For a caller-supplied id the client swallows "Already Exists" and returns the existing job when it is
            // under 24 hours old (BigQueryImpl.create), fetched with fields(STATISTICS) only, so it has no status.
            // A job created before this call is not this call's, and only a full fetch says whether it failed.
            if (!isAdopted(job, submittedAt)) {
                return job;
            }

            var current = job.getStatus() != null ? job : connection.getJob(job.getJobId());
            // A vanished job has nothing to resubmit under, so leave it to the caller's poll to report.
            if (current == null) {
                return job;
            }
            if (!isFailed(current)) {
                return adopt(job, candidate, logger);
            }

            logger.warn("Job '{}' already ran and failed, trying the next id", candidate.getJobId().getJob());
        }

        logger.warn(
            "Job '{}' failed {} times, submitting under a BigQuery-assigned id; worker-loss deduplication no longer applies to this attempt",
            base.getJob(),
            MAX_RESUBMITS + 1
        );
        return connection.create(withJobId(jobInfo, randomJobId(base)));
    }

    static final int MAX_RESUBMITS = 100;

    // Absorbs clock skew between the worker and BigQuery for jobs that carry a status. Status-less adopted jobs
    // are recognised by shape instead, so sub-second retry intervals do not depend on this margin.
    static final long ADOPTED_JOB_MARGIN_MS = 2_000;

    private static Job adopt(Job job, JobInfo candidate, Logger logger) {
        logger.warn("Adopting job '{}' already started by this taskrun instead of submitting a duplicate", candidate.getJobId().getJob());
        return job;
    }

    private static boolean isFailed(Job job) {
        var status = job.getStatus();
        return status != null && status.getState() == JobStatus.State.DONE && status.getError() != null;
    }

    // The client's adopt path fetches with fields(STATISTICS), so an adopted job has statistics but no status,
    // whereas a job this call created comes back from jobs.insert with its status. That needs no clock; the
    // creation-time check backs it up should the client ever return the full job.
    private static boolean isAdopted(Job job, long submittedAt) {
        return (job.getStatus() == null && job.getStatistics() != null) || createdBefore(job, submittedAt);
    }

    private static boolean createdBefore(Job job, long submittedAt) {
        var statistics = job.getStatistics();
        var creationTime = statistics != null ? statistics.getCreationTime() : null;
        return creationTime != null && creationTime < submittedAt - ADOPTED_JOB_MARGIN_MS;
    }

    private static JobInfo withJob(JobInfo jobInfo, String job) {
        var id = jobInfo.getJobId();
        return withJobId(jobInfo, JobId.newBuilder().setProject(id.getProject()).setLocation(id.getLocation()).setJob(job).build());
    }

    private static JobInfo withJobId(JobInfo jobInfo, JobId jobId) {
        return JobInfo.newBuilder(jobInfo.getConfiguration()).setJobId(jobId).build();
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
        } else if (job.getStatus() == null) {
            throw new IllegalStateException("Job '" + job.getJobId().getJob() + "' has no status to report");
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
