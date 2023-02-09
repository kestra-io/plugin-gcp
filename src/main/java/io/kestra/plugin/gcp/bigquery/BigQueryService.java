package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.TableId;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.util.ArrayList;

public class BigQueryService {
    public static JobId jobId(RunContext runContext, AbstractBigquery abstractBigquery)
        throws IllegalVariableEvaluationException {
        return JobId.newBuilder()
            .setProject(runContext.render(abstractBigquery.getProjectId()))
            .setLocation(runContext.render(abstractBigquery.getLocation()))
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
}
