package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.*;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.runners.RunContext;
import org.kestra.task.gcp.AbstractConnection;
import org.slf4j.Logger;

import java.util.UUID;

public class BigQueryService extends AbstractConnection {
    public BigQuery of(String projectId, String location) {
        return BigQueryOptions
            .newBuilder()
            .setProjectId(projectId)
            .setLocation(location)
            .build()
            .getService();
    }

    public BigQuery of(String serviceAccount, String projectId, String location) {
        return BigQueryOptions
            .newBuilder()
            .setCredentials(this.credentials(serviceAccount))
            .setProjectId(projectId)
            .setLocation(location)
            .build()
            .getService();
    }

    public static JobId jobId(RunContext runContext, AbstractBigquery abstractBigquery) throws IllegalVariableEvaluationException {
        return JobId.newBuilder()
            .setProject(runContext.render(abstractBigquery.getProjectId()))
            .setLocation(runContext.render(abstractBigquery.getLocation()))
            .setJob(runContext
                .render("{{flow.namespace}}.{{flow.id}}_{{execution.id}}_{{taskrun.id}}_" + UUID.randomUUID())
                .replace(".", "-")
            )
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
            if (job.getStatus().getExecutionErrors() != null) {
                job
                    .getStatus()
                    .getExecutionErrors()
                    .forEach(bigQueryError -> {
                        logger.warn(
                            "Error on job '{}' query with error [\n - {}\n]",
                            job.getJobId().getJob(),
                            bigQueryError.toString()
                        );
                    });
            }

            if (job.getStatus().getError() != null) {
                throw new BigQueryException(
                    job.getStatus().getError(),
                    job.getStatus().getExecutionErrors()
                );
            }
        }
    }
}
