package io.kestra.plugin.gcp.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

public interface BigQueryFactory {
    BigQuery create(RunContext runContext, GoogleCredentials credentials, String projectId, String location) throws IllegalVariableEvaluationException;
}
