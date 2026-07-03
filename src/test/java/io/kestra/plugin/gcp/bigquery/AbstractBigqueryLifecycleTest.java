package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.JobId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractBigqueryLifecycleTest {
    @Test
    void killCancelsTrackedJob() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        task.trackJob(connection, jobId);
        task.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void secondKillIsNoOp() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        task.trackJob(connection, jobId);
        task.kill();
        task.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void killOnRetryTracksLatestJobOnly() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId staleJobId = JobId.of("my-project", "stale-job");
        JobId liveJobId = JobId.of("my-project", "live-job");

        task.trackJob(connection, staleJobId);
        task.trackJob(connection, liveJobId);
        task.kill();

        verify(connection, times(1)).cancel(liveJobId);
        verify(connection, times(0)).cancel(staleJobId);
    }

    @Test
    void killWithoutTrackedJobIsNoOp() {
        Copy task = Copy.builder().build();

        assertDoesNotThrow(task::kill);
    }

    @Test
    void killSwallowsCancelException() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");
        when(connection.cancel(jobId)).thenThrow(new com.google.cloud.bigquery.BigQueryException(500, "boom"));

        task.trackJob(connection, jobId);

        assertDoesNotThrow(task::kill);
    }

    @Test
    void stopCancelsTrackedJob() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        task.trackJob(connection, jobId);
        task.stop();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void secondStopIsNoOp() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        task.trackJob(connection, jobId);
        task.stop();
        task.stop();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void stopAfterKillIsNoOp() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        task.trackJob(connection, jobId);
        task.kill();
        task.stop();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void killAfterStopIsNoOp() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        task.trackJob(connection, jobId);
        task.stop();
        task.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void stopWithoutTrackedJobIsNoOp() {
        Copy task = Copy.builder().build();

        assertDoesNotThrow(task::stop);
    }

    @Test
    void stopSwallowsCancelException() {
        Copy task = Copy.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");
        when(connection.cancel(jobId)).thenThrow(new com.google.cloud.bigquery.BigQueryException(500, "boom"));

        task.trackJob(connection, jobId);

        assertDoesNotThrow(task::stop);
    }
}
