package io.kestra.plugin.gcp.bigquery;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.JobId;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TriggerKillTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerKillTest.class);

    @Test
    void killCancelsTrackedBigQueryJob() {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        query.trackJob(connection, jobId, LOGGER);
        trigger.trackTask(query);

        trigger.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void secondKillDoesNotCancelJobAgain() {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        query.trackJob(connection, jobId, LOGGER);
        trigger.trackTask(query);

        trigger.kill();
        trigger.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void killCancelsSecondJobTrackedAfterRetry() {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId firstJobId = JobId.of("my-project", "first-job");
        JobId secondJobId = JobId.of("my-project", "second-job");

        query.trackJob(connection, firstJobId, LOGGER);
        trigger.trackTask(query);

        trigger.kill();

        query.trackJob(connection, secondJobId, LOGGER);
        trigger.kill();

        verify(connection, times(1)).cancel(firstJobId);
        verify(connection, times(1)).cancel(secondJobId);
    }

    @Test
    void killBeforeJobIsTrackedIsSafe() {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        trigger.trackTask(query);

        trigger.kill();

        verify(connection, times(0)).cancel(jobId);

        query.trackJob(connection, jobId, LOGGER);
        trigger.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void stopCancelsTrackedBigQueryJob() {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        query.trackJob(connection, jobId, LOGGER);
        trigger.trackTask(query);

        trigger.stop();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void completedPollIsNotCancelledByLaterKill() {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        query.trackJob(connection, jobId, LOGGER);
        trigger.trackTask(query);

        trigger.clearTask(query);
        trigger.kill();

        verify(connection, times(0)).cancel(jobId);
    }

    @Test
    void killWithoutNestedQueryTaskIsNoOp() {
        Trigger trigger = Trigger.builder().build();

        assertDoesNotThrow(trigger::kill);
    }
}
