package io.kestra.plugin.gcp.bigquery;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

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
    void killCancelsTrackedBigQueryJob() throws Exception {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        query.trackJob(connection, jobId, LOGGER);
        trackedQueryTask(trigger).set(query);

        trigger.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void secondKillDoesNotCancelJobAgain() throws Exception {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        query.trackJob(connection, jobId, LOGGER);
        trackedQueryTask(trigger).set(query);

        trigger.kill();
        trigger.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void killBeforeJobIsTrackedIsSafe() throws Exception {
        Trigger trigger = Trigger.builder().build();
        Query query = Query.builder().build();
        BigQuery connection = mock(BigQuery.class);
        JobId jobId = JobId.of("my-project", "my-job");

        trackedQueryTask(trigger).set(query);

        trigger.kill();

        verify(connection, times(0)).cancel(jobId);

        query.trackJob(connection, jobId, LOGGER);
        trigger.kill();

        verify(connection, times(1)).cancel(jobId);
    }

    @Test
    void killWithoutNestedQueryTaskIsNoOp() {
        Trigger trigger = Trigger.builder().build();

        assertDoesNotThrow(trigger::kill);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Query> trackedQueryTask(Trigger trigger) throws Exception {
        Field field = Trigger.class.getDeclaredField("queryTask");
        field.setAccessible(true);
        return (AtomicReference<Query>) field.get(trigger);
    }
}
