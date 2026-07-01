package io.kestra.plugin.gcp.dataflow;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.*;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class DataflowTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Mock
    private Dataflow mockDataflow;

    @Mock
    private Dataflow.Projects mockProjects;

    @Mock
    private Dataflow.Projects.Locations mockLocations;

    @Mock
    private Dataflow.Projects.Locations.Templates mockTemplates;

    @Mock
    private Dataflow.Projects.Locations.Templates.Launch mockTemplatesLaunch;

    @Mock
    private Dataflow.Projects.Locations.FlexTemplates mockFlexTemplates;

    @Mock
    private Dataflow.Projects.Locations.FlexTemplates.Launch mockFlexTemplatesLaunch;

    @Mock
    private Dataflow.Projects.Locations.Jobs mockJobs;

    @Mock
    private Dataflow.Projects.Locations.Jobs.Get mockJobsGet;

    @Mock
    private Dataflow.Projects.Locations.Jobs.GetMetrics mockJobsGetMetrics;

    @Mock
    private Dataflow.Projects.Locations.Jobs.Update mockJobsUpdate;

    @Mock
    private Dataflow.Projects.Locations.Jobs.List mockJobsList;

    private AutoCloseable closeable;

    @BeforeEach
    void setUp() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);

        when(mockDataflow.projects()).thenReturn(mockProjects);
        when(mockProjects.locations()).thenReturn(mockLocations);
        when(mockLocations.templates()).thenReturn(mockTemplates);
        when(mockLocations.flexTemplates()).thenReturn(mockFlexTemplates);
        when(mockLocations.jobs()).thenReturn(mockJobs);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws Exception {
        if (closeable != null) {
            closeable.close();
        }
    }

    @Test
    void launchTemplateTask() throws Exception {
        var runContext = runContextFactory.of();

        var response = new LaunchTemplateResponse()
            .setJob(new Job().setId("job-123").setCurrentState("JOB_STATE_RUNNING"));

        when(mockTemplates.launch(any(), any(), any())).thenReturn(mockTemplatesLaunch);
        when(mockTemplatesLaunch.setGcsPath(any())).thenReturn(mockTemplatesLaunch);
        when(mockTemplatesLaunch.execute()).thenReturn(response);

        var task = LaunchTemplate.builder()
            .id("launch-template")
            .type(LaunchTemplate.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobName(Property.ofValue("my-classic-job"))
            .gcsPath(Property.ofValue("gs://bucket/template"))
            .parameters(Property.ofValue(Map.of("param1", "value1")))
            .environment(Property.ofValue(Map.of("maxWorkers", 5)))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        var output = spyTask.run(runContext);
        assertThat(output.getJobId(), is("job-123"));
        assertThat(output.getJobState(), is("JOB_STATE_RUNNING"));
    }

    @Test
    void launchFlexTemplateTask() throws Exception {
        var runContext = runContextFactory.of();

        var response = new LaunchFlexTemplateResponse()
            .setJob(new Job().setId("flex-job-123").setCurrentState("JOB_STATE_RUNNING"));

        when(mockFlexTemplates.launch(any(), any(), any())).thenReturn(mockFlexTemplatesLaunch);
        when(mockFlexTemplatesLaunch.execute()).thenReturn(response);

        var task = LaunchFlexTemplate.builder()
            .id("launch-flex-template")
            .type(LaunchFlexTemplate.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobName(Property.ofValue("my-flex-job"))
            .containerSpecGcsPath(Property.ofValue("gs://bucket/flex-spec.json"))
            .parameters(Property.ofValue(Map.of("param1", "value1")))
            .environment(Property.ofValue(Map.of("maxWorkers", 10)))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        var output = spyTask.run(runContext);
        assertThat(output.getJobId(), is("flex-job-123"));
        assertThat(output.getJobState(), is("JOB_STATE_RUNNING"));
    }

    @Test
    void getJobTask() throws Exception {
        var runContext = runContextFactory.of();

        var job = new Job()
            .setId("job-123")
            .setCurrentState("JOB_STATE_DONE")
            .setCreateTime("2026-06-25T12:00:00Z")
            .setCurrentStateTime("2026-06-25T13:00:00Z")
            .setType("JOB_TYPE_BATCH");

        var metricsResponse = new JobMetrics()
            .setMetrics(
                List.of(
                    new MetricUpdate()
                        .setName(new MetricStructuredName().setName("test-metric"))
                        .setScalar(456L)
                )
            );

        when(mockJobs.get(any(), any(), any())).thenReturn(mockJobsGet);
        when(mockJobsGet.execute()).thenReturn(job);
        when(mockJobs.getMetrics(any(), any(), any())).thenReturn(mockJobsGetMetrics);
        when(mockJobsGetMetrics.execute()).thenReturn(metricsResponse);

        var task = GetJob.builder()
            .id("get-job")
            .type(GetJob.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobId(Property.ofValue("job-123"))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        var output = spyTask.run(runContext);
        assertThat(output.getJobId(), is("job-123"));
        assertThat(output.getCurrentState(), is("JOB_STATE_DONE"));
        assertThat(output.getCreateTime(), is(Instant.parse("2026-06-25T12:00:00Z")));
        assertThat(output.getCurrentStateTime(), is(Instant.parse("2026-06-25T13:00:00Z")));
        assertThat(output.getType(), is("JOB_TYPE_BATCH"));
        assertThat(output.getMetrics(), hasEntry("test-metric", 456L));
    }

    @Test
    void cancelJobTask() throws Exception {
        var runContext = runContextFactory.of();

        var response = new Job()
            .setId("job-123")
            .setCurrentState("JOB_STATE_CANCELLED");

        when(mockJobs.update(any(), any(), any(), any())).thenReturn(mockJobsUpdate);
        when(mockJobsUpdate.execute()).thenReturn(response);

        var task = CancelJob.builder()
            .id("cancel-job")
            .type(CancelJob.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobId(Property.ofValue("job-123"))
            .drain(Property.ofValue(false))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        var output = spyTask.run(runContext);
        assertThat(output.getJobId(), is("job-123"));
        assertThat(output.getCurrentState(), is("JOB_STATE_CANCELLED"));
    }

    @Test
    void waitForJobTask() throws Exception {
        var runContext = runContextFactory.of();

        var runningJob = new Job().setId("job-123").setCurrentState("JOB_STATE_RUNNING");
        var doneJob = new Job().setId("job-123").setCurrentState("JOB_STATE_DONE");

        var metricsResponse = new JobMetrics()
            .setMetrics(
                List.of(
                    new MetricUpdate()
                        .setName(new MetricStructuredName().setName("test-metric"))
                        .setScalar(789L)
                )
            );

        when(mockJobs.get(any(), any(), any())).thenReturn(mockJobsGet);
        when(mockJobsGet.execute()).thenReturn(runningJob, doneJob);
        when(mockJobs.getMetrics(any(), any(), any())).thenReturn(mockJobsGetMetrics);
        when(mockJobsGetMetrics.execute()).thenReturn(metricsResponse);

        var task = WaitForJob.builder()
            .id("wait-for-job")
            .type(WaitForJob.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobId(Property.ofValue("job-123"))
            .pollInterval(Property.ofValue(Duration.ofMillis(100)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        var output = spyTask.run(runContext);
        assertThat(output.getJobId(), is("job-123"));
        assertThat(output.getState(), is("JOB_STATE_DONE"));
        assertThat(output.getMetrics(), hasEntry("test-metric", 789L));
    }

    @Test
    void triggerEvaluation() throws Exception {
        var trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobNamePrefix(Property.ofValue("my-etl-"))
            .targetState(Property.ofValue(JobState.JOB_STATE_DONE))
            .lookback(Property.ofValue(Duration.ofSeconds(10)))
            .build();

        var spyTrigger = spy(trigger);
        doReturn(mockDataflow).when(spyTrigger).dataflowClient(any());

        var response = new ListJobsResponse()
            .setJobs(
                List.of(
                    new Job()
                        .setId("job-123")
                        .setName("my-etl-job")
                        .setCurrentState("JOB_STATE_DONE")
                        .setCurrentStateTime(Instant.now().minus(Duration.ofSeconds(2)).toString())
                )
            );

        when(mockJobs.list(any(), any())).thenReturn(mockJobsList);
        when(mockJobsList.execute()).thenReturn(response);

        var triggerContext = TestsUtils.mockTrigger(runContextFactory, spyTrigger);
        var execution = spyTrigger.evaluate(triggerContext.getKey(), triggerContext.getValue());

        assertThat(execution.isPresent(), is(true));
        var variables = execution.get().getTrigger().getVariables();
        assertThat(variables.get("jobId"), is("job-123"));
        assertThat(variables.get("jobName"), is("my-etl-job"));
        assertThat(variables.get("state"), is("JOB_STATE_DONE"));
    }

    @Test
    void cancelJobTask_drain() throws Exception {
        var runContext = runContextFactory.of();

        var response = new Job()
            .setId("job-123")
            .setCurrentState("JOB_STATE_DRAINING");

        when(mockJobs.update(any(), any(), any(), any())).thenReturn(mockJobsUpdate);
        when(mockJobsUpdate.execute()).thenReturn(response);

        var task = CancelJob.builder()
            .id("cancel-job")
            .type(CancelJob.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobId(Property.ofValue("job-123"))
            .drain(Property.ofValue(true))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        var output = spyTask.run(runContext);
        assertThat(output.getJobId(), is("job-123"));
        assertThat(output.getCurrentState(), is("JOB_STATE_DRAINING"));
    }

    @Test
    void waitForJobTask_failedState() throws Exception {
        var runContext = runContextFactory.of();

        var runningJob = new Job().setId("job-123").setCurrentState("JOB_STATE_RUNNING");
        var failedJob = new Job().setId("job-123").setCurrentState("JOB_STATE_FAILED");

        when(mockJobs.get(any(), any(), any())).thenReturn(mockJobsGet);
        when(mockJobsGet.execute()).thenReturn(runningJob, failedJob);

        var task = WaitForJob.builder()
            .id("wait-for-job")
            .type(WaitForJob.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobId(Property.ofValue("job-123"))
            .pollInterval(Property.ofValue(Duration.ofMillis(100)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> spyTask.run(runContext));
    }

    @Test
    void waitForJobTask_cancelledState() throws Exception {
        var runContext = runContextFactory.of();

        var runningJob = new Job().setId("job-123").setCurrentState("JOB_STATE_RUNNING");
        var cancelledJob = new Job().setId("job-123").setCurrentState("JOB_STATE_CANCELLED");

        when(mockJobs.get(any(), any(), any())).thenReturn(mockJobsGet);
        when(mockJobsGet.execute()).thenReturn(runningJob, cancelledJob);

        var task = WaitForJob.builder()
            .id("wait-for-job")
            .type(WaitForJob.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobId(Property.ofValue("job-123"))
            .pollInterval(Property.ofValue(Duration.ofMillis(100)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> spyTask.run(runContext));
    }

    @Test
    void waitForJobTask_timeout() throws Exception {
        var runContext = runContextFactory.of();

        var runningJob = new Job().setId("job-123").setCurrentState("JOB_STATE_RUNNING");

        when(mockJobs.get(any(), any(), any())).thenReturn(mockJobsGet);
        when(mockJobsGet.execute()).thenReturn(runningJob);

        var task = WaitForJob.builder()
            .id("wait-for-job")
            .type(WaitForJob.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobId(Property.ofValue("job-123"))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .maxDuration(Property.ofValue(Duration.ofMillis(150)))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.TimeoutException.class, () -> spyTask.run(runContext));
    }

    @Test
    void getJobTask_metricsError() throws Exception {
        var runContext = runContextFactory.of();

        var job = new Job()
            .setId("job-123")
            .setCurrentState("JOB_STATE_DONE")
            .setCreateTime("2026-06-25T12:00:00Z")
            .setCurrentStateTime("2026-06-25T13:00:00Z")
            .setType("JOB_TYPE_BATCH");

        when(mockJobs.get(any(), any(), any())).thenReturn(mockJobsGet);
        when(mockJobsGet.execute()).thenReturn(job);

        when(mockJobs.getMetrics(any(), any(), any())).thenReturn(mockJobsGetMetrics);
        when(mockJobsGetMetrics.execute()).thenThrow(new java.io.IOException("Google API metrics error"));

        var task = GetJob.builder()
            .id("get-job")
            .type(GetJob.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobId(Property.ofValue("job-123"))
            .build();

        var spyTask = spy(task);
        doReturn(mockDataflow).when(spyTask).dataflowClient(any());

        var output = spyTask.run(runContext);
        assertThat(output.getJobId(), is("job-123"));
        assertThat(output.getCurrentState(), is("JOB_STATE_DONE"));
        assertThat(output.getMetrics().isEmpty(), is(true));
    }

    @Test
    void triggerEvaluation_noMatch() throws Exception {
        var trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .projectId(Property.ofValue("test-project"))
            .location(Property.ofValue("us-central1"))
            .jobNamePrefix(Property.ofValue("my-etl-"))
            .targetState(Property.ofValue(JobState.JOB_STATE_DONE))
            .lookback(Property.ofValue(Duration.ofSeconds(10)))
            .build();

        var spyTrigger = spy(trigger);
        doReturn(mockDataflow).when(spyTrigger).dataflowClient(any());

        var response = new ListJobsResponse()
            .setJobs(
                List.of(
                    new Job()
                        .setId("job-123")
                        .setName("other-etl-job")
                        .setCurrentState("JOB_STATE_DONE")
                        .setCurrentStateTime(Instant.now().minus(Duration.ofSeconds(2)).toString())
                )
            );

        when(mockJobs.list(any(), any())).thenReturn(mockJobsList);
        when(mockJobsList.execute()).thenReturn(response);

        var triggerContext = TestsUtils.mockTrigger(runContextFactory, spyTrigger);
        var execution = spyTrigger.evaluate(triggerContext.getKey(), triggerContext.getValue());

        assertThat(execution.isPresent(), is(false));
    }
}
