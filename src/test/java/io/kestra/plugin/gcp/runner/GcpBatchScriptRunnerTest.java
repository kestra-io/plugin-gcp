package io.kestra.plugin.gcp.runner;

import io.kestra.core.models.script.AbstractScriptRunnerTest;
import io.kestra.core.models.script.ScriptRunner;

class GcpBatchScriptRunnerTest extends AbstractScriptRunnerTest {

    @Override
    protected ScriptRunner scriptRunner() {
        return GcpBatchScriptRunner.builder()
            .projectId("kestra-unit-test")
            .region("us-central1")
            .gcsBucket("kestra-unit-test")
            .build();
    }
}