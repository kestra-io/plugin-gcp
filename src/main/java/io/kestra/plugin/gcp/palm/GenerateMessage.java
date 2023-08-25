package io.kestra.plugin.gcp.palm;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "TODO",
    description = "TODO"
)
@Plugin(
    examples = {
        @Example(
            title = "TODO",
            code = {
                "TODO"
            }
        )
    }
)
//https://developers.generativeai.google/api/rest/generativelanguage/models/generateMessage
public class GenerateMessage extends AbstractPalmApi implements RunnableTask<VoidOutput> {
    private static final String MODEL_ID = "text-bison-001"; //TODO multiple ones exists
    private static final String URI_PATTERN = "https://generativelanguage.googleapis.com/v1beta2/models/%s:generateMessage";

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        return null;
    }

    protected URI getPredictionURI(RunContext runContext, String modelId) {
        var formatted = URI_PATTERN.formatted(modelId);
        runContext.logger().debug("Calling PaLM API {}", formatted);
        return URI.create(formatted);
    }

    @Override
    protected MutableHttpRequest<?> getPredictionRequest(RunContext runContext) throws IllegalVariableEvaluationException {
        var request = (String) null;
        return HttpRequest.POST(getPredictionURI(runContext, MODEL_ID), request);
    }
}
