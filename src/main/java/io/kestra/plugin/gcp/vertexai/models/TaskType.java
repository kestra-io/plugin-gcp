package io.kestra.plugin.gcp.vertexai.models;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    title = "Vertex AI Text Embedding Task Type",
    description = "Specifies the intended downstream use case for the embedding vector."
)
public enum TaskType {
    @Schema(description = "Specifies the given text is a query in a search/retrieval setting.")
    RETRIEVAL_QUERY,

    @Schema(description = "Specifies the given text is a document in a search/retrieval setting.")
    RETRIEVAL_DOCUMENT,

    @Schema(description = "Specifies the given text will be used for STS (semantic textual similarity).")
    SEMANTIC_SIMILARITY,

    @Schema(description = "Specifies that the embeddings will be used for classification.")
    CLASSIFICATION,

    @Schema(description = "Specifies that the embeddings will be used for clustering.")
    CLUSTERING,

    @Schema(description = "Specifies that the embeddings will be used for question answering.")
    QUESTION_ANSWERING,

    @Schema(description = "Specifies that the embeddings will be used for fact verification.")
    FACT_VERIFICATION
}
