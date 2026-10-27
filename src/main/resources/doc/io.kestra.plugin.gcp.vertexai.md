# How to use the Google Cloud Vertex AI plugin

Google Cloud Vertex AI is a fully-managed machine learning platform that allows you to train, deploy, and customize ML models and AI applications.

This sub-package provides tasks to generate completions and vector embeddings using foundation models (such as `text-embedding-005` and `gemini-1.5-flash`), deploy custom jobs, and interact with Vertex AI generative services.

## Authentication

All tasks inherit GCP authentication properties from the common GCP plugin structure (`io.kestra.plugin.gcp.shared.AbstractTask`). Sensitive credentials should be stored securely using Kestra [secrets](https://kestra.io/docs/concepts/secret), and common settings can be configured across flows via [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

You can authenticate using:
- A service account JSON key via the `serviceAccount` property (e.g. `serviceAccount: "{{ secret('GCP_SA') }}"`).
- Service account impersonation via `impersonatedServiceAccount`.
- Environment-provided credentials (e.g. `GOOGLE_APPLICATION_CREDENTIALS`).
- Workload Identity or Compute Engine metadata IAM credentials when running inside GCP (GKE, GCE).

Scopes default to `https://www.googleapis.com/auth/cloud-platform`.

## Tasks

### Embeddings
Generates dense vector embeddings for semantic search, retrieval-augmented generation (RAG), clustering, and classification using Google Cloud Vertex AI foundation models (such as `text-embedding-005`, `text-embedding-004`, or `text-multilingual-embedding-002`).

- `region`: Regional endpoint for Vertex AI (e.g., `us-central1`, `europe-west1`).
- `modelId`: Identifier of the embedding model (defaults to `text-embedding-005`).
- `text`: Single text string to vectorize (mutually exclusive with `texts`).
- `texts`: List of text strings for high-throughput batch vectorization (maximum 250 texts per request).
- `taskType`: Downstream task optimization (`RETRIEVAL_QUERY`, `RETRIEVAL_DOCUMENT`, `SEMANTIC_SIMILARITY`, `CLASSIFICATION`, `CLUSTERING`, `QUESTION_ANSWERING`, or `FACT_VERIFICATION`).
- `title`: Document title (recommended when `taskType` is `RETRIEVAL_DOCUMENT`).
- `outputDimensionality`: Optional reduced vector dimensionality (e.g., 256, 512, 768).
- `autoTruncate`: Automatically truncates texts that exceed model token limit (defaults to `true`).
- **Outputs**: `embeddings` (all vectors), `embedding` (single convenience vector), `count`, and `dimension`.

### TextCompletion
Generates text responses from text prompts using Google Cloud Vertex AI generative models (such as `gemini-pro`).

### ChatCompletion
Conducts multi-turn conversational interactions using Gemini models.

### MultimodalCompletion
Processes multimodal prompts containing both text and images/files stored in Google Cloud Storage.

### CustomJob
Creates and manages custom training jobs on Vertex AI compute infrastructure.
