import type { Meta, StoryObj } from "@storybook/vue3";
import BigqueryQueryTopologyDetails from "./components/BigqueryQueryTopologyDetails.vue";

const meta: Meta<typeof BigqueryQueryTopologyDetails> = {
    title: "Plugin UI / topology-details / BigqueryQueryTopologyDetails",
    component: BigqueryQueryTopologyDetails,
    tags: ["autodocs"],
};

export default meta;
type Story = StoryObj<typeof BigqueryQueryTopologyDetails>;

const baseTask = {
    id: "query-bq",
    type: "io.kestra.plugin.gcp.bigquery.Query",
    sql: "SELECT id, name, email\nFROM `my-project.my_dataset.users`\nWHERE active = true\nLIMIT 1000",
    projectId: "my-gcp-project",
    location: "US",
    fetch: true,
};

export const Default: Story = {
    name: "Pre-execution",
    args: {
        task: baseTask,
        namespace: "company.team",
        flowId: "bq-pipeline",
    },
};

export const WithExecution: Story = {
    name: "Post-execution",
    args: {
        task: baseTask,
        namespace: "company.team",
        flowId: "bq-pipeline",
        execution: {
            id: "exec-abc123",
            namespace: "company.team",
            flowId: "bq-pipeline",
            state: { current: "SUCCESS", startDate: "2024-01-15T10:00:00Z" } as any,
            taskRunList: [
                {
                    id: "tr-001",
                    taskId: "query-bq",
                    executionId: "exec-abc123",
                },
            ],
        } as any,
    },
};

// Task whose GCP config is driven by Pebble expressions (flow vars). Storybook resolves these
// through the mocked expression-render endpoint (see .storybook/mocks/expressions.ts), so the panel
// shows the RESOLVED values ("my-gcp-project", "EU") and a resolved SQL block instead of the raw
// "{{ … }}" templates. `displayMode: "full"` opens the full view where the Query section renders.
const expressionTask = {
    id: "query-bq",
    type: "io.kestra.plugin.gcp.bigquery.Query",
    projectId: "{{ vars.projectId }}",
    location: "{{ vars.location }}",
    sql: 'SELECT\n  "{{ vars.projectId }}" AS project,\n  "{{ vars.location }}" AS location,\n  CURRENT_TIMESTAMP() AS ran_at',
    fetch: true,
};

// Live (unsaved) flow source the host would hand the slot; used as render context for resolution.
const expressionSource = [
    "id: bq-pipeline",
    "namespace: company.team",
    "variables:",
    "  projectId: my-gcp-project",
    "  location: EU",
    "tasks:",
    "  - id: query-bq",
    "    type: io.kestra.plugin.gcp.bigquery.Query",
    '    projectId: "{{ vars.projectId }}"',
    '    location: "{{ vars.location }}"',
].join("\n");

export const PreExecutionExpressions: Story = {
    name: "Pre-execution — expressions resolved",
    args: {
        task: expressionTask,
        namespace: "company.team",
        flowId: "bq-pipeline",
        source: expressionSource,
        displayMode: "full",
    } as any,
};

export const PostExecutionExpressions: Story = {
    name: "Post-execution — expressions resolved",
    args: {
        task: expressionTask,
        namespace: "company.team",
        flowId: "bq-pipeline",
        source: expressionSource,
        displayMode: "full",
        execution: {
            id: "exec-abc123",
            namespace: "company.team",
            flowId: "bq-pipeline",
            state: { current: "SUCCESS", startDate: "2024-01-15T10:00:00Z" } as any,
            taskRunList: [
                {
                    id: "tr-001",
                    taskId: "query-bq",
                    executionId: "exec-abc123",
                },
            ],
        } as any,
    } as any,
};