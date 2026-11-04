import type { Meta, StoryObj } from "@storybook/vue3";
import { setup } from "@storybook/vue3";
import { within, expect, waitFor } from "storybook/test";
import { reactive } from "vue";
import { createI18n } from "vue-i18n";
import KestraDesignSystem from "@kestra-io/design-system";
import BigqueryQueryTopologyDetails from "../../../src/components/BigqueryQueryTopologyDetails.vue";

// The addon-vitest browser runner renders stories without the interactive Storybook's `initApp`
// preview hook, so install the app context the component needs (i18n + design system) via the
// global Storybook `setup` hook — the same mechanism initApp itself uses. The component uses
// `useScope: "local"` with its own messages, so an empty global i18n instance satisfies useI18n().
const i18n = createI18n({ legacy: false, locale: "en", messages: { en: {} } });
setup((app) => {
    app.use(i18n);
    app.use(KestraDesignSystem);
});

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

// The sample task-run outputs/metrics the post-execution stories' fetchOutputs/fetchMetrics fixtures
// return — mirrors what the host would resolve server-side for this execution/task.
const SAMPLE_OUTPUTS = {
    jobId: "bquxjob_1a2b3c4d_1234567890ab",
    projectId: "resolved-gcp-project",
    location: "EU",
    size: 15234,
    destinationTable: {
        project: "my-gcp-project",
        dataset: "analytics",
        table: "users_active",
    },
};

const SAMPLE_METRICS = {
    results: [
        { name: "total.bytes.billed", value: 1_099_511_627_776 }, // 1 TiB
        { name: "total.bytes.processed", value: 987_842_478_899 }, // ~0.9 TiB
        { name: "total.slot.ms", value: 45_678 },
        { name: "cache.hit", value: 0 },
        { name: "duration", value: 3_420 },
    ],
    total: 5,
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
        fetchOutputs: async () => SAMPLE_OUTPUTS,
        fetchMetrics: async () => SAMPLE_METRICS,
    } as any,
};

// Task whose GCP config is driven by Pebble expressions (flow vars). Before an execution the panel
// shows the raw definition values marked as templates; after an execution it shows only what the
// task outputs report, never the raw expression.
// `displayMode: "full"` opens the full view where the Query section renders.
const expressionTask = {
    id: "query-bq",
    type: "io.kestra.plugin.gcp.bigquery.Query",
    projectId: "{{ vars.projectId }}",
    location: "{{ vars.location }}",
    sql: 'SELECT\n  "{{ vars.projectId }}" AS project,\n  "{{ vars.location }}" AS location,\n  CURRENT_TIMESTAMP() AS ran_at',
    fetch: true,
};

const executionArgs = {
    namespace: "company.team",
    flowId: "bq-pipeline",
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
    fetchMetrics: async () => SAMPLE_METRICS,
};

export const PreExecutionExpressions: Story = {
    name: "Pre-execution — expressions marked as templates",
    args: {
        task: expressionTask,
        namespace: "company.team",
        flowId: "bq-pipeline",
        displayMode: "full",
    } as any,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() =>
            expect(canvas.getByText("(template) {{ vars.projectId }}")).toBeInTheDocument(),
        );
        expect(canvas.getByText("(template) {{ vars.location }}")).toBeInTheDocument();
        expect(canvas.getByText("Query (template)")).toBeInTheDocument();
    },
};

export const PostExecutionExpressions: Story = {
    name: "Post-execution — resolved values from outputs",
    args: {
        task: expressionTask,
        ...executionArgs,
        fetchOutputs: async () => SAMPLE_OUTPUTS,
    } as any,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(canvas.getByText("resolved-gcp-project")).toBeInTheDocument());
        expect(canvas.getByText("EU")).toBeInTheDocument();
        expect(canvas.getByText("bquxjob_1a2b3c4d_1234567890ab")).toBeInTheDocument();
        expect(canvas.queryByText(/\{\{ vars\.projectId \}\}$/)).toBeNull();
        expect(canvas.queryByText(/\{\{ vars\.location \}\}$/)).toBeNull();
    },
};

// Outputs from an older plugin version carry no projectId/location: show "—", never the definition.
export const PostExecutionOutputsWithoutProjectAndLocation: Story = {
    name: "Post-execution — outputs without project and location",
    args: {
        task: baseTask,
        ...executionArgs,
        fetchOutputs: async () => ({ jobId: SAMPLE_OUTPUTS.jobId, size: SAMPLE_OUTPUTS.size }),
    } as any,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(canvas.getByText(SAMPLE_OUTPUTS.jobId)).toBeInTheDocument());
        expect(canvas.queryByText("my-gcp-project")).toBeNull();
        expect(canvas.queryByText("US")).toBeNull();
        expect(canvas.getAllByText("—").length).toBeGreaterThanOrEqual(2);
    },
};

// Pebble tags (`{% %}`) alone make a value templated, even without any `{{ }}` expression.
export const PreExecutionTagOnlySql: Story = {
    name: "Pre-execution — {% %}-only SQL marked as template",
    args: {
        task: {
            ...baseTask,
            sql: "SELECT id FROM t\n{% if inputs.full %}LIMIT 1000{% endif %}",
        },
        namespace: "company.team",
        flowId: "bq-pipeline",
        displayMode: "full",
    } as any,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(canvas.getByText("Query (template)")).toBeInTheDocument());
        expect(canvas.getByText("my-gcp-project")).toBeInTheDocument();
    },
};

// Node opened while the query runs: outputs are empty until the task run reaches SUCCESS, then the
// state change triggers a refetch. `reactive` lets the play function advance the task run in place.
const runningExecution = reactive({
    ...executionArgs.execution,
    taskRunList: [{ ...executionArgs.execution.taskRunList[0], state: { current: "RUNNING" } }],
});

export const PostExecutionOutputsAfterRunning: Story = {
    name: "Post-execution — outputs refetched when the running task succeeds",
    args: {
        task: expressionTask,
        ...executionArgs,
        execution: runningExecution,
        fetchOutputs: async () =>
            runningExecution.taskRunList[0].state.current === "SUCCESS" ? SAMPLE_OUTPUTS : {},
    } as any,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(canvas.queryByText("resolved-gcp-project")).toBeNull();
        runningExecution.taskRunList[0].state.current = "SUCCESS";
        await waitFor(() => expect(canvas.getByText("resolved-gcp-project")).toBeInTheDocument());
        expect(canvas.getByText("EU")).toBeInTheDocument();
    },
};