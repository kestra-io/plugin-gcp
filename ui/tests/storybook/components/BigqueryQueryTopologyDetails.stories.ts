import type { Meta, StoryObj } from "@storybook/vue3";
import { setup } from "@storybook/vue3";
import { within, expect, waitFor } from "storybook/test";
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
    jobId: "my-gcp-project:EU.bquxjob_1a2b3c4d_1234567890ab",
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

// Task whose GCP config is driven by Pebble expressions (flow vars). Server-side expression
// rendering was dropped along with @kestra-io/kestra-sdk, so the panel now shows these RAW,
// unresolved ("{{ vars.projectId }}", "{{ vars.location }}") instead of a resolved value.
// `displayMode: "full"` opens the full view where the Query section renders.
const expressionTask = {
    id: "query-bq",
    type: "io.kestra.plugin.gcp.bigquery.Query",
    projectId: "{{ vars.projectId }}",
    location: "{{ vars.location }}",
    sql: 'SELECT\n  "{{ vars.projectId }}" AS project,\n  "{{ vars.location }}" AS location,\n  CURRENT_TIMESTAMP() AS ran_at',
    fetch: true,
};

export const PreExecutionExpressions: Story = {
    name: "Pre-execution — expressions shown raw",
    args: {
        task: expressionTask,
        namespace: "company.team",
        flowId: "bq-pipeline",
        displayMode: "full",
    } as any,
    // Regression guard: the panel must show the raw "{{ … }}" template, unresolved.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() =>
            expect(canvas.getAllByText("{{ vars.projectId }}").length).toBeGreaterThan(0),
        );
        expect(canvas.getAllByText("{{ vars.location }}").length).toBeGreaterThan(0);
    },
};

export const PostExecutionExpressions: Story = {
    name: "Post-execution — expressions shown raw",
    args: {
        task: expressionTask,
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
        fetchOutputs: async () => SAMPLE_OUTPUTS,
        fetchMetrics: async () => SAMPLE_METRICS,
    } as any,
    // Regression guard: raw "{{ … }}" templates AND the post-execution job details (from the
    // fetchOutputs/fetchMetrics fixtures) must render together.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() =>
            expect(canvas.getAllByText("{{ vars.projectId }}").length).toBeGreaterThan(0),
        );
        expect(canvas.getAllByText("{{ vars.location }}").length).toBeGreaterThan(0);
        expect(
            canvas.getByText("my-gcp-project:EU.bquxjob_1a2b3c4d_1234567890ab"),
        ).toBeInTheDocument();
    },
};