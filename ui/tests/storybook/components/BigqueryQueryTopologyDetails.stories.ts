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

export const PreExecutionExpressions: Story = {
    name: "Pre-execution — expressions resolved",
    args: {
        task: expressionTask,
        namespace: "company.team",
        flowId: "bq-pipeline",
        displayMode: "full",
    } as any,
    // Regression guard: the panel must show RESOLVED values, never the raw "{{ … }}" templates.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // Resolution is async (backend round-trip), so wait for the resolved value to appear.
        await waitFor(() =>
            expect(canvas.getAllByText("my-gcp-project").length).toBeGreaterThan(0),
        );
        expect(canvas.getAllByText("EU").length).toBeGreaterThan(0);
        expect(canvas.queryByText("{{ vars.projectId }}")).not.toBeInTheDocument();
        expect(canvas.queryByText("{{ vars.location }}")).not.toBeInTheDocument();
    },
};

export const PostExecutionExpressions: Story = {
    name: "Post-execution — expressions resolved",
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
    } as any,
    // Regression guard: resolved values AND the post-execution job details (from the mocked
    // metrics/outputs) must render, still with no raw "{{ … }}" templates.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() =>
            expect(canvas.getAllByText("my-gcp-project").length).toBeGreaterThan(0),
        );
        expect(canvas.getAllByText("EU").length).toBeGreaterThan(0);
        expect(
            canvas.getByText("my-gcp-project:EU.bquxjob_1a2b3c4d_1234567890ab"),
        ).toBeInTheDocument();
        expect(canvas.queryByText("{{ vars.projectId }}")).not.toBeInTheDocument();
    },
};