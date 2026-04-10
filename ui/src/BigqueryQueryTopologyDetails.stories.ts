import type { Meta, StoryObj } from "@storybook/vue3";
import BigqueryQueryTopologyDetails from "./components/BigqueryQueryTopologyDetails.vue";

const meta: Meta<typeof BigqueryQueryTopologyDetails> = {
  title: "Plugin UI / topology-details / BigqueryQueryTopologyDetails",
  component: BigqueryQueryTopologyDetails,
  tags: ["autodocs"],
};

export default meta;
type Story = StoryObj<typeof BigqueryQueryTopologyDetails>;

const mockTask = {
  id: "bq-query",
  type: "io.kestra.plugin.gcp.bigquery.Query",
  sql: {
    value:
      "SELECT\n  user_id,\n  COUNT(*) AS event_count\nFROM `my_project.analytics.events`\nWHERE date >= '2024-01-01'\nGROUP BY 1\nORDER BY 2 DESC\nLIMIT 100",
  },
};

/** Task with SQL only — no execution. Shows the SQL toggle button. */
export const Default: Story = {
  args: {
    task: mockTask,
    namespace: "company.team",
    flowId: "my-flow",
  },
};

/** Full post-execution stats: bytes processed, cost estimate, slot time, duration, outputs. */
export const WithExecution: Story = {
  args: {
    task: mockTask,
    namespace: "company.team",
    flowId: "my-flow",
    execution: {
      outputs: {
        jobId: "bqjob_r1234abcd_00005678",
        size: 42,
        destinationTable: { project: "my_project", dataset: "analytics", table: "results_temp" },
      },
      metrics: [
        { name: "total.bytes.billed", value: 2684354560, type: "counter" }, // ~2.5 GB
        { name: "total.bytes.processed", value: 2684354560, type: "counter" },
        { name: "estimated.bytes.processed", value: 2684354560, type: "counter" },
        { name: "total.slot.ms", value: 1523, type: "counter" },
        { name: "cache.hit", value: 0, type: "counter" },
        { name: "duration", value: 3200000000, type: "timer" }, // 3.2 s in nanoseconds
      ],
      state: { current: "SUCCESS" },
    },
  },
};

/** Execution served from BigQuery cache: zero bytes billed, green "Cached ✓" badge. */
export const CacheHit: Story = {
  args: {
    task: mockTask,
    namespace: "company.team",
    flowId: "my-flow",
    execution: {
      outputs: { jobId: "bqjob_cached_00001234", size: 42 },
      metrics: [
        { name: "total.bytes.billed", value: 0, type: "counter" },
        { name: "total.bytes.processed", value: 0, type: "counter" },
        { name: "cache.hit", value: 1, type: "counter" },
        { name: "duration", value: 450000000, type: "timer" }, // 0.45 s in nanoseconds
      ],
      state: { current: "SUCCESS" },
    },
  },
};
