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
      state: { current: "SUCCESS", startDate: "2024-01-15T10:00:00Z" },
      taskRunList: [
        {
          id: "tr-001",
          taskId: "query-bq",
          executionId: "exec-abc123",
          outputs: {
            jobId: "my-gcp-project:US.bqjob_r1234abcd_56789xyz",
            size: 42500,
            destinationTable: {
              project: "my-gcp-project",
              dataset: "_tmp_results",
              table: "_anon_abc123def456",
            },
          },
        },
      ],
    },
  },
};
