// Storybook fake for `@kestra-io/kestra-sdk/executions`. The component fetches the full execution
// to read task-run outputs; the sample task run carries the `query-bq` outputs so the post-execution
// story can populate the Job details and Cost & performance sections.
import { SAMPLE_OUTPUTS } from "./outputs";

export async function execution({ executionId }: { executionId: string }) {
    return {
        id: executionId,
        taskRunList: [{ id: "tr-001", taskId: "query-bq", executionId, outputs: SAMPLE_OUTPUTS }],
    };
}