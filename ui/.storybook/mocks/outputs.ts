// Storybook fake for `@kestra-io/kestra-sdk/outputs`, plus the shared sample outputs object the
// executions mock reuses.
export const SAMPLE_OUTPUTS = {
    jobId: "my-gcp-project:EU.bquxjob_1a2b3c4d_1234567890ab",
    size: 15234,
    destinationTable: {
        project: "my-gcp-project",
        dataset: "analytics",
        table: "users_active",
    },
};

export async function taskRunOutputs() {
    return SAMPLE_OUTPUTS;
}