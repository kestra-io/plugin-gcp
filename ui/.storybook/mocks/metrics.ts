// Storybook fake for `@kestra-io/kestra-sdk/metrics`. Returns a representative set of BigQuery job
// metrics so the post-execution story shows real-looking duration, bytes and cost instead of "—".
export async function searchByExecution() {
    return {
        results: [
            { name: "total.bytes.billed", value: 1_099_511_627_776 }, // 1 TiB
            { name: "total.bytes.processed", value: 987_842_478_899 }, // ~0.9 TiB
            { name: "total.slot.ms", value: 45_678 },
            { name: "cache.hit", value: 0 },
            { name: "duration", value: 3_420 },
        ],
    };
}