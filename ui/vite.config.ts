import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";

// Post-execution card has 4 rows (node base 56 + 4 x ~31 = 180, measured 184 on Kestra 2.0.9)
// + 1px border, so it is not height + the host badge row.
const POST_EXECUTION_CONTENT_HEIGHT = 185;

export default defaultViteConfig({
    plugin: "io.kestra.plugin.gcp",

    exposes: {
        "bigquery.Query": [
            {
                slotName: "topology-details",
                path: "./src/components/BigqueryQueryTopologyDetails.vue",
                additionalProperties: {
                    height: 120,
                    heightWithExecution: POST_EXECUTION_CONTENT_HEIGHT,
                    customAction: { label: "Show Details", taskProp: "sql", lang: "sql" },
                },
            },
            {
                slotName: "topology-task-drawer",
                path: "./src/components/BigqueryQueryTopologyDetails.vue",
            },
            {
                slotName: "execution-artifact/details",
                path: "./src/components/BigqueryQueryArtifact.vue",
                additionalProperties: { title: "Query details" },
            },
        ],
    },
});