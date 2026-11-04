import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";

export default defaultViteConfig({
    plugin: "io.kestra.plugin.gcp",

    exposes: {
        "bigquery.Query": [
            {
                slotName: "topology-details",
                path: "./src/components/BigqueryQueryTopologyDetails.vue",
                additionalProperties: {
                    // node base (56) + KsTopologyDetails rows (~31px each)
                    // no execution: Project + Location (2 rows)
                    height: 120,
                    // with an execution the host adds a ~30px badge row under the node, so this is height + 30
                    heightWithExecution: 150,
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