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
                    // with execution: + Duration + Estimated cost (4 rows)
                    heightWithExecution: 185,
                    customAction: { label: "Show Details", taskProp: "sql", lang: "sql" },
                },
            },
            {
                slotName: "topology-task-drawer",
                path: "./src/components/BigqueryQueryTopologyDetails.vue",
            },
        ],
    },
});