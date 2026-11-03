import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";

export default defaultViteConfig({
    plugin: "io.kestra.plugin.gcp",

    exposes: {
        "bigquery.Query": [
            {
                slotName: "execution-artifact/details",
                path: "./src/components/BigqueryQueryArtifact.vue",
                additionalProperties: { title: "Query details" },
            },
        ],
    },
});