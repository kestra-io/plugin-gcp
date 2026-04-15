import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";

export default defaultViteConfig({
  plugin: "io.kestra.plugin.gcp",
  
  exposes: {
    "bigquery.Query": [
      {
        slotName: "topology-details",
        path: "./src/components/BigqueryQueryTopologyDetails.vue",
        additionalProperties: {
          // Height without execution: header (44) + Project/Location rows (~64)
          "height": 108,
          // Height with execution: header (44) + 4 rows (~64) + 3 gaps (~7) + padding (~16) + small buffer (~4)
          "heightWithExecution": 135,
          "customAction": { "label": "Show SQL", "taskProp": "sql", "lang": "sql" }
        },
      },
    ],
  },
  
});
