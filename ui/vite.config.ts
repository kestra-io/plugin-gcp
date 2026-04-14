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
          // Height with execution: sized for the expanded state so layout is never broken.
          // Collapsed (3 rows) leaves whitespace inside the node border, which is acceptable.
          "heightWithExecution": 320,
          "customAction": { "label": "Show SQL", "taskProp": "sql", "lang": "sql" }
        },
      },
    ],
  },
  
});
