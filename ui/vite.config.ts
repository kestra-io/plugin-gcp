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
          // Height with execution: above + Job Details + Cost & Performance sections (~265)
          // Add ~26px if destinationTable is shown (rare for plain SELECT queries)
          "heightWithExecution": 320,
          "customAction": { "label": "Show SQL", "taskProp": "sql", "lang": "sql" }
        },
      },
    ],
  },
  
});
