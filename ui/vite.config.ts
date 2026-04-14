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
          // Height with execution: sized for 4 rows (Project, Location, Duration,
          // Estimated Cost) plus the "Details" button. The popover teleports to <body>
          // so it never affects node height.
          "heightWithExecution": 165,
          "customAction": { "label": "Show SQL", "taskProp": "sql", "lang": "sql" }
        },
      },
    ],
  },
  
});
