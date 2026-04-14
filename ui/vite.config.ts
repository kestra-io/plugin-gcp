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
          // Height with execution: sized for the COLLAPSED state (~192px content).
          // The expanded state overflows the layout box but renders above the canvas
          // (node-wrapper z-index: 150000, no overflow:hidden on vue-flow__node).
          // In default horizontal layout this is always safe; vertical flows may see
          // the expanded content touch the next node, which is an acceptable edge case.
          "heightWithExecution": 200,
          "customAction": { "label": "Show SQL", "taskProp": "sql", "lang": "sql" }
        },
      },
    ],
  },
  
});
