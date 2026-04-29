<script setup lang="ts">
import BigqueryQueryTopologyDetails from "./components/BigqueryQueryTopologyDetails.vue";
import { SLOTS } from "@kestra-io/artifact-sdk";
</script>

<template>
  <div style="padding: 1rem; font-family: sans-serif">
    <BigqueryQueryTopologyDetails v-bind="SLOTS['topology-details'].defaultProps" />
  </div>
</template>
