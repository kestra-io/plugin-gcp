<script setup lang="ts">
import BigqueryQueryArtifact from "./components/BigqueryQueryArtifact.vue";
import { SLOTS } from "@kestra-io/artifact-sdk";
</script>

<template>
    <div style="padding: 1rem; font-family: sans-serif">
        <BigqueryQueryArtifact v-bind="SLOTS['execution-artifact'].defaultProps" />
    </div>
</template>