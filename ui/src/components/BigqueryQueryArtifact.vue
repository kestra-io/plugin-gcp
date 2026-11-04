<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import BigqueryQueryTopologyDetails from "./BigqueryQueryTopologyDetails.vue";

// The Artifacts tab passes no displayMode and no progress: render the full view of the topology details.
defineOptions({ inheritAttrs: false });
const props = defineProps<KnownSlotProps["execution-artifact"]>();
</script>

<template>
    <BigqueryQueryTopologyDetails v-bind="props" :progress="[]" displayMode="full" />
</template>
