<script setup lang="ts">
import type { TopologyDetailsProps } from "@kestra-io/artifact-sdk";
import { computed, useAttrs } from "vue";

interface BigqueryQueryTopologyDetailsProps extends TopologyDetailsProps {
  outputs?: Record<string, unknown> | null;
  metrics?: Array<{ name: string; value: number; taskId?: string }> | null;
}

const props = defineProps<BigqueryQueryTopologyDetailsProps>();
const attrs = useAttrs();
const isFullView = computed(() => attrs.displayMode === "full");

const taskId = computed(() => props.task?.id as string | undefined);

const projectId = computed(() => props.task?.projectId as string | undefined);
const location = computed(() => props.task?.location as string | undefined);

// Execution state
const hasExecution = computed(() => !!props.execution?.id);

const taskRun = computed(() => {
  const list = props.execution?.taskRunList as any[] | undefined;
  return list?.filter((tr: any) => tr.taskId === taskId.value).at(-1);
});

const taskOutputs = computed(() =>
  props.outputs ?? taskRun.value?.outputs ?? null
);

// Parse project and location from the job ID as fallback.
// BigQuery job IDs have the format: project:location.jobname
const resolvedProject = computed(() => {
  if (projectId.value) return projectId.value;
  const jid = taskOutputs.value?.jobId as string | undefined;
  if (!jid) return undefined;
  const colonIdx = jid.indexOf(":");
  return colonIdx > 0 ? jid.slice(0, colonIdx) : undefined;
});

const resolvedLocation = computed(() => {
  if (location.value) return location.value;
  const jid = taskOutputs.value?.jobId as string | undefined;
  if (!jid) return undefined;
  const colonIdx = jid.indexOf(":");
  const dotIdx = jid.indexOf(".", colonIdx);
  if (colonIdx < 0 || dotIdx < 0) return undefined;
  return jid.slice(colonIdx + 1, dotIdx);
});

// Metrics
interface MetricEntry {
  name: string;
  value: number;
  taskId?: string;
}

const filteredMetrics = computed(() =>
  (props.metrics ?? []).filter(
    (m) => !m.taskId || m.taskId === taskId.value
  )
);

const getMetric = (name: string) => filteredMetrics.value.find((m) => m.name === name)?.value;

const bytesBilled = computed(() => getMetric("total.bytes.billed"));
const bytesProcessed = computed(() => getMetric("total.bytes.processed"));
const slotMs = computed(() => getMetric("total.slot.ms"));
const cacheHit = computed(() => getMetric("cache.hit") === 1);
const durationMs = computed(() => getMetric("duration"));

function formatBytes(b?: number): string {
  if (b === undefined) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let v = b;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

function formatCost(b?: number): string {
  if (b === undefined) return "—";
  const cost = (b / Math.pow(1024, 4)) * 5;
  return cost < 0.01 ? "< $0.01" : `~$${cost.toFixed(4)}`;
}

function formatDuration(ms?: number): string {
  if (ms === undefined) return "—";
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(2)} s`;
}

function formatSlotMs(v?: number): string {
  return v === undefined ? "—" : `${v.toLocaleString()} slot·ms`;
}

</script>

<template>
  <div class="bq-details">
    <!-- Single unified grid: always shows Project + Location; adds Duration + Estimated Cost post-execution -->
    <dl class="bq-grid">
      <dt>Project</dt>
      <dd>{{ resolvedProject ?? "—" }}</dd>
      <dt>Location</dt>
      <dd>{{ resolvedLocation ?? "—" }}</dd>
      <template v-if="hasExecution">
        <dt>Duration</dt>
        <dd>{{ formatDuration(durationMs) }}</dd>
        <dt>Estimated cost</dt>
        <dd>{{ formatCost(bytesBilled) }} <span class="bq-hint">@$5/TB</span></dd>
      </template>
    </dl>

    <!-- Post-execution only -->
    <template v-if="hasExecution">

      <!-- Full details: only when displayMode="full" (rendered in the drawer) -->
      <template v-if="isFullView">
        <section class="bq-section">
          <h4 class="bq-section__title">Job Details</h4>
          <dl class="bq-grid">
            <dt>Job ID</dt>
            <dd class="bq-mono">{{ taskOutputs?.jobId ?? "—" }}</dd>
            <dt>Rows</dt>
            <dd>{{ taskOutputs?.size !== undefined ? taskOutputs.size.toLocaleString() : "—" }}</dd>
            <template v-if="taskOutputs?.destinationTable">
              <dt>Destination</dt>
              <dd class="bq-mono">
                {{ [taskOutputs.destinationTable.project, taskOutputs.destinationTable.dataset, taskOutputs.destinationTable.table].join(".") }}
              </dd>
            </template>
          </dl>
        </section>

        <section class="bq-section">
          <h4 class="bq-section__title">Cost &amp; Performance</h4>
          <dl class="bq-grid">
            <dt>Bytes billed</dt>
            <dd>{{ formatBytes(bytesBilled) }}</dd>
            <dt>Estimated cost</dt>
            <dd>{{ formatCost(bytesBilled) }} <span class="bq-hint">@$5/TB</span></dd>
            <dt>Bytes processed</dt>
            <dd>{{ formatBytes(bytesProcessed) }}</dd>
            <dt>Slot time</dt>
            <dd>{{ formatSlotMs(slotMs) }}</dd>
            <dt>Duration</dt>
            <dd>{{ formatDuration(durationMs) }}</dd>
            <dt>Cache hit</dt>
            <dd>
              <span :class="['bq-badge', cacheHit ? 'bq-badge--hit' : 'bq-badge--miss']">
                {{ cacheHit ? "Yes" : "No" }}
              </span>
            </dd>
          </dl>
        </section>
      </template>
    </template>

  </div>
</template>

<style scoped>
.bq-details {
  position: relative;
  z-index: 1;
  padding: 0.5rem 0.75rem;
  font-size: 0.7rem;
  line-height: 1.4;
}

.bq-section {
  margin-bottom: 0.5rem;
}

.bq-section:last-child {
  margin-bottom: 0;
}

.bq-section__title {
  margin: 0 0 0.25rem;
  font-size: 0.6875rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--ks-color-text-secondary, #6b7280);
}

.bq-grid {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 0.15rem 0.625rem;
  margin: 0;
}

.bq-grid dt {
  font-weight: 500;
  color: var(--ks-color-text-secondary, #6b7280);
  white-space: nowrap;
}

.bq-grid dd {
  margin: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.bq-mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.7rem;
}

.bq-hint {
  font-size: 0.7rem;
  color: var(--ks-color-text-secondary, #6b7280);
}

.bq-badge {
  display: inline-block;
  padding: 0.1rem 0.4rem;
  border-radius: 3px;
  font-size: 0.7rem;
  font-weight: 600;
}

.bq-badge--hit {
  background: #d1fae5;
  color: #065f46;
}

.bq-badge--miss {
  background: var(--ks-color-surface-subtle, #f3f4f6);
  color: var(--ks-color-text-secondary, #6b7280);
}
</style>
