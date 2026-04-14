<script setup lang="ts">
import type { TopologyDetailsProps } from "@kestra-io/artifact-sdk";
import { computed, ref, watch } from "vue";

const props = defineProps<TopologyDetailsProps>();

// Task fields
const taskId = computed(() => props.task?.id as string | undefined);
const projectId = computed(() => props.task?.projectId as string | undefined);
const location = computed(() => props.task?.location as string | undefined);

// Execution state
const hasExecution = computed(() => !!props.execution?.id);
const executionId = computed(() => props.execution?.id as string | undefined);

const taskRunId = computed(() => {
  const list = props.execution?.taskRunList as any[] | undefined;
  return list?.filter((tr: any) => tr.taskId === taskId.value).at(-1)?.id as string | undefined;
});

// Task outputs: fetched from the v2 OutputController endpoint
const taskOutputs = ref<Record<string, any> | null>(null);

async function fetchOutputs(execId: string, trId: string) {
  try {
    const tenant = props.execution?.tenantId as string | undefined;
    const base = tenant ? `/api/v1/${tenant}/outputs` : "/api/v1/outputs";
    const res = await fetch(`${base}/${execId}/${trId}`, { credentials: "include" });
    if (!res.ok) return;
    taskOutputs.value = await res.json();
  } catch {
    // silently ignore — outputs are best-effort
  }
}

watch(
  [executionId, taskRunId],
  ([execId, trId]) => {
    taskOutputs.value = null;
    if (execId && trId) fetchOutputs(execId, trId);
  },
  { immediate: true }
);

// Metrics (best-effort fetch)
interface MetricEntry {
  name: string;
  value: number;
  taskId?: string;
}

const metrics = ref<MetricEntry[]>([]);

async function fetchMetrics(execId: string) {
  try {
    const res = await fetch(`/api/v1/metrics/${execId}`, { credentials: "include" });
    if (!res.ok) return;
    const data = await res.json();
    metrics.value = ((data.results as MetricEntry[]) ?? []).filter(
      (m) => !m.taskId || m.taskId === taskId.value
    );
  } catch {
    // silently ignore — metrics are best-effort
  }
}

watch(executionId, (id) => { if (id) fetchMetrics(id); }, { immediate: true });

const getMetric = (name: string) => metrics.value.find((m) => m.name === name)?.value;

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
    <!-- Always visible: task config -->
    <section class="bq-section">
      <dl class="bq-grid">
        <dt>Project</dt>
        <dd>{{ projectId ?? "—" }}</dd>
        <dt>Location</dt>
        <dd>{{ location ?? "—" }}</dd>
      </dl>
    </section>

    <!-- Post-execution only -->
    <template v-if="hasExecution">
      <!-- Job details -->
      <section v-if="taskOutputs" class="bq-section">
        <h4 class="bq-section__title">Job Details</h4>
        <dl class="bq-grid">
          <dt>Job ID</dt>
          <dd class="bq-mono">{{ taskOutputs.jobId ?? "—" }}</dd>
          <dt>Rows</dt>
          <dd>{{ taskOutputs.size !== undefined ? taskOutputs.size.toLocaleString() : "—" }}</dd>
          <template v-if="taskOutputs.destinationTable">
            <dt>Destination</dt>
            <dd class="bq-mono">
              {{ [taskOutputs.destinationTable.project, taskOutputs.destinationTable.dataset, taskOutputs.destinationTable.table].join(".") }}
            </dd>
          </template>
        </dl>
      </section>

      <!-- Cost & Performance -->
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
  </div>
</template>

<style scoped>
.bq-details {
  padding: 0.75rem 1rem;
  font-size: 0.8125rem;
  line-height: 1.5;
}

.bq-section {
  margin-bottom: 0.875rem;
}

.bq-section:last-child {
  margin-bottom: 0;
}

.bq-section__title {
  margin: 0 0 0.375rem;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--ks-color-text-secondary, #6b7280);
}

.bq-grid {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 0.2rem 0.875rem;
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
  font-size: 0.75rem;
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
