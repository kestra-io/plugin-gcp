<script setup lang="ts">
/**
 * Topology detail panel for the BigQuery Query task.
 *
 * Rendered inside the Kestra topology panel when a BigQuery Query node is
 * selected. Shows the SQL query (collapsible) and post-execution stats
 * (bytes processed, cost estimate, slot usage, cache status, and outputs).
 *
 * Props are injected by the Kestra runtime via @kestra-io/artifact-sdk.
 */
import { ref, computed } from "vue";
import type { TopologyDetailsProps } from "@kestra-io/artifact-sdk";

const props = defineProps<TopologyDetailsProps>();

// ---------------------------------------------------------------------------
// SQL toggle
// ---------------------------------------------------------------------------

/**
 * Extract the SQL string from the task property, which may be a plain string
 * or a Kestra Property<String> object ({ value: "..." }).
 */
const sqlText = computed<string | null>(() => {
  const raw = props.task?.sql;
  if (!raw) return null;
  if (typeof raw === "string") return raw;
  if (typeof raw === "object" && raw !== null && "value" in raw) {
    const v = (raw as { value: unknown }).value;
    return typeof v === "string" ? v : null;
  }
  return null;
});

const sqlVisible = ref(false);

function toggleSql() {
  sqlVisible.value = !sqlVisible.value;
}

// ---------------------------------------------------------------------------
// Execution — typed access helpers
// ---------------------------------------------------------------------------

interface DestinationTable {
  project: string;
  dataset: string;
  table: string;
}

interface ExecutionOutputs {
  jobId?: string;
  size?: number;
  destinationTable?: DestinationTable;
}

interface Metric {
  name: string;
  value: number;
  type: string;
  tags?: string[];
}

const outputs = computed<ExecutionOutputs | null>(() => {
  if (!props.execution) return null;
  return (props.execution.outputs ?? null) as ExecutionOutputs | null;
});

const metrics = computed<Metric[]>(() => {
  if (!props.execution) return [];
  const raw = props.execution.metrics;
  return Array.isArray(raw) ? (raw as Metric[]) : [];
});

function metricValue(name: string): number | null {
  const m = metrics.value.find((x) => x.name === name);
  return m != null ? m.value : null;
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  if (bytes < 1024 ** 4) return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
  return `${(bytes / 1024 ** 4).toFixed(3)} TB`;
}

/**
 * On-demand BigQuery pricing: $5 per TB processed.
 * Returns a human-readable cost string.
 */
function formatCost(bytes: number): string {
  const cost = (bytes / 1024 ** 4) * 5;
  if (cost < 0.01) return "< $0.01";
  return `~$${cost.toFixed(4)}`;
}

function formatSlotMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  return `${ms} ms`;
}

/** Duration is stored in nanoseconds (Timer type). */
function formatDurationNs(ns: number): string {
  const seconds = ns / 1_000_000_000;
  if (seconds < 1) return `${(ns / 1_000_000).toFixed(0)} ms`;
  return `${seconds.toFixed(2)} s`;
}

// ---------------------------------------------------------------------------
// Computed stats
// ---------------------------------------------------------------------------

const totalBytesBilled = computed(() => metricValue("total.bytes.billed"));
const totalBytesProcessed = computed(() => metricValue("total.bytes.processed"));
const estimatedBytesProcessed = computed(() => metricValue("estimated.bytes.processed"));
const totalSlotMs = computed(() => metricValue("total.slot.ms"));
const cacheHit = computed(() => metricValue("cache.hit"));
const durationNs = computed(() => metricValue("duration"));

const isCached = computed(() => cacheHit.value != null && cacheHit.value > 0);

const hasMetrics = computed(
  () =>
    totalBytesBilled.value != null ||
    totalBytesProcessed.value != null ||
    estimatedBytesProcessed.value != null ||
    totalSlotMs.value != null ||
    durationNs.value != null ||
    isCached.value
);

const hasOutputs = computed(
  () => outputs.value?.jobId != null || outputs.value?.size != null || outputs.value?.destinationTable != null
);

const hasExecution = computed(() => !!props.execution && (hasMetrics.value || hasOutputs.value));
</script>

<template>
  <div class="bq-details">
    <!-- Header: SQL toggle (Kestra already renders task-id + task-type above this slot) -->
    <div class="bq-details__header">
      <button
        v-if="sqlText"
        class="bq-details__sql-toggle"
        :class="{ 'bq-details__sql-toggle--active': sqlVisible }"
        type="button"
        @click="toggleSql"
      >
        SQL
        <span class="bq-details__sql-toggle-caret">{{ sqlVisible ? "▲" : "▼" }}</span>
      </button>
    </div>

    <!-- Collapsible SQL block — floats as an overlay so it doesn't push card height -->
    <div v-if="sqlText && sqlVisible" class="bq-details__sql-block">
      <pre class="bq-details__sql-pre">{{ sqlText }}</pre>
    </div>

    <!-- Post-execution stats -->
    <template v-if="hasExecution">
      <div class="bq-details__section-label">Execution</div>

      <!-- Cache badge — prominent when cached -->
      <div v-if="isCached" class="bq-details__cached-badge">Cached ✓</div>

      <dl v-if="hasOutputs" class="bq-details__grid">
        <template v-if="outputs?.jobId">
          <dt>Job ID</dt>
          <dd class="bq-details__monospace">{{ outputs.jobId }}</dd>
        </template>
        <template v-if="outputs?.size != null">
          <dt>Rows</dt>
          <dd>{{ outputs.size.toLocaleString() }}</dd>
        </template>
        <template v-if="outputs?.destinationTable">
          <dt>Destination</dt>
          <dd class="bq-details__monospace">
            {{ outputs.destinationTable.project }}.{{ outputs.destinationTable.dataset }}.{{ outputs.destinationTable.table }}
          </dd>
        </template>
      </dl>

      <dl v-if="hasMetrics" class="bq-details__grid">
        <template v-if="durationNs != null">
          <dt>Duration</dt>
          <dd>{{ formatDurationNs(durationNs) }}</dd>
        </template>
        <template v-if="totalBytesBilled != null">
          <dt>Bytes billed</dt>
          <dd>
            {{ formatBytes(totalBytesBilled) }}
            <span class="bq-details__cost">({{ formatCost(totalBytesBilled) }})</span>
          </dd>
        </template>
        <template v-if="totalBytesProcessed != null">
          <dt>Bytes processed</dt>
          <dd>{{ formatBytes(totalBytesProcessed) }}</dd>
        </template>
        <template v-if="estimatedBytesProcessed != null">
          <dt>Est. bytes</dt>
          <dd>{{ formatBytes(estimatedBytesProcessed) }}</dd>
        </template>
        <template v-if="totalSlotMs != null">
          <dt>Slot time</dt>
          <dd>{{ formatSlotMs(totalSlotMs) }}</dd>
        </template>
      </dl>
    </template>
  </div>
</template>

<style scoped>
.bq-details {
  padding: 0.5rem 1rem 0.5rem;
  font-size: 0.875rem;
  position: relative;
}

/* Header ------------------------------------------------------------------ */

.bq-details__header {
  display: flex;
  align-items: center;
  margin-bottom: 0.25rem;
}

/* SQL toggle button ------------------------------------------------------- */

.bq-details__sql-toggle {
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.2rem 0.5rem;
  font-size: 0.75rem;
  font-weight: 500;
  line-height: 1.4;
  border: 1px solid var(--ks-color-border, #d1d5db);
  border-radius: 4px;
  background: transparent;
  color: var(--ks-color-text-secondary, #6b7280);
  cursor: pointer;
  white-space: nowrap;
  flex-shrink: 0;
  transition: background-color 0.1s, color 0.1s;
}

.bq-details__sql-toggle:hover {
  background: var(--ks-color-surface-subtle, #f3f4f6);
}

.bq-details__sql-toggle--active {
  background: var(--ks-color-surface-subtle, #f3f4f6);
  color: inherit;
}

.bq-details__sql-toggle-caret {
  font-size: 0.6rem;
}

/* SQL block --------------------------------------------------------------- */
/* Rendered as an absolute overlay so it floats over nodes below and does   */
/* not push the card height (which is fixed by additionalProperties.height). */

.bq-details__sql-block {
  position: absolute;
  top: 100%;
  left: 0;
  right: 0;
  min-width: 100%;
  z-index: 9999;
  border: 1px solid var(--ks-color-border, #d1d5db);
  border-radius: 4px;
  background: var(--ks-color-surface-subtle, #f3f4f6);
  overflow: auto;
  max-height: 240px;
}

.bq-details__sql-pre {
  margin: 0;
  padding: 0.625rem 0.75rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.75rem;
  line-height: 1.6;
  white-space: pre;
}

/* Stats ------------------------------------------------------------------- */

.bq-details__section-label {
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--ks-color-text-secondary, #6b7280);
  margin-bottom: 0.5rem;
}

.bq-details__cached-badge {
  display: inline-block;
  margin-bottom: 0.625rem;
  padding: 0.15rem 0.5rem;
  font-size: 0.75rem;
  font-weight: 600;
  border-radius: 999px;
  background: #dcfce7;
  color: #15803d;
}

.bq-details__grid {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 0.3rem 1rem;
  margin: 0 0 0.75rem;
}

.bq-details__grid dt {
  font-weight: 500;
  color: var(--ks-color-text-secondary, #6b7280);
  white-space: nowrap;
}

.bq-details__grid dd {
  margin: 0;
  word-break: break-all;
}

.bq-details__monospace {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.75rem;
}

.bq-details__cost {
  font-size: 0.75rem;
  color: var(--ks-color-text-secondary, #6b7280);
}
</style>
