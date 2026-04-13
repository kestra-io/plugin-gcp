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
import { ref, computed, onMounted, onUnmounted } from "vue";
import type { TopologyDetailsProps } from "@kestra-io/artifact-sdk";

const props = defineProps<TopologyDetailsProps>();

// ---------------------------------------------------------------------------
// Label (étiquette) state
// ---------------------------------------------------------------------------

const containerRef = ref<HTMLElement | null>(null);
const activeLabelType = ref<"sql" | "results" | null>(null);
const labelStyle = ref<Record<string, string>>({});

function openLabel(type: "sql" | "results") {
  if (!containerRef.value) return;
  const rect = containerRef.value.getBoundingClientRect();
  const maxWidth = Math.min(480, window.innerWidth - rect.right - 40);
  labelStyle.value = {
    top: `${rect.top + rect.height / 2}px`,
    left: `${rect.right + 20}px`,
    transform: "translateY(-50%)",
    width: `${maxWidth}px`,
  };
  activeLabelType.value = type;
}

function closeLabel() {
  activeLabelType.value = null;
}

function toggleLabel(type: "sql" | "results") {
  if (activeLabelType.value === type) {
    closeLabel();
  } else {
    openLabel(type);
  }
}

function onEscape(e: KeyboardEvent) {
  if (e.key === "Escape") closeLabel();
}

onMounted(() => window.addEventListener("keydown", onEscape));
onUnmounted(() => window.removeEventListener("keydown", onEscape));

// ---------------------------------------------------------------------------
// SQL
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
  <div class="bq-details" ref="containerRef">
    <!-- Button row -->
    <div class="bq-details__actions">
      <button
        v-if="sqlText"
        class="bq-btn"
        type="button"
        @click="toggleLabel('sql')"
      >
        SQL
      </button>
      <button
        v-if="hasExecution"
        class="bq-btn"
        type="button"
        @click="toggleLabel('results')"
      >
        Results
      </button>
    </div>

    <Teleport to="body">
      <template v-if="activeLabelType">
        <!-- Transparent full-screen overlay — catches outside clicks -->
        <div class="bq-label-overlay" @click="closeLabel" />

        <!-- Étiquette panel -->
        <div class="bq-label" :style="labelStyle" role="dialog" aria-modal="true">
          <div class="bq-label__header">
            <span class="bq-label__title">
              {{ activeLabelType === "sql" ? "SQL Query" : "Query Results" }}
            </span>
            <button class="bq-label__close" type="button" @click="closeLabel" aria-label="Close">✕</button>
          </div>

          <div class="bq-label__body">
            <!-- SQL content -->
            <pre v-if="activeLabelType === 'sql'" class="bq-label__pre">{{ sqlText }}</pre>

            <!-- Results content -->
            <template v-if="activeLabelType === 'results'">
              <!-- Cache badge -->
              <div v-if="isCached" class="bq-cached-badge">Cached ✓</div>

              <!-- Outputs: job, rows, destination -->
              <dl v-if="hasOutputs" class="bq-grid">
                <template v-if="outputs?.jobId">
                  <dt>Job ID</dt>
                  <dd class="bq-mono">{{ outputs.jobId }}</dd>
                </template>
                <template v-if="outputs?.size != null">
                  <dt>Rows</dt>
                  <dd>{{ outputs.size.toLocaleString() }}</dd>
                </template>
                <template v-if="outputs?.destinationTable">
                  <dt>Destination</dt>
                  <dd class="bq-mono">
                    {{ outputs.destinationTable.project }}.{{ outputs.destinationTable.dataset }}.{{ outputs.destinationTable.table }}
                  </dd>
                </template>
              </dl>

              <!-- Metrics: bytes, cost, slot time, duration -->
              <dl v-if="hasMetrics" class="bq-grid">
                <template v-if="durationNs != null">
                  <dt>Duration</dt>
                  <dd>{{ formatDurationNs(durationNs) }}</dd>
                </template>
                <template v-if="totalBytesBilled != null">
                  <dt>Bytes billed</dt>
                  <dd>
                    {{ formatBytes(totalBytesBilled) }}
                    <span class="bq-cost">({{ formatCost(totalBytesBilled) }})</span>
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
        </div>
      </template>
    </Teleport>
  </div>
</template>

<style scoped>
/* Container — deliberately compact so it does not affect node height */
.bq-details {
  padding: 0.25rem 0.75rem;
  font-size: 0.875rem;
}

/* Button row ------------------------------------------------------------- */

.bq-details__actions {
  display: flex;
  align-items: center;
  gap: 0.375rem;
}

.bq-btn {
  display: inline-flex;
  align-items: center;
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
  transition: background-color 0.1s, color 0.1s;
}

.bq-btn:hover {
  background: var(--ks-color-surface-subtle, #f3f4f6);
  color: var(--ks-color-text, #111827);
}

/* Overlay — transparent, only catches outside clicks -------------------- */

.bq-label-overlay {
  position: fixed;
  inset: 0;
  z-index: 9998;
}

/* Étiquette panel ------------------------------------------------------- */

.bq-label {
  position: fixed;
  z-index: 9999;
  background: var(--ks-color-surface, #ffffff);
  border: 1px solid var(--ks-color-border, #e5e7eb);
  border-radius: 8px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.12), 0 1px 4px rgba(0, 0, 0, 0.06);
  display: flex;
  flex-direction: column;
  max-height: 70vh;
  overflow: hidden;
}

/* Left-pointing arrow beak (fill — matches surface) */
.bq-label::before {
  content: "";
  position: absolute;
  left: -12px;
  top: 50%;
  transform: translateY(-50%);
  border: 12px solid transparent;
  border-right: 12px solid var(--ks-color-surface, #ffffff);
  z-index: 1;
}

/* Left-pointing arrow beak (border outline, sits behind ::before) */
.bq-label::after {
  content: "";
  position: absolute;
  left: -14px;
  top: 50%;
  transform: translateY(-50%);
  border: 13px solid transparent;
  border-right: 13px solid var(--ks-color-border, #e5e7eb);
  z-index: 0;
}

/* Header ----------------------------------------------------------------- */

.bq-label__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.625rem 0.875rem;
  border-bottom: 1px solid var(--ks-color-border, #e5e7eb);
  background: var(--ks-color-surface-subtle, #f9fafb);
  flex-shrink: 0;
}

.bq-label__title {
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--ks-color-text, #111827);
}

.bq-label__close {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 1.75rem;
  height: 1.75rem;
  padding: 0;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: var(--ks-color-text-secondary, #6b7280);
  cursor: pointer;
  font-size: 0.875rem;
  transition: background-color 0.1s, color 0.1s;
}

.bq-label__close:hover {
  background: var(--ks-color-surface-subtle, #f3f4f6);
  color: var(--ks-color-text, #111827);
}

/* Body ------------------------------------------------------------------- */

.bq-label__body {
  overflow-y: auto;
  padding: 0.875rem;
  flex: 1;
}

/* SQL pre ---------------------------------------------------------------- */

.bq-label__pre {
  margin: 0;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.8125rem;
  line-height: 1.6;
  white-space: pre;
  color: var(--ks-color-text, #111827);
}

/* Results content -------------------------------------------------------- */

.bq-cached-badge {
  display: inline-block;
  margin-bottom: 0.75rem;
  padding: 0.15rem 0.5rem;
  font-size: 0.75rem;
  font-weight: 600;
  border-radius: 999px;
  background: var(--ks-color-success-subtle, #dcfce7);
  color: var(--ks-color-success, #15803d);
}

.bq-grid {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 0.35rem 1.25rem;
  margin: 0 0 0.875rem;
}

.bq-grid:last-child {
  margin-bottom: 0;
}

.bq-grid dt {
  font-size: 0.8125rem;
  font-weight: 500;
  color: var(--ks-color-text-secondary, #6b7280);
  white-space: nowrap;
}

.bq-grid dd {
  margin: 0;
  font-size: 0.8125rem;
  word-break: break-all;
  color: var(--ks-color-text, #111827);
}

.bq-mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.75rem;
}

.bq-cost {
  font-size: 0.75rem;
  color: var(--ks-color-text-secondary, #6b7280);
}
</style>
