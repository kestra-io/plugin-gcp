<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed, ref, watch, useAttrs } from "vue";
import { useI18n } from "vue-i18n";
import { KsTopologyDetails } from "@kestra-io/design-system";
import * as MetricsAPI from "@kestra-io/kestra-sdk/metrics";
import * as FlowAPI from "@kestra-io/kestra-sdk/flows";
import * as ExecutionAPI from "@kestra-io/kestra-sdk/executions";
import * as OutputsAPI from "@kestra-io/kestra-sdk/outputs";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            project: "Project",
            location: "Location",
            duration: "Duration",
            estimatedCost: "Estimated cost",
            jobDetails: "Job details",
            jobId: "Job ID",
            rows: "Rows",
            destination: "Destination",
            costAndPerformance: "Cost and performance",
            bytesBilled: "Bytes billed",
            bytesProcessed: "Bytes processed",
            slotTime: "Slot time",
            cacheHit: "Cache hit",
            yes: "Yes",
            no: "No",
        },
    },
});

const props = defineProps<KnownSlotProps["topology-details"]>();
const attrs = useAttrs();
const isFullView = computed(() => attrs.displayMode === "full");
const namespace = computed(() => props.namespace);
const flowId = computed(() => props.flowId);

const taskId = computed(() => props.task?.id as string | undefined);

// Full flow task config (fetched via SDK — inherits host EE auth automatically)
const flowTask = ref<Record<string, any> | null>(null);

async function loadFlowTask() {
    if (!namespace.value || !flowId.value) return;
    if (namespace.value.startsWith("{") || flowId.value.startsWith("{")) return;
    try {
        const f = await FlowAPI.flow(
            { namespace: namespace.value, id: flowId.value },
            {
                showMessageOnError: false,
                validateStatus: (s: number) => s === 200 || s === 404,
            },
        );
        const tasks = (f as any).tasks as any[] | undefined;
        flowTask.value = tasks?.find((t: any) => t.id === taskId.value) ?? null;
    } catch {
        /* best-effort */
    }
}

watch(
    [namespace, flowId],
    ([ns, fid]) => {
        if (ns && fid && !ns.startsWith("{") && !fid.startsWith("{")) loadFlowTask();
    },
    { immediate: true },
);

const projectId = computed(
    () => ((props.task as any).projectId ?? flowTask.value?.projectId) as string | undefined,
);
const location = computed(
    () => ((props.task as any).location ?? flowTask.value?.location) as string | undefined,
);

// Execution state
const hasExecution = computed(() => !!props.execution?.id);
const executionId = computed(() => props.execution?.id as string | undefined);

const taskRun = computed(() => {
    const list = props.execution?.taskRunList as any[] | undefined;
    return list?.filter((tr: any) => tr.taskId === taskId.value).at(-1);
});

// Fetch the full execution to get outputs (props.execution has task runs but no outputs)
const fetchedOutputs = ref<Record<string, any> | null>(null);

async function loadTaskOutputs(execId: string) {
    try {
        const exec = await ExecutionAPI.execution(
            { executionId: execId },
            {
                showMessageOnError: false,
                validateStatus: (s: number) => s === 200 || s === 404,
            },
        );
        const list = exec.taskRunList as any[] | undefined;
        const tr = list?.filter((tr) => tr.taskId === taskId.value).at(-1);
        // for 1.3
        fetchedOutputs.value = (tr as any)?.outputs ?? null;

        // for 1.4+, fetch via dedicated API if not present in execution response
        if (!fetchedOutputs.value) {
            fetchedOutputs.value = await OutputsAPI.taskRunOutputs({
                executionId: execId,
                taskRunId: tr?.id,
            });
        }
    } catch {
        /* best-effort */
    }
}

watch(
    executionId,
    (id) => {
        if (id) loadTaskOutputs(id);
    },
    { immediate: true },
);

const taskOutputs = computed(() => fetchedOutputs.value ?? taskRun.value?.outputs ?? null);

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

// Metrics (fetched via SDK — best-effort)
interface MetricEntry {
    name: string;
    value: number;
    taskId?: string;
}

const metrics = ref<MetricEntry[]>([]);

async function loadMetrics(execId: string) {
    try {
        const resp = await MetricsAPI.searchByExecution(
            { executionId: execId },
            {
                showMessageOnError: false,
                validateStatus: (s: number) => s === 200 || s === 404,
            },
        );
        metrics.value = ((resp.results as MetricEntry[]) ?? []).filter(
            (m) => !m.taskId || m.taskId === taskId.value,
        );
    } catch {
        /* best-effort */
    }
}

watch(
    executionId,
    (id) => {
        if (id) loadMetrics(id);
    },
    { immediate: true },
);

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
    while (v >= 1024 && i < units.length - 1) {
        v /= 1024;
        i++;
    }
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

const summaryRows = computed(() => {
    const rows = [
        { label: t("project"), value: resolvedProject.value ?? "—" },
        { label: t("location"), value: resolvedLocation.value ?? "—" },
    ];
    if (hasExecution.value) {
        rows.push({ label: t("duration"), value: formatDuration(durationMs.value) });
        rows.push({ label: t("estimatedCost"), value: `${formatCost(bytesBilled.value)} @$5/TB` });
    }
    return rows;
});

const jobRows = computed(() => {
    const rows = [
        { label: t("jobId"), value: (taskOutputs.value?.jobId as string) ?? "—" },
        {
            label: t("rows"),
            value:
                taskOutputs.value?.size !== undefined
                    ? taskOutputs.value.size.toLocaleString()
                    : "—",
        },
    ];
    const dt = taskOutputs.value?.destinationTable;
    if (dt) {
        rows.push({ label: t("destination"), value: [dt.project, dt.dataset, dt.table].join(".") });
    }
    return rows;
});

const perfRows = computed(() => [
    { label: t("bytesBilled"), value: formatBytes(bytesBilled.value) },
    { label: t("estimatedCost"), value: `${formatCost(bytesBilled.value)} @$5/TB` },
    { label: t("bytesProcessed"), value: formatBytes(bytesProcessed.value) },
    { label: t("slotTime"), value: formatSlotMs(slotMs.value) },
    { label: t("duration"), value: formatDuration(durationMs.value) },
    { label: t("cacheHit"), value: cacheHit.value ? t("yes") : t("no") },
]);
</script>

<template>
    <div class="bq-details">
        <KsTopologyDetails :rows="summaryRows" />

        <!-- Job details: full view, post-execution only -->
        <section v-if="hasExecution && isFullView" class="bq-section">
            <h4 class="bq-section__title">{{ t("jobDetails") }}</h4>
            <KsTopologyDetails :rows="jobRows" />
        </section>

        <!-- Cost & performance: full view, post-execution only -->
        <section v-if="hasExecution && isFullView" class="bq-section">
            <h4 class="bq-section__title">{{ t("costAndPerformance") }}</h4>
            <KsTopologyDetails :rows="perfRows" />
        </section>
    </div>
</template>

<style scoped>
.bq-section {
    margin: var(--ks-spacing-3) var(--ks-spacing-3) 0;
}

.bq-section__title {
    margin: 0 0 var(--ks-spacing-2);
    font-size: var(--ks-font-size-xs);
    font-weight: 600;
    color: var(--ks-text-secondary);
}
</style>