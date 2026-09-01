<script setup lang="ts">
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { computed, ref, watch, onMounted, onBeforeUnmount, useAttrs } from "vue";
import { useI18n } from "vue-i18n";
import { KsTopologyDetails, KsEditor } from "@kestra-io/design-system";
import { renderExpressions } from "@kestra-io/kestra-sdk/expressions";

const { t } = useI18n({
    inheritLocale: true,
    useScope: "local",
    messages: {
        en: {
            project: "Project",
            location: "Location",
            query: "Query",
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

// props.task is the host-merged, complete task definition (graph node + parsed flow source) —
// no separate flow fetch needed to read task-specific config.
const projectId = computed(() => (props.task as any)?.projectId as string | undefined);
const location = computed(() => (props.task as any)?.location as string | undefined);
const sql = computed(() => (props.task as any)?.sql as string | undefined);

// Execution state
const hasExecution = computed(() => !!props.execution?.id);
const executionId = computed(() => props.execution?.id as string | undefined);

const resolved = (v?: string) => (v && !v.startsWith("{") ? v : undefined);

// Resolve the task config's Pebble expressions (projectId / location / sql) for display via
// POST /expressions/render. Rendering is server-side and all-or-nothing per expression: anything the
// restricted display engine cannot resolve (env(), kv(), missing vars, …) comes back unchanged, and
// any failure keeps the raw template (see display()). Only values that actually contain a `{{…}}` are
// worth a round-trip. This call still goes through @kestra-io/kestra-sdk directly (host-side expression
// rendering isn't available yet — kestra-io/kestra-ee#10488), so the tenant is threaded from the
// `tenant` prop the host now provides rather than read from localStorage.
const EXPRESSION_RE = /\{\{.*?}}/;
const rendered = ref<Record<string, string>>({});

async function loadRenderedExpressions() {
    const values = [projectId.value, location.value, sql.value].filter(
        (v): v is string => typeof v === "string" && EXPRESSION_RE.test(v),
    );
    if (!values.length) {
        rendered.value = {};
        return;
    }
    try {
        const { rendered: result } = await renderExpressions(
            {
                expressions: values,
                tenant: props.tenant,
                executionId: executionId.value,
                namespace: resolved(namespace.value),
                flowId: resolved(flowId.value),
            },
            {
                // Best-effort display call: keep failures off the host's global error UI.
                validateStatus: (s: number) => s === 200 || s === 404,
                showMessageOnError: false,
            },
        );
        rendered.value = result ?? {};
    } catch {
        // Drop rendered values so display() falls back to the raw template.
        rendered.value = {};
    }
}

watch(
    [projectId, location, sql, executionId, namespace, flowId],
    loadRenderedExpressions,
    { immediate: true },
);

/** Returns the rendered value for `value`, falling back to the raw value. */
const display = (value?: string) =>
    value === undefined ? undefined : (rendered.value[value] ?? value);

const taskRun = computed(() => {
    const list = props.execution?.taskRunList as any[] | undefined;
    return list?.filter((tr: any) => tr.taskId === taskId.value).at(-1);
});

// props.execution carries task runs but no outputs; fetch the current task run's outputs from the
// host-provided lazy fetcher (scoped server-side to this execution/task — costs no request if never
// called, and resolves to {} outside an execution).
const fetchedOutputs = ref<Record<string, any> | null>(null);

async function loadTaskOutputs() {
    try {
        fetchedOutputs.value = (await props.fetchOutputs?.({ taskRunId: taskRun.value?.id })) ?? null;
    } catch {
        /* best-effort */
    }
}

watch(
    executionId,
    (id) => {
        if (id) loadTaskOutputs();
    },
    { immediate: true },
);

const taskOutputs = computed(() => fetchedOutputs.value ?? taskRun.value?.outputs ?? null);

// Parse project and location from the job ID as fallback.
// BigQuery job IDs have the format: project:location.jobname
const resolvedProject = computed(() => {
    if (projectId.value) return display(projectId.value);
    const jid = taskOutputs.value?.jobId as string | undefined;
    if (!jid) return undefined;
    const colonIdx = jid.indexOf(":");
    return colonIdx > 0 ? jid.slice(0, colonIdx) : undefined;
});

const resolvedLocation = computed(() => {
    if (location.value) return display(location.value);
    const jid = taskOutputs.value?.jobId as string | undefined;
    if (!jid) return undefined;
    const colonIdx = jid.indexOf(":");
    const dotIdx = jid.indexOf(".", colonIdx);
    if (colonIdx < 0 || dotIdx < 0) return undefined;
    return jid.slice(colonIdx + 1, dotIdx);
});

const resolvedSql = computed(() => display(sql.value));

// KsEditor needs an explicit "dark"/"light" theme. The host marks dark mode with a `dark` class on
// <html> (same signal the design-system's own components observe), so mirror it and react to toggles.
const isDark = ref(false);
const editorTheme = computed(() => (isDark.value ? "dark" : "light"));
let themeObserver: MutationObserver | null = null;
onMounted(() => {
    if (typeof document === "undefined") return;
    const detect = () => (isDark.value = document.documentElement.classList.contains("dark"));
    detect();
    themeObserver = new MutationObserver(detect);
    themeObserver.observe(document.documentElement, {
        attributes: true,
        attributeFilter: ["class"],
    });
});
onBeforeUnmount(() => themeObserver?.disconnect());

// Metrics (via the host-provided lazy fetcher — already scoped server-side to this task, so no
// client-side taskId filtering is needed anymore).
interface MetricEntry {
    name: string;
    value: number;
    taskId?: string;
}

const metrics = ref<MetricEntry[]>([]);

async function loadMetrics() {
    try {
        const resp = await props.fetchMetrics?.({ taskRunId: taskRun.value?.id });
        metrics.value = (resp?.results as MetricEntry[]) ?? [];
    } catch {
        /* best-effort */
    }
}

watch(
    executionId,
    (id) => {
        if (id) loadMetrics();
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

        <!-- Rendered SQL: full view (available pre-execution, straight from the flow definition) -->
        <section v-if="isFullView && resolvedSql" class="bq-section">
            <h4 class="bq-section__title">{{ t("query") }}</h4>
            <div class="bq-sql">
                <KsEditor
                    :model-value="resolvedSql"
                    lang="sql"
                    :theme="editorTheme"
                    read-only
                    :navbar="false"
                />
            </div>
        </section>

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

/* KsEditor auto-sizes to its content; cap the height so a long query scrolls instead of pushing
   the rest of the panel off-screen. */
.bq-sql {
    max-height: 20rem;
    overflow: auto;
    border: 1px solid var(--ks-border-primary);
    border-radius: var(--ks-border-radius);
}
</style>