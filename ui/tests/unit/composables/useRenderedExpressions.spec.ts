import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { defineComponent, h } from "vue";
import { mount, flushPromises } from "@vue/test-utils";

// Mock the SDK expression-render endpoint — these unit tests exercise the composable's wiring
// (filtering, tenant resolution, reactive fallback), not a real backend.
const { renderExpressionsMock } = vi.hoisted(() => ({ renderExpressionsMock: vi.fn() }));
vi.mock("@kestra-io/kestra-sdk/expressions", () => ({
    renderExpressions: renderExpressionsMock,
}));

import { useRenderedExpressions } from "../../../src/composables/useRenderedExpressions";

/** Mounts a throwaway component that runs the composable, returning its `display()`. */
function mountComposable(exprs: Array<string | undefined>, ctx: Record<string, unknown> = {}) {
    let display!: (v?: string) => string | undefined;
    const wrapper = mount(
        defineComponent({
            setup() {
                ({ display } = useRenderedExpressions(
                    () => exprs,
                    () => ctx,
                ));
                return () => h("div");
            },
        }),
    );
    return { display: (v?: string) => display(v), wrapper };
}

describe("useRenderedExpressions", () => {
    beforeEach(() => {
        renderExpressionsMock.mockReset();
    });

    afterEach(() => {
        // Reset URL and any injected csrf meta tags so tests don't leak into each other.
        window.history.replaceState(null, "", "/");
        document.querySelectorAll('meta[name="csrf-token"]').forEach((m) => m.remove());
    });

    it("returns the resolved value for an expression", async () => {
        renderExpressionsMock.mockResolvedValue({
            rendered: { "{{ vars.projectId }}": "my-gcp-project" },
        });

        const { display } = mountComposable(["{{ vars.projectId }}"]);
        await flushPromises();

        expect(display("{{ vars.projectId }}")).toBe("my-gcp-project");
    });

    it("only sends values that contain a Pebble expression", async () => {
        renderExpressionsMock.mockResolvedValue({ rendered: {} });

        mountComposable(["plain-value", "{{ vars.location }}"]);
        await flushPromises();

        expect(renderExpressionsMock).toHaveBeenCalledTimes(1);
        const [body] = renderExpressionsMock.mock.calls[0];
        expect(body.expressions).toEqual(["{{ vars.location }}"]);
    });

    it("does not call the backend when there is nothing to render", async () => {
        const { display } = mountComposable(["plain-value"]);
        await flushPromises();

        expect(renderExpressionsMock).not.toHaveBeenCalled();
        expect(display("plain-value")).toBe("plain-value");
    });

    it("falls back to the raw value when rendering fails", async () => {
        renderExpressionsMock.mockRejectedValue(new Error("boom"));

        const { display } = mountComposable(["{{ vars.projectId }}"]);
        await flushPromises();

        expect(display("{{ vars.projectId }}")).toBe("{{ vars.projectId }}");
    });

    it("does not set a per-call CSRF header (handled by the host's shared SDK client)", async () => {
        const meta = document.createElement("meta");
        meta.setAttribute("name", "csrf-token");
        meta.setAttribute("content", "tok-123");
        document.head.appendChild(meta);
        renderExpressionsMock.mockResolvedValue({ rendered: {} });

        mountComposable(["{{ vars.projectId }}"]);
        await flushPromises();

        const [, opts] = renderExpressionsMock.mock.calls[0];
        expect(opts?.headers).toBeUndefined();
    });

    it("resolves the tenant from the UI path and passes it explicitly", async () => {
        window.history.replaceState(null, "", "/ui/my-tenant/flows/company.team/flow-a");
        renderExpressionsMock.mockResolvedValue({ rendered: {} });

        mountComposable(["{{ vars.projectId }}"]);
        await flushPromises();

        const [body] = renderExpressionsMock.mock.calls[0];
        expect(body.tenant).toBe("my-tenant");
    });
});