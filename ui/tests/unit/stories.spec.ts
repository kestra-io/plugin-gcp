import { describe, test, vi } from "vitest";
import { composeStories } from "@storybook/vue3";

// The interactive Storybook resolves expressions through the SDK mocks via .storybook/main.ts
// viteFinal aliases; those aliases don't apply under vitest, so reuse the same mock modules here at
// the import boundary. Single source of truth for both.
vi.mock("@kestra-io/kestra-sdk/expressions", () => import("../../.storybook/mocks/expressions"));
vi.mock("@kestra-io/kestra-sdk/metrics", () => import("../../.storybook/mocks/metrics"));
vi.mock("@kestra-io/kestra-sdk/flows", () => import("../../.storybook/mocks/flows"));
vi.mock("@kestra-io/kestra-sdk/executions", () => import("../../.storybook/mocks/executions"));
vi.mock("@kestra-io/kestra-sdk/outputs", () => import("../../.storybook/mocks/outputs"));

// Monaco (KsEditor) can't run in jsdom — it needs a real <canvas> 2D context. Stub just that
// component; the rest of the design system stays real. The assertions target the resolved summary
// rows and job details, not the editor, and the stub still surfaces the resolved SQL as text.
vi.mock("@kestra-io/design-system", async (importOriginal) => {
    const actual = await importOriginal<typeof import("@kestra-io/design-system")>();
    return {
        ...actual,
        KsEditor: {
            name: "KsEditor",
            props: ["modelValue"],
            template: '<pre class="ks-editor-stub">{{ modelValue }}</pre>',
        },
    };
});

import * as topologyStories from "../storybook/components/BigqueryQueryTopologyDetails.stories";

// composeStories turns each exported story into a portable, runnable component. Story.run() renders
// it and executes its `play` function (the resolved-value assertions) in jsdom — no browser needed.
const stories = composeStories(topologyStories);

describe("BigqueryQueryTopologyDetails stories", () => {
    for (const [storyName, Story] of Object.entries(stories)) {
        if (typeof Story.play !== "function") continue;
        test(storyName, async () => {
            await Story.run();
        });
    }
});