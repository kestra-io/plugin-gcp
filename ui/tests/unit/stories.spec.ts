import { describe, test, vi } from "vitest";
import { composeStories } from "@storybook/vue3";

// Monaco (KsEditor) can't run in jsdom — it needs a real <canvas> 2D context. Stub just that
// component; the rest of the design system stays real. The assertions target the summary rows and
// job details, not the editor, and the stub still surfaces the raw SQL as text.
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