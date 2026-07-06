import type { StorybookConfig } from "@storybook/vue3-vite";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const mock = (file: string) => resolve(here, "mocks", file);

// The component reaches the Kestra backend through the generated SDK (expression rendering, metrics,
// task outputs, …). Storybook has no backend, so alias those SDK entry points to local fakes that
// return deterministic data — this is what lets the stories show *resolved* Pebble expressions and
// populated job metrics fully offline. Storybook-only: the production build (vite.config.ts) never
// sees these aliases.
const sdkMocks = [
    { find: /^@kestra-io\/kestra-sdk\/expressions$/, replacement: mock("expressions.ts") },
    { find: /^@kestra-io\/kestra-sdk\/metrics$/, replacement: mock("metrics.ts") },
    { find: /^@kestra-io\/kestra-sdk\/flows$/, replacement: mock("flows.ts") },
    { find: /^@kestra-io\/kestra-sdk\/executions$/, replacement: mock("executions.ts") },
    { find: /^@kestra-io\/kestra-sdk\/outputs$/, replacement: mock("outputs.ts") },
];

const config: StorybookConfig = {
    stories: ["../src/**/*.stories.@(js|jsx|mjs|ts|tsx)"],
    addons: ["@storybook/addon-themes", "@storybook/addon-docs"],
    framework: {
        name: "@storybook/vue3-vite",
        options: {},
    },
    viteFinal(cfg) {
        // The incoming alias config may be an object or an array depending on the base config;
        // normalize to the array form so we can prepend ours without clobbering existing entries.
        const existing = cfg.resolve?.alias;
        const asArray = Array.isArray(existing)
            ? existing
            : Object.entries(existing ?? {}).map(([find, replacement]) => ({
                  find,
                  replacement: replacement as string,
              }));
        cfg.resolve = { ...cfg.resolve, alias: [...sdkMocks, ...asArray] };
        return cfg;
    },
};

export default config;