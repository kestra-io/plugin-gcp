import type { StorybookConfig } from "@storybook/vue3-vite";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const mock = (file: string) => resolve(here, "mocks", file);

// The component only reaches the Kestra backend through the generated SDK for expression rendering
// (host-side rendering hasn't landed yet — kestra-ee#10488); task, outputs and metrics all come from
// host-provided story args/props now. Storybook has no backend, so alias that one SDK entry point to a
// local fake that returns deterministic data — this is what lets the stories show *resolved* Pebble
// expressions fully offline. Storybook-only: the production build (vite.config.ts) never sees this alias.
const sdkMocks = [
    { find: /^@kestra-io\/kestra-sdk\/expressions$/, replacement: mock("expressions.ts") },
];

const config: StorybookConfig = {
    stories: ["../tests/**/*.stories.@(js|jsx|mjs|ts|tsx)"],
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