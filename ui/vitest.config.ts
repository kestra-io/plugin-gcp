import { defineConfig } from "vitest/config";
import vue from "@vitejs/plugin-vue";

// Runs the Storybook story `play` functions headlessly in jsdom (portable stories via
// `composeStories`, see tests/unit/stories.spec.ts) plus plain composable unit tests, so CI catches
// UI regressions with no browser runner — matching the kestra-io/plugin-ai test setup. Deliberately
// does NOT extend the module-federation vite.config.ts; vitest prefers this file.
export default defineConfig({
    plugins: [vue()],
    test: {
        environment: "jsdom",
        include: ["tests/**/*.spec.ts"],
        setupFiles: ["tests/unit/setup.ts"],
        // The design-system dist imports .css; inline it so Vite transforms those imports instead of
        // Node trying to load raw .css files.
        server: { deps: { inline: [/@kestra-io\/design-system/] } },
    },
});