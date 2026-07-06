import { defineConfig } from "vitest/config";
import vue from "@vitejs/plugin-vue";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";
import { playwright } from "@vitest/browser-playwright";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

// Mirrors the Kestra core UI test architecture (kestra ui/vitest.config.js): two Vitest projects.
//  - "unit"      — fast jsdom tests for composables/logic (see vitest.config.unit.ts)
//  - "storybook" — every story (and its `play` function) runs as a real test in headless Chromium
//                  via the Storybook Vitest addon. Because it drives the same Storybook config
//                  (.storybook/main.ts), the SDK mocks in .storybook/mocks apply here too, so the
//                  stories render *resolved* Pebble expressions with no backend.
// Docs: https://storybook.js.org/docs/writing-tests/integrations/vitest-addon
export default defineConfig({
    test: {
        projects: [
            "./vitest.config.unit.ts",
            {
                // `vue()` is required so Vue SFCs (including those in node_modules, e.g.
                // vue-material-design-icons used by the design system) are transformed in the
                // browser-test pipeline — the Storybook framework's own transform doesn't cover them.
                plugins: [vue(), storybookTest({ configDir: join(here, ".storybook") })],
                // In production the artifact-sdk shares vue/vue-i18n as Module-Federation
                // singletons; the browser-test pipeline has no MF, so force a single instance to
                // avoid a dual-copy mismatch (component's useI18n not seeing initApp's i18n).
                resolve: { dedupe: ["vue", "vue-i18n"] },
                // vue-i18n's esm-bundler build breaks when esbuild code-splits it during dep
                // pre-bundling ("init_shared_esm_bundler is not defined"). Keep it out of the
                // prebundle so Vite transforms it directly.
                optimizeDeps: { exclude: ["vue-i18n"] },
                // Signals preview.ts to skip initApp (no host context under this runner).
                define: { __SB_VITEST__: "true" },
                test: {
                    name: "storybook",
                    setupFiles: ["./.storybook/vitest.setup.ts"],
                    browser: {
                        enabled: true,
                        headless: true,
                        provider: playwright(),
                        instances: [{ browser: "chromium" }],
                    },
                },
            },
        ],
        coverage: {
            provider: "v8",
            reporter: ["text", "html"],
            include: ["src/**/*.{ts,vue}"],
            exclude: [
                "**/node_modules/**",
                "**/*.stories.*",
                "**/*.spec.{ts,tsx}",
                "**/*.d.ts",
                "**/.storybook/**",
            ],
        },
    },
});