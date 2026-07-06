import { defineProject } from "vitest/config";
import vue from "@vitejs/plugin-vue";

// jsdom unit project — mirrors kestra ui/vitest.config.unit.js. Fast, no browser; for testing
// composables and component logic in isolation.
export default defineProject({
    plugins: [vue()],
    test: {
        name: "unit",
        environment: "jsdom",
        setupFiles: ["./tests/unit/setup.ts"],
        include: ["tests/unit/**/*.spec.{ts,tsx}"],
        reporters: [["default"], ["junit"]],
        outputFile: {
            junit: "./test-report.junit.xml",
        },
    },
});