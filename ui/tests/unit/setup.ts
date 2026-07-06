import { vi } from "vitest";
import { config } from "@vue/test-utils";

// Mirrors kestra ui/tests/unit/setup.ts (trimmed to what this plugin needs).

// Components mounted in isolation without vue-router can't resolve a literal <router-link>, which
// spams "[Vue warn]: Failed to resolve component: router-link". Register a minimal global stub.
config.global.stubs = {
    ...config.global.stubs,
    RouterLink: {
        name: "RouterLink",
        props: ["to"],
        template: "<a><slot /></a>",
    },
};

// Tests build their own createI18n() with only the messages they assert on, so unrelated keys used
// by mounted children are legitimately absent. Default missing/fallback warnings off so that
// expected gap doesn't spam "[intlify] Not found '<key>' key ..."; tests can still override.
vi.mock("vue-i18n", async (importOriginal) => {
    const actual = await importOriginal<typeof import("vue-i18n")>();
    return {
        ...actual,
        createI18n: (options: Record<string, unknown> = {}) =>
            actual.createI18n({ missingWarn: false, fallbackWarn: false, ...options }),
    };
});

// jsdom polyfills for Monaco editor (KsEditor), so component-level unit tests can mount it.
if (typeof document !== "undefined" && typeof document.queryCommandSupported !== "function") {
    (document as unknown as { queryCommandSupported: () => boolean }).queryCommandSupported = () =>
        false;
}
if (typeof document !== "undefined" && typeof document.execCommand !== "function") {
    (document as unknown as { execCommand: () => boolean }).execCommand = () => false;
}
if (typeof window !== "undefined" && typeof window.matchMedia !== "function") {
    (window as unknown as { matchMedia: (q: string) => object }).matchMedia = (query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: () => {},
        removeListener: () => {},
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false,
    });
}