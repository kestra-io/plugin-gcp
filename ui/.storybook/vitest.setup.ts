// Storybook Vitest project setup — mirrors kestra ui/.storybook/vitest.setup.js.
// Story templates are runtime-compiled by Vue in the browser, which triggers a false-positive
// "@vue/compiler-core: decodeEntities option is passed but will be ignored in non-browser builds"
// warning from the esm-bundler build (it sets __BROWSER__=false even in browser environments).
// Suppress it so test output stays clean. The Storybook Vitest addon applies .storybook/preview.ts
// (which calls initApp) to every story automatically, so nothing else is needed here.
const origWarn = console.warn.bind(console);
console.warn = (...args: unknown[]) => {
    if (typeof args[0] === "string" && args[0].includes("decodeEntities")) return;
    origWarn(...args);
};