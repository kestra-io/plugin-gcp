import type { Preview } from "@storybook/vue3";
import { setup } from "@storybook/vue3";
import { initApp } from "@kestra-io/artifact-sdk";

// `__SB_VITEST__` is defined only by the Storybook Vitest project (see vitest.config.ts).
declare const __SB_VITEST__: boolean | undefined;

setup((app) => {
    // Under the Vitest browser runner there is no host/Module-Federation context, so initApp (which
    // wires host singletons, i18n, an app errorHandler, …) can't complete and would leave the app
    // in a broken state. Skip it there — stories install the app context they need via decorators.
    if (typeof __SB_VITEST__ !== "undefined" && __SB_VITEST__) return;
    initApp(app);
});

const preview: Preview = {
    parameters: {
        controls: {
            matchers: {
                color: /(background|color)$/i,
                date: /Date$/i,
            },
        },
    },
};

export default preview;