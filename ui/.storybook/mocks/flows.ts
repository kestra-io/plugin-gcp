// Storybook fake for `@kestra-io/kestra-sdk/flows`. The component only uses this as a fallback
// source for task config; the stories pass a complete `task`, so an empty flow is enough to keep
// the best-effort fetch from hitting a (non-existent) backend.
export async function flow() {
    return { tasks: [] };
}