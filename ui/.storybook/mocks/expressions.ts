// Storybook fake for `@kestra-io/kestra-sdk/expressions`.
// Mirrors the backend `POST /expressions/render` contract: given a list of expression strings it
// returns a `{ rendered }` map keyed by the original string. Here we resolve a fixed set of
// vars/inputs by simple token substitution, which is enough to demonstrate resolved
// projectId/location values and a resolved SQL block fully offline.
const VALUES: Record<string, string> = {
    "vars.projectId": "my-gcp-project",
    "vars.location": "EU",
    "inputs.projectId": "my-gcp-project",
    "inputs.location": "EU",
};

export async function renderExpressions(body: { expressions?: string[] }) {
    const rendered: Record<string, string> = {};
    for (const expr of body.expressions ?? []) {
        rendered[expr] = expr.replace(
            /\{\{\s*([\w.]+)\s*}}/g,
            (match, key) => VALUES[key] ?? match,
        );
    }
    return { rendered };
}