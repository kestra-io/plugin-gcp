# Plugin GCP — UI

Vue 3 + Vite micro-frontend for the `io.kestra.plugin.gcp` plugin.

## Development

```bash
npm install
npm run dev        # start Vite dev server
npm run build      # production build
npm run storybook  # component workbench on :6006
npm run lint       # oxlint
npm run format     # oxfmt --check
```

## Local linking of `@kestra-io/kestra-sdk` with yalc

During development you sometimes need to test against an unreleased build of
`@kestra-io/kestra-sdk` (for example, SDK changes that haven't landed on
`develop` yet). We use [yalc](https://github.com/wclr/yalc) for this rather than
`npm link`, because yalc copies the package into the consumer and avoids the
symlink/duplicate-dependency issues that break Vite and module federation.

In the `kestra-sdk` repo:

```bash
npx yalc publish        # publish the current build to the local yalc store
```

In this `ui/` directory:

```bash
npx yalc add @kestra-io/kestra-sdk   # link the local build (writes .yalc/ + yalc.lock)
npm install
```

After changing the SDK, re-publish and push the update:

```bash
# in kestra-sdk
npx yalc push           # publish + update all linked consumers
```

The `.yalc/` directory and `yalc.lock` are gitignored — they must never be
committed.

### Removing the link

Once the SDK change is merged and released, pin the real version in
`package.json` and drop the local link:

```bash
npx yalc remove @kestra-io/kestra-sdk
npm install
```

> The `@kestra-io/kestra-sdk` dependency in `package.json` should always point
> at a published (merged) version before committing — a yalc link is a local-only
> development aid, never a checked-in state.