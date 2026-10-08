# Releasing to npm

## One-time initial publication

The package is intentionally published once by an authorized maintainer before npm
can attach a trusted publisher to it. From this package directory, after reviewing the
dry-run output:

```sh
npm whoami
npm run pack:check
npm publish --access public --provenance=false
```

The one-time local publication explicitly disables provenance because a local shell has
no OIDC provider. The package still declares provenance by default, and the GitHub
workflow will generate provenance through npm trusted publishing. Do not add an npm
token to the repository or GitHub secrets for that workflow.

## Configure npm trusted publishing

After `@mhbdev/redis` exists on npm, open the package's npm settings and add a GitHub
Actions trusted publisher with:

- GitHub organization/user: `mhbdev`
- Repository: `redis`
- Workflow filename: `.github/workflows/release.yml`
- Environment: `npm-release`

The GitHub repository must also have the `npm-release` environment available. The
release job already requests only the permissions required for Changesets and npm
provenance: repository contents, pull requests, and the OIDC `id-token`. It does not
provide `NODE_AUTH_TOKEN`, so npm uses trusted publishing rather than a long-lived
automation token.

## Subsequent releases

1. Add a Changeset describing the public change.
2. Merge the Changesets version pull request created by `release.yml`.
3. The next push to `main` runs the release gate and publishes the version through npm
   trusted publishing.

The workflow validates linting, types, tests, build output, declaration packaging,
`publint`, `arethetypeswrong`, tarball imports, and npm audit before Changesets is
allowed to version or publish the package.
