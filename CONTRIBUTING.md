# Contributing

Install Node.js 20+ and run:

```sh
npm install
npm run lint
npm run typecheck
npm test
npm run build
npm run pack:check
```

Changes must include tests for behavior changes and a Changesets entry for publishable
changes. Integration tests must use disposable Redis credentials or a local service.
