# DSL plans

Each `*.yaml` file in this directory defines a derivative plan that Pattern D
can run. The schema:

```yaml
name: <string, required, unique identifier>
version: <int, required, bump on semantic change>
description: <string, optional>
derivatives:
  - kind: <one of types.DerivativeKind>
    depends_on: [<other kinds, optional>]
    config:                # optional map[string]any
      sleep_ms: 3000
```

Validation rules (enforced both client-side at submit time AND inside the
workflow as defense-in-depth):

1. `name` is required.
2. `version` is a positive integer.
3. `derivatives` is non-empty.
4. Every `kind` must be a known `DerivativeKind` (see `internal/types/types.go`).
5. Every `depends_on` entry must reference a `kind` present in the same file.
6. The dependency graph must be a DAG (no cycles).

A malformed YAML never reaches the worker: the starter rejects it before
calling `ExecuteWorkflow`.

To add a new plan: drop a new YAML file in this directory, no code change
required. The worker picks it up the next time the starter is invoked with
`--plan-file`.
