# Public API policy

This page clarifies what is considered **stable** for semver (minor releases may add features; patch releases are bugfixes only) versus **internal** details that may change without notice.

## Stable (documented + exported)

Treat the following as the supported surface:

- Everything listed under `export` in the package module (see the [API](../api.md) page and `src/SimplePipelines.jl`).
- Documented keyword arguments of [`run`](@ref) (including `report`, resource and spill options).
- `StepFailure` `kind` symbols documented for user-visible outcomes (e.g. `:process_failed`, `:exception`, `:timed_out`, `:inner_exception`, `:no_matches`, …).

The persisted state file uses a fixed binary layout (magic `SPstate\\0`); the on-disk format is considered stable within the same major version unless called out in the changelog.

## Extension hooks

- [`materialize`](@ref) / [`materialize_table`](@ref): specialise `materialize` on your own types in user code; `materialize_table` for [`FilePath`](@ref) is provided when the **Table I/O** package extension loads (`using CSV, DataFrames` after SimplePipelines).

## Internal (do not rely on)

- Submodules such as `StateFormat` (implementation detail for persistence).
- Function names not exported (e.g. layout helpers, internal `run_node` helpers).
- Exact log text or styling from verbose mode.

If you need a symbol that is not exported, open an issue: it may be a candidate for promotion to the public API.
