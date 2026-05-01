# API Reference

## Module

```@autodocs
Modules = [SimplePipelines]
Order = [:module]
```

## Types

```@docs
AbstractNode
SimplePipelines.AbstractStepResult
StepResult
Step
ShRun
Sequence
Parallel
Retry
Fallback
Branch
Timeout
Reduce
Force
Pipeline
SimplePipelines.Pipe
SameInputPipe
BroadcastPipe
Resources
Resourced
FilePath
Rule
NoWork
```

## Macros

```@docs
@step
@rule
@sh_str
@shell_raw_str
```

## Resource-aware execution

Annotate heavy nodes with [`with_resources`](@ref) and pass `memory_budget_mb` /
`thread_budget` to `run` so the parallel scheduler keeps total concurrent
resource use under a soft cap. Wrap heavy values in [`FilePath`](@ref) and use
[`materialize`](@ref) at the consumer to keep memory bounded between steps.

```@docs
with_resources
materialize
```

## Output-side wildcard inference

Declare reusable [`Rule`](@ref)s with `{wildcard}` patterns, then call
[`resolve`](@ref) to walk a target list backward through the rule set and build
a runnable DAG. Rule work supports both shell templates and `(inputs, outputs,
wildcards) -> Cmd | String | Function`.

[`expand`](@ref) generates concrete target lists from a template by Cartesian
product, and a [`Workflow`](@ref) bundles rules + default targets behind a single
`run(::Workflow)` entry point.

```@docs
resolve
expand
Workflow
plan
```

## Operators

The package extends these operators for pipeline composition. `Cmd` and `Function` arguments are auto-wrapped in `Step`.

| Operator | Name          | Description                                              |
| -------- | ------------- | -------------------------------------------------------- |
| `>>`     | Sequence      | Run in order; pass previous output to next (function step)|
| `&`      | Parallel      | Run nodes concurrently                                   |
| `\|`     | Fallback      | Run fallback if primary fails                            |
| `^`      | Retry         | Wrap with retries, e.g. `node^3`                         |
| `\|>`    | Pipe          | Run right with left's output(s) (single or vector)       |
| `>>>`    | SameInputPipe | Run both with the same input (e.g. branch id)            |
| `.>>`    | BroadcastPipe | Attach right to each branch of left (per-branch pipeline)|

When the left has one output, `>>`, `|>`, and `.>>` all pass that value to the next step. When the left has **multiple** outputs (ForEach, Parallel):

| Left side     | ``>>``               | Pipe                  | ``.>>``                    |
|:-------------|:---------------------|:----------------------|:----------------------------|
| Single output | step(one value)      | step(one value)       | step(one value)             |
| Multi output  | step(**last** only)  | step(**vector** of all) | step **per branch** (one call each) |

## Functions

```@docs
ForEach
fe
```

## Shell

```@docs
sh
```

The string macro `shell_raw"..."` (and triple-quoted `shell_raw\"\"\"...\"\"\"`) is documented in [`@shell_raw_str`](@ref); use it for scripts where the dollar sign must not be interpreted by Julia.

## Execution

Execution is recursive: `run(pipeline)` calls `run_node(root, ...)` which dispatches on node type and recurses (Sequence in order, Parallel/ForEach with optional `@spawn`).

```@docs
run
```

## Freshness and state

State is stored in `.pipeline_state` as a fixed-layout, memory-mapped file. Completions are batched and written when `run()` finishes.

```@docs
is_fresh
clear_state!
```

### State file format

The state file uses a fixed binary layout (see `src/StateFormat.jl`) for random access and mmap.

```@docs
SimplePipelines.StateFormat
SimplePipelines.StateFormat.StateFileLayout
SimplePipelines.StateFormat.state_init
SimplePipelines.StateFormat.state_read
SimplePipelines.StateFormat.state_write
SimplePipelines.StateFormat.state_append
```

## Utilities

```@docs
count_steps
steps
print_dag
```

## Index

```@index
```
