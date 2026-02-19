# API Reference

## Module

```@autodocs
Modules = [SimplePipelines]
Order = [:module]
```

## Types

```@docs
SimplePipelines.AbstractNode
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
```

## Macros

```@docs
@step
@sh_str
@shell_raw_str
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
