# API Reference

## Module

```@docs
SimplePipelines
```

## Types

```@docs
SimplePipelines.AbstractNode
Step
Sequence
Parallel
Retry
Fallback
Branch
Timeout
Reduce
Force
Pipeline
```

## Macros

```@docs
@step
@sh_str
```

## Operators

The package extends these operators for pipeline composition. `Cmd` and `Function` arguments are auto-wrapped in `Step`.

| Operator | Name     | Description                          |
| -------- | -------- | ------------------------------------ |
| `>>`     | Sequence | Run nodes in order                   |
| `&`      | Parallel | Run nodes concurrently               |
| `\|`     | Fallback | Run fallback if primary fails        |
| `^`      | Retry    | Wrap with retries, e.g. `node^3`     |

## Functions

```@docs
Map
ForEach
fe
```

## Shell

```@docs
sh
```

## Execution

Execution is recursive: `run(pipeline)` calls `run_node(root, ...)` which dispatches on node type and recurses (Sequence in order, Parallel/ForEach/Map with optional `@spawn`).

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
