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
```

## Shell

```@docs
sh
```

## Execution

```@docs
run
```

## Freshness and state

```@docs
is_fresh
clear_state!
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
