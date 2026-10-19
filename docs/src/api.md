# API reference

```@meta
CurrentModule = SimplePipelines
```

```@docs
SimplePipelines
```

## Steps

```@docs
Step
@step
@sh_str
sh
SimplePipelines.Shell
julia
with_resources
```

## Composition

The operators `>>`, `&`, `|` and `^` build these nodes; see
[Composing pipelines](guide/composition.md).

```@docs
AbstractNode
Sequence
Parallel
Retry
Fallback
Branch
@branch
Timeout
Force
ForEach
Pipeline
```

## Rules and workflows

```@docs
Rule
@rule
gather
Workflow
@workflow
expand
discover
SimplePipelines.Wildcards
plan
```

## Running

```@docs
run
check
RunResult
StepResult
StepFailure
```

## Inspection and state

```@docs
print_dag
steps
count_steps
is_fresh
clear_state!
```

## Index

```@index
```
