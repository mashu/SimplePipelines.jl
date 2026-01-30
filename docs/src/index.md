# SimplePipelines.jl

*Minimal, type-stable DAG pipelines for Julia*

## Interface

```
┌─────────────────────────────────────────────────────────────┐
│                    SimplePipelines.jl                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  @step name = `command`       Create a shell step           │
│  @step name = () -> expr      Create a Julia step           │
│                                                             │
│  a >> b                       Sequential: a then b          │
│  a & b                        Parallel: a and b together    │
│  a | b                        Fallback: b runs if a fails   │
│                                                             │
│  Retry(node, n)               Retry up to n times           │
│  Branch(cond, a, b)           Conditional: a if true, b if  │
│                                                             │
│  run_pipeline(pipeline)       Execute the pipeline          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Overview

SimplePipelines.jl lets you define and execute directed acyclic graph (DAG) pipelines
using two operators:

| Operator | Meaning | Example |
|----------|---------|---------|
| `>>` | Sequential (a then b) | `download >> process >> upload` |
| `&` | Parallel (a and b together) | `sample_a & sample_b & sample_c` |

## Quick Start

```julia
using SimplePipelines

# Simple sequence
pipeline = `echo "step 1"` >> `echo "step 2"` >> `echo "step 3"`
run_pipeline(pipeline)

# Parallel branches that merge
pipeline = (`process A` & `process B`) >> `merge`
run_pipeline(pipeline)
```

## DAG Patterns

### Diamond (fork-join)

```
       ┌── step_b ──┐
step_a─┤            ├── step_d
       └── step_c ──┘
```

```julia
pipeline = step_a >> (step_b & step_c) >> step_d
```

### Multi-stage parallel

```
       ┌─ b ─┐     ┌─ e ─┐
    a ─┤     ├─ d ─┤     ├─ g
       └─ c ─┘     └─ f ─┘
```

```julia
pipeline = a >> (b & c) >> d >> (e & f) >> g
```

### Independent branches merging

```
    ┌─ a ── b ─┐
    │          │
    ├─ c ── d ─┼── merge
    │          │
    └─ e ── f ─┘
```

```julia
branch1 = a >> b
branch2 = c >> d
branch3 = e >> f

pipeline = (branch1 & branch2 & branch3) >> merge
```

## Features

- **Two operators** - `>>` for sequence, `&` for parallel
- **Type-stable** - Zero runtime type checks, full compile-time specialization
- **Minimal overhead** - `@inline` functions and tuple recursion
- **Unified interface** - Shell commands and Julia functions compose seamlessly

## Contents

```@contents
Pages = ["tutorial.md", "examples.md", "api.md", "design.md", "development.md"]
Depth = 2
```
