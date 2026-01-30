# SimplePipelines.jl

*Minimal, type-stable DAG pipelines for Julia*

## Interface

```
┌───────────────────────────────────────────────────────────────────┐
│                       SimplePipelines.jl                          │
├───────────────────────────────────────────────────────────────────┤
│ STEPS                                                             │
│   @step name = `command`          Shell command step              │
│   @step name = () -> expr         Julia function step             │
│                                                                   │
│ OPERATORS                                                         │
│   a >> b                          Sequential: a then b            │
│   a & b                           Parallel: a and b together      │
│   a | b                           Fallback: b if a fails          │
│   a^n                             Retry: up to n attempts         │
│                                                                   │
│ CONTROL FLOW                                                      │
│   Timeout(a, seconds)             Fail if exceeds time limit      │
│   Branch(cond, a, b)              Conditional: a if true, else b  │
│   Map(f, items)                   Fan-out: parallel over items    │
│   Retry(a, n, delay=1.0)          Retry with delay between        │
│                                                                   │
│ EXECUTION                                                         │
│   run_pipeline(p)                 Run pipeline, return results    │
│   run_pipeline(p, verbose=false)  Run silently                    │
│   run_pipeline(p, dry_run=true)   Preview without executing       │
│                                                                   │
│ RESULTS                                                           │
│   results[i].success              Did step succeed?               │
│   results[i].duration             Execution time (seconds)        │
│   results[i].output               Captured output or error        │
│                                                                   │
│ UTILITIES                                                         │
│   print_dag(node)                 Visualize DAG structure         │
│   count_steps(node)               Count total steps               │
│   steps(node)                     Get all steps as vector         │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

## Overview

SimplePipelines.jl lets you define and execute directed acyclic graph (DAG) pipelines
using intuitive operators:

| Operator | Meaning | Example |
|----------|---------|---------|
| `>>` | Sequential (a then b) | `download >> process >> upload` |
| `&` | Parallel (a and b together) | `sample_a & sample_b & sample_c` |
| `\|` | Fallback (b if a fails) | `fast_method \| slow_method` |
| `^n` | Retry (up to n times) | `flaky_step^3` |

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

- **Intuitive operators** - `>>` sequence, `&` parallel, `|` fallback, `^` retry
- **Control flow** - `Timeout`, `Branch`, `Map` for complex workflows
- **Type-stable** - Zero runtime type checks, full compile-time specialization
- **Minimal overhead** - `@inline` functions and tuple recursion
- **Composable** - All operators work together seamlessly
- **Unified interface** - Shell commands and Julia functions compose seamlessly

## Contents

```@contents
Pages = ["tutorial.md", "examples.md", "api.md", "design.md", "development.md"]
Depth = 2
```
