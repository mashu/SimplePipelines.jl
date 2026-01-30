![simplepipelines_logo](https://github.com/user-attachments/assets/f4a31cd2-294e-42ac-86be-ab4a6386c78a)

# SimplePipelines.jl

[![Docs](https://img.shields.io/badge/docs-dev-blue.svg)](https://mashu.github.io/SimplePipelines.jl/dev)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)

Minimal, type-stable DAG pipelines for Julia. Build workflows with shell commands and Julia functions using intuitive operators.

## Installation

```julia
using Pkg
Pkg.add("SimplePipelines")
```

## Interface Overview

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

## Documentation

See the [full documentation](https://mashu.github.io/SimplePipelines.jl/dev) for tutorials, examples, and API reference.
