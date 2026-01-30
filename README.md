![simplepipelines_logo](https://github.com/user-attachments/assets/f4a31cd2-294e-42ac-86be-ab4a6386c78a)

# SimplePipelines.jl

[![Docs](https://img.shields.io/badge/docs-dev-blue.svg)](https://mashu.github.io/SimplePipelines.jl/dev)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)

Minimal, type-stable DAG pipelines for Julia.

## Installation

```julia
using Pkg
Pkg.add("SimplePipelines")
```

## Interface

### Steps

| Syntax | Description |
|--------|-------------|
| `@step name = \`cmd\`` | Shell command |
| `@step name = () -> expr` | Julia function |

### Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `>>` | Sequential | `a >> b >> c` |
| `&` | Parallel | `a & b & c` |
| `\|` | Fallback | `a \| b` (b if a fails) |
| `^n` | Retry | `a^3` (up to 3 times) |

### Control Flow

| Function | Description |
|----------|-------------|
| `Timeout(a, secs)` | Fail if exceeds time |
| `Branch(cond, a, b)` | Conditional execution |
| `Map(f, items)` | Fan-out over collection |
| `Reduce(f, a & b)` | Combine parallel outputs |
| `Retry(a, n, delay=d)` | Retry with delay |

### Execution

| Function | Description |
|----------|-------------|
| `run_pipeline(p)` | Run, return results |
| `run_pipeline(p, verbose=false)` | Run silently |
| `run_pipeline(p, dry_run=true)` | Preview only |

### Results

| Field | Description |
|-------|-------------|
| `results[i].success` | Did step succeed? |
| `results[i].duration` | Time in seconds |
| `results[i].output` | Output or error |

### Utilities

| Function | Description |
|----------|-------------|
| `print_dag(node)` | Visualize structure |
| `count_steps(node)` | Count steps |
| `steps(node)` | Get all steps |

## Documentation

See the [full documentation](https://mashu.github.io/SimplePipelines.jl/dev) for tutorials, examples, and API reference.
