# SimplePipelines.jl

*Minimal, type-stable DAG pipelines for Julia*

## Overview

SimplePipelines.jl lets you define and execute directed acyclic graph (DAG) pipelines
using intuitive operators.

### Steps

| Syntax | Description |
|--------|-------------|
| `@step name = sh"cmd"` | Shell command |
| `@step name = sh"cmd > file"` | Shell with redirection/pipes |
| `@step name = () -> expr` | Julia function |

### Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `>>` | Sequential (pass output to next) | `a >> b >> c` |
| `&` | Parallel | `a & b & c` |
| `\|` | Fallback | `a \| b` (b if a fails) |
| `^n` | Retry | `a^3` (up to 3 times) |
| `\|>` | Pipe (output → input) | `a \|> b` |
| `>>>` | Same input (both get same context) | `a >>> b` |
| `.>>` | Broadcast (attach to each branch) | `fe .>> step` |

When the left has **multiple** outputs (e.g. ForEach), `>>` passes only the **last** to the next step; `|>` passes a **vector** of all; `.>>` runs the next step **per branch**.

| Left side     | `>>`              | `|>`                 | `.>>`                    |
| ------------- | ------------------ | -------------------- | ------------------------ |
| Single output | step(one value)    | step(one value)      | step(one value)          |
| Multi output  | step(**last** only)| step(**vector** of all) | step **per branch** |

### Control Flow

| Function | Description |
|----------|-------------|
| `Timeout(a, secs)` | Fail if exceeds time |
| `Branch(cond, a, b)` | Conditional execution |
| `ForEach(items) do x ... end` | Fan-out over collection |
| `Reduce(f, a & b)` | Combine parallel outputs |
| `ForEach(pattern) do ...` | Discover files by pattern |
| `Retry(a, n, delay=d)` | Retry with delay |

### Execution & Results

| Function / Field | Description |
|------------------|-------------|
| `run(p)` / `p |> run` | Run, return results |
| `run(p, verbose=false)` | Run silently |
| `run(p, dry_run=true)` | Preview only |
| `results[i].success` | Did step succeed? |
| `results[i].duration` | Time in seconds |
| `results[i].output` | Output or error |

### Utilities

| Function | Description |
|----------|-------------|
| `print_dag(node)` | Visualize structure |
| `count_steps(node)` | Count steps |
| `steps(node)` | Get all steps |

## Quick Start

```julia
using SimplePipelines

# Simple sequence
pipeline = sh"echo 'step 1'" >> sh"echo 'step 2'" >> sh"echo 'step 3'"
run(pipeline)

# Parallel branches that merge
pipeline = (sh"process A" & sh"process B") >> sh"merge"
run(pipeline)
```

## DAG Patterns

### Diamond (fork-join)

```
       ┌── step_b ──┐
step_a─┤            ├── step_d
       └── step_c ──┘
```

```julia
step_a = @step a = sh"fetch"
step_b = @step b = sh"analyze_a"
step_c = @step c = sh"analyze_b"
step_d = @step d = sh"report"
pipeline = step_a >> (step_b & step_c) >> step_d
```

### Multi-stage parallel

```
       ┌─ b ─┐     ┌─ e ─┐
    a ─┤     ├─ d ─┤     ├─ g
       └─ c ─┘     └─ f ─┘
```

```julia
a = @step a = sh"stage_a"
b = @step b = sh"stage_b"
c = @step c = sh"stage_c"
d = @step d = sh"stage_d"
e = @step e = sh"stage_e"
f = @step f = sh"stage_f"
g = @step g = sh"stage_g"
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
a = @step a = sh"branch1_a"
b = @step b = sh"branch1_b"
c = @step c = sh"branch2_a"
d = @step d = sh"branch2_b"
e = @step e = sh"branch3_a"
f = @step f = sh"branch3_b"
merge = @step merge = sh"merge"
branch1 = a >> b
branch2 = c >> d
branch3 = e >> f
pipeline = (branch1 & branch2 & branch3) >> merge
```

## Features

- **Intuitive operators** - `>>` sequence, `&` parallel, `|` fallback, `^` retry
- **Control flow** - `Timeout`, `Branch`, `ForEach` (pattern or collection) for complex workflows
- **Type-stable** - Zero runtime type checks, full compile-time specialization
- **Minimal overhead** - `@inline` functions and tuple recursion
- **Composable** - All operators work together seamlessly
- **Unified interface** - Shell commands and Julia functions compose seamlessly

## Contents

```@contents
Pages = ["tutorial.md", "examples.md", "api.md", "design.md", "development.md"]
Depth = 2
```
