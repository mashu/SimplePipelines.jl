# SimplePipelines.jl

[![Docs](https://img.shields.io/badge/docs-stable-blue.svg)](https://mashu.github.io/SimplePipelines.jl/stable)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)

Minimal, type-stable DAG pipelines for Julia.

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
│                                                             │
│  run_pipeline(pipeline)       Execute the pipeline          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Quick Example

```julia
using SimplePipelines

# Define steps
download = @step download = `curl -o data.txt https://example.com/data`
process_a = @step process_a = `tool_a data.txt`
process_b = @step process_b = `tool_b data.txt`
merge = @step merge = () -> combine_results()

# Build DAG: download -> (process_a & process_b) -> merge
#
#            ┌─ process_a ─┐
#  download ─┤             ├─ merge
#            └─ process_b ─┘

pipeline = download >> (process_a & process_b) >> merge
run_pipeline(pipeline)
```

## Complex Branching

For graphs with multiple convergence points, compose sub-pipelines:

```julia
#     ┌─ b ─┐     ┌─ e ─┐
#  a ─┤     ├─ d ─┤     ├─ g
#     └─ c ─┘     └─ f ─┘

# Build in stages
stage1 = a
stage2 = b & c
stage3 = d
stage4 = e & f
stage5 = g

pipeline = stage1 >> stage2 >> stage3 >> stage4 >> stage5
```

## Installation

```julia
using Pkg
Pkg.add("SimplePipelines")
```

## Documentation

See the [documentation](https://mashu.github.io/SimplePipelines.jl/stable) for the full tutorial and API reference.
