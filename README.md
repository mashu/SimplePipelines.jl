![simplepipelines_logo](https://github.com/user-attachments/assets/f4a31cd2-294e-42ac-86be-ab4a6386c78a)

# SimplePipelines.jl

[![Docs](https://img.shields.io/badge/docs-dev-blue.svg)](https://mashu.github.io/SimplePipelines.jl/dev)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)

Minimal, type-stable DAG pipelines for Julia.

## Quick Start

```julia
using SimplePipelines

# Chain steps with >>
pipeline = `download data.csv` >> `process data.csv` >> `upload results.csv`

# Run in parallel with &
pipeline = (`task_a` & `task_b` & `task_c`) >> `merge`

# Mix shell and Julia
pipeline = @step fetch = `curl -o data.csv url` >>
           @step analyze = () -> sum(parse.(Int, readlines("data.csv")))

run_pipeline(pipeline)
```

## Interface

```
Operators:  a >> b      sequential       Retry:     a^3  or  Retry(a, 3)
            a & b       parallel         Fallback:  a | b
            
Control:    Branch(cond, a, b)           conditional
            Timeout(a, 30.0)             time limit
            Reduce(f, a & b)             combine outputs

Discovery:  ForEach("{sample}.fq") do s  auto-discover files
              `process $(s).fq`
            end
```

## Multi-file Processing

```julia
# Discover files, create parallel branches automatically
ForEach("data/{sample}_R1.fq.gz") do sample
    `pear -f $(sample)_R1.fq.gz -r $(sample)_R2.fq.gz` >> `process $(sample)`
end
```

## Documentation

[Full docs](https://mashu.github.io/SimplePipelines.jl/dev) · [Tutorial](https://mashu.github.io/SimplePipelines.jl/dev/tutorial/) · [Examples](https://mashu.github.io/SimplePipelines.jl/dev/examples/) · [API](https://mashu.github.io/SimplePipelines.jl/dev/api/)
