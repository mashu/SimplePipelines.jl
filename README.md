![simplepipelines_logo](https://github.com/user-attachments/assets/f4a31cd2-294e-42ac-86be-ab4a6386c78a)

# SimplePipelines.jl

[![Docs](https://img.shields.io/badge/docs-dev-blue.svg)](https://mashu.github.io/SimplePipelines.jl/dev)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Minimal, type-stable DAG pipelines for Julia.

## Quick Start

```julia
using SimplePipelines

# Chain steps with >>
pipeline = sh"curl -o data.csv url" >> sh"wc -l data.csv"

# Redirection and pipes
pipeline = sh"curl -o data.csv url" >> sh"sort data.csv | uniq > sorted.csv"

# Run in parallel with &
pipeline = (sh"task_a" & sh"task_b" & sh"task_c") >> sh"merge"

# Mix shell and Julia
pipeline = @step fetch = sh"curl -o data.csv url" >>
           @step analyze = () -> sum(parse.(Int, readlines("data.csv")))

run_pipeline(pipeline)
```

## Interface

```
Commands:   sh"cmd"     shell (redirection, pipes)   sh("$(var)")  with interpolation

Operators:  a >> b      sequential       a^3             retry 3x
            a & b       parallel         a | b           fallback
            
Control:    Branch(cond, a, b)           Timeout(a, 30.0)
            Reduce(f, a & b)             ForEach(pattern) do ...
```

## Multi-file Processing

```julia
# Discover files, create parallel branches automatically
ForEach("data/{sample}_R1.fq.gz") do sample
    sh("pear -f $(sample)_R1.fq.gz -r $(sample)_R2.fq.gz") >> sh("process $(sample)")
end
```
