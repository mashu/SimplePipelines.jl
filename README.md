![simplepipelines_logo](https://github.com/user-attachments/assets/f4a31cd2-294e-42ac-86be-ab4a6386c78a)

# SimplePipelines.jl

[![Docs](https://img.shields.io/badge/docs-dev-blue.svg)](https://mashu.github.io/SimplePipelines.jl/dev)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/mashu/SimplePipelines.jl/actions)

Minimal, type-stable DAG pipelines for Julia with Make-like incremental builds.

## Quick Start

```julia
using SimplePipelines

# Chain steps with >>
pipeline = sh"echo '1,2,3' > data.csv" >> sh"wc -l data.csv"

# Redirection and pipes
pipeline = sh"(echo a; echo b; echo a) > data.csv" >> sh"sort data.csv | uniq > sorted.csv"

# Run in parallel with &
pipeline = (sh"echo task_a" & sh"echo task_b" & sh"echo task_c") >> sh"echo merge"

# Mix shell and Julia
pipeline = @step fetch = sh"(echo 1; echo 2; echo 3) > data.csv" >>
           @step analyze = () -> sum(parse.(Int, readlines("data.csv")))

run(pipeline)
```

## Interface

> **Commands** — `sh"cmd"` · `sh("$(var)")` (interpolation)

> **Operators** — `a >> b` sequence (passes output to next) · `a & b` parallel · `a | b` fallback · `a^3` retry · `a |> b` pipe (output → input) · `a >>> b` same input · `a .>> b` attach step to each branch

> **Control** — `Branch(cond,a,b)` · `Timeout(a,t)` · `Reduce(f,a&b)` · `ForEach(pat)` / `ForEach(items) do ...`

> **Freshness** — `Force(step)` · `run(p, force=true)` · `is_fresh(step)` · `clear_state!()`

> **Run** — `run(pipeline)` · `run(p, verbose=false)` · `run(p, dry_run=true)`

## Make-like Incremental Builds

Steps are automatically skipped if their outputs are fresh:

```julia
# Define steps with input/output dependencies
download = @step download([] => ["data.csv"]) = sh"curl -o data.csv http://example.com/data"
process  = @step process(["data.csv"] => ["result.csv"]) = sh"sort data.csv > result.csv"

pipeline = download >> process
run(pipeline)  # First run: executes both steps
run(pipeline)  # Second run: skips both (outputs exist and are newer than inputs)
```

Force execution when needed:
```julia
run(pipeline, force=true)    # Force all steps
Force(process) >> cleanup    # Force specific step
```

## Colored Output

Pipeline execution shows colored, tree-structured output:

```
═══ Pipeline: ETL ═══
▶ Running: download
  ✓ Completed in 0.5s
⊕ Running 2 branches in parallel...
▶ Running: parse
▶ Running: validate  
  ✓ Completed in 0.1s
  ✓ Completed in 0.2s
═══ Completed: 3/3 steps in 0.8s ═══
```

Visualize pipeline structure:
```julia
display(pipeline)
# ▸ Sequence
#   ├─○ download
#   ├─⊕ Parallel
#   │   ├─○ parse
#   │   └─○ validate
#   └─○ save
```

## Multi-file Processing

```julia
# Discover files, create parallel branches automatically
ForEach("data/{sample}_R1.fq.gz") do sample
    sh("pear -f $(sample)_R1.fq.gz -r $(sample)_R2.fq.gz") >> sh("process $(sample)")
end
```
