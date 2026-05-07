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

> **Commands** — `sh"cmd"` · `sh("$(var)")` (interpolation) · `sh(cmd_func)` (run-time) · `shell_raw"..."` / `shell_raw"""..."""` (shell variables like `$VAR` without Julia interpolation)

> **Operators** — `a >> b` sequence: next (function) step gets previous output; if left has many branches, only the **last** is passed. `a |> b` pipe: next step gets left's output(s); if many branches, **all** as one vector; RHS must be a function step. `a & b` parallel · `a | b` fallback · `a^3` retry · `a >>> b` same input · `a .>> b` per-branch

> **Control** — `Branch(cond,a,b)` · `Timeout(a,t)` · `Reduce(f,a&b)` · `ForEach(pat)` / `ForEach(items) do ...`

> **Freshness** — `Force(step)` · `run(p, force=true)` · `is_fresh(step)` · `clear_state!()`

> **Run** — `run(pipeline)` · `run(p, verbose=false)` (silent) · `run(p, verbose=true)` (default: prints each shell command) · `run(p, dry_run=true)`

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

Pipeline execution shows **colored**, tree-structured output in the terminal. Headers are blue, ▶ (running) and step names are cyan, ✓ (success) green, ✗ (failure) red, ⊕ (parallel) magenta, and shell commands in grey.

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
*(The above is a plain preview; run `run(pipeline)` in a terminal to see the actual colors.)*

Visualize pipeline structure (also colored in the terminal: ◆/○ cyan, ▸ blue, ⊕ magenta, ← green, → yellow):

```julia
display(pipeline)
# ▸ Sequence
#   ├─○ download
#   ├─⊕ Parallel
#   │   ├─○ parse
#   │   └─○ validate
#   └─○ save
```

## Streaming shell pipelines

Use `sh_pipe(cmd1, cmd2, …)` to fold several commands into one OS-level pipeline. Stdout flows through OS pipes between stages — nothing is buffered in Julia between steps. Only the final stage's stdout is captured. This is the right pattern for `samtools view huge.bam | awk … | sort` where the intermediate streams would otherwise OOM.

```julia
@step align(["foo.bam"] => ["filtered.txt"]) =
    sh_pipe(sh"samtools view foo.bam", sh"awk -F'\t' '\$5>30'", sh"sort > filtered.txt")
```

`sh_pipe` is recognised by `@step` so the call is evaluated eagerly (the `Step`'s `work` is an `AbstractCmd`, not a thunk). For Julia value interpolation, `sh"…"` and `sh(...)` interpolate via `Cmd` (no shell injection):

```julia
threshold = 30
sh_pipe(sh"samtools view foo.bam", sh("awk -F'\t' '\$5>$threshold'"))
```

Compare with `sh"a" >> sh"b"`, which materialises each step's stdout in a Julia `IOBuffer` before running the next — fine for small outputs, OOM for big ones.

## Multi-file Processing

```julia
# Discover files, create parallel branches automatically
ForEach("data/{sample}_R1.fq.gz") do sample
    sh("pear -f $(sample)_R1.fq.gz -r $(sample)_R2.fq.gz") >> sh("process $(sample)")
end
```

## Output-side Wildcard Inference (Snakemake-style)

Declare reusable rules with `{wildcard}` patterns, then ask the runtime to build a DAG that produces the targets you want:

```julia
align = @rule align("raw/{sample}.fq" => "out/{sample}.bam") =
    "bwa mem ref.fa {input} > {output}"
index = @rule index("out/{sample}.bam" => "out/{sample}.bam.bai") =
    "samtools index {input}"

# Targets: ask for the .bai files; resolve walks back through rules,
# instantiates concrete Steps, and dedupes shared deps automatically.
plan = resolve([align, index], ["out/A.bam.bai", "out/B.bam.bai"])
run(plan)
```

`{input}` / `{output}` / `{wildcard_name}` placeholders inside the work template are filled at resolve time. Rule work can also be a function: `(inputs, outputs, wildcards) -> Cmd | String | Function`. Targets that already exist on disk are skipped.

`expand` generates a target list from a template by Cartesian product:

```julia
expand("out/{s}.bam"; s=["A","B","C"])
# ["out/A.bam", "out/B.bam", "out/C.bam"]
expand("out/{s}.{ext}"; s=["A","B"], ext=["bam","bai"])
# ["out/A.bam", "out/A.bai", "out/B.bam", "out/B.bai"]
```

A `Workflow` is the top-level object that bundles rules and default targets:

```julia
wf = Workflow(name="rnaseq")
push!(wf,
    @rule align("raw/{s}.fq" => "out/{s}.bam") = "bwa mem ref.fa {input} > {output}",
    @rule index("out/{s}.bam" => "out/{s}.bam.bai") = "samtools index {input}")
push!(wf, expand("out/{s}.bam.bai"; s=["A","B","C"]))
run(wf)                              # uses default targets
run(wf, targets=["out/A.bam.bai"])   # override
```

`push!(wf, x)` dispatches on type: `Rule` items go to `wf.rules`, strings (or vectors of strings) go to `wf.targets`.

## Memory-aware Parallel Scheduling

The package treats **disk as effectively infinite, RAM as finite**, and `run` is memory-safe by default:

| Default | Value | Why |
|---|---|---|
| `jobs` | `min(Threads.nthreads(), 8)` | Concurrent fan-out can't exceed the number of OS threads, so the host can't be oversubscribed. |
| `memory_budget_mb` | 50% of total RAM | Steps wrapped in `with_resources(...; mem_mb=N)` charge the budget; nothing can collectively exceed half RAM. |

You can disable either cap (`jobs=0` / `memory_budget_mb=0`) but the defaults are tuned so a fresh user calling `run(plan)` won't freeze their box. For pipelines whose steps each load a multi-GB DataFrame, annotate with `with_resources` so the scheduler can serialise them:

```julia
heavy = with_resources(@step align = sh"bwa ..."; mem_mb=4_000)
plan  = ForEach("data/{s}.fq") do s
    with_resources(@step ali = sh"bwa $s.fq"; mem_mb=4_000)
end
run(plan)                              # uses 50%-of-RAM budget by default
run(plan; memory_budget_mb=12_000)     # explicit cap: at most 3 × 4 GB
```

For values too big to keep in RAM between steps, return a `FilePath`:

```julia
@step extract = inputs -> begin
    df = load_huge_table(inputs[1])
    p = tempname() * ".jls"
    serialize(p, summarize(df))
    FilePath(p)         # tiny path travels downstream; df is freed
end
```

A custom `materialize(::FilePath)` lets the consumer load only when needed.
