# Running and inspecting

After a DAG looks right, `run` executes it and returns a vector of results. The
printed output is for humans; the returned values are for code.

## Run A Pipeline

```julia
results = run(pipeline)
```

By default, shell commands are printed before they run. Turn that off when the
pipeline is already familiar:

```julia
results = run(pipeline; verbose=false)
```

To inspect structure without executing work, use `print_dag` or `dry_run`:

```julia
print_dag(pipeline)
run(pipeline; dry_run=true)
```

If you want a name in logs, wrap the DAG in a `Pipeline`:

```julia
p = Pipeline(pipeline; name="qc")
run(p)
```

## Read Results

Each entry is a [`StepResult`](@ref). It tells you whether the step succeeded,
how long it took, which files were declared, and what value the step returned.

```julia
results = run(pipeline; verbose=false)

if all(r -> r.success, results)
    println("all steps completed")
else
    failed = filter(r -> !r.success, results)
    println("failed steps: ", join((r.step.name for r in failed), ", "))
end
```

The final result is often enough for simple pipelines:

```julia
last(results).result
```

## Report After A Run

Use a `report` callback when another system needs a summary. It runs after the
DAG finishes and before state is saved.

```julia
run(pipeline; report=(res; pipeline) -> begin
    @info "pipeline finished" pipeline steps=length(res)
end)
```

## Memory Safety

SimplePipelines assumes disk is cheaper than RAM, and that unbounded parallelism
can make a laptop unusable. A default `run(pipeline)` therefore has three safety
limits:

- `jobs=default_jobs()` runs at most `min(Threads.nthreads(), 8)` parallel
  branches at once.
- `auto_spill=true` spills large captured results to disk instead of keeping
  every result in RAM.
- `memory_budget_mb=default_memory_budget_mb()` and
  `thread_budget=default_thread_budget()` limit nodes annotated with
  [`with_resources`](@ref).

Most users do not need to configure these for small pipelines.

The resource budgets apply when a step declares what it needs:

```julia
heavy = with_resources(step; mem_mb=4_000, threads=4)
run(heavy & other)
```

A node that declares both must fit both budgets before it starts. The caps are
composed: memory pressure can queue the node, CPU-thread pressure can queue the
node, and either one is enough to delay it until resources are free.

Unannotated steps are still protected by the `jobs` branch cap, but the runtime
cannot guess how much RAM or how many CPU threads an external tool will use. Add
`with_resources` around known-heavy nodes so they queue instead of running all at
once.

## Spilled Results

When you do need to read a spilled value, call [`materialize`](@ref):

```julia
value = materialize(last(results).result)
```

For big data, the most predictable pattern is to write files yourself and return
[`FilePath`](@ref). Then downstream steps can load or stream that path as needed.

```julia
write_table = @step write_table = () -> begin
    # write result.csv here
    FilePath("result.csv")
end
```

To make CSV loading convenient, load CSV/DataFrames and use
[`materialize_table`](@ref):

```julia
using CSV, DataFrames
df = materialize_table(FilePath("result.csv"))
```

You can tighten or disable spilling for special cases:

```julia
run(pipeline; spill_threshold_bytes=1_000_000)
run(pipeline; auto_spill=false)
```

## Useful Inspection Helpers

```julia
count_steps(pipeline)
steps(pipeline)
print_dag(pipeline)
```

Use these while developing a DAG; use `check` and `explain` for rule-based
workflows.

## What To Remember

Run returns data; printed logs are only a view. Keep big values on disk when you
can, inspect the DAG before long runs, and read `StepResult` when automating.

**Next:** [Rules and diagnostics](rules-and-diagnostics.md) shows how to scale
from one explicit DAG to many pattern-generated targets.

For compact syntax lookup, use the [quick reference](../reference/quickref.md).
