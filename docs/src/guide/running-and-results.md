# Running and inspecting

## Running pipelines

Use `run(pipeline)` or `pipeline |> run`:

```julia
results = run(pipeline)

# Default: verbose=true prints each shell command before it runs
results = run(pipeline)

results = run(pipeline, verbose=false)

run(pipeline, dry_run=true)

step_a = @step a = sh"first"
step_b = @step b = sh"second"
p = Pipeline(step_a >> step_b, name="My Workflow")
run(p)
```

## Checking results

```julia
results = run(pipeline)

for r in results
    if r.success
        println("$(r.step.name): completed in $(r.duration)s")
    else
        println("$(r.step.name): FAILED - $(r.result)")
    end
end

all_ok = all(r -> r.success, results)
```

Relevant fields include `.success`, `.duration`, `.result`, `.inputs`, and `.outputs`.

## Memory safety: auto-spill

The package treats **disk as effectively infinite, RAM as finite**. Three defaults make a default-run pipeline memory-safe by construction:

1. `jobs = min(Threads.nthreads(), 8)` — concurrent fan-out can't oversubscribe the host.
2. `auto_spill = true` — keeps both shell *and* function step results out of RAM:
   - **Shell steps** stream stdout straight to a tempfile in `spill_dir` while the command runs, so peak RAM is the OS pipe buffer (≈ 64 KB) rather than the full output. After the process exits, outputs below `spill_threshold_bytes` (10 MB by default) are read back as a `String` and the tempfile is deleted; outputs above the threshold stay on disk and `r.result` becomes a [`SpilledStdout`](@ref).
   - **Function steps** are checked after they return: if `Base.summarysize(r.result) > spill_threshold_bytes`, the value is serialised to a tempfile and `r.result` is replaced with a [`SpilledValue`](@ref). Small results stay in RAM (no I/O cost).
3. `memory_budget_mb = 50% of total RAM` — caps the *concurrent* memory of nodes wrapped in [`with_resources`](@ref).

Downstream consumers call [`materialize`](@ref) to load a `SpilledValue` (round-trips via `Base.Serialization`), a `SpilledStdout` (read as `String`), or a [`FilePath`](@ref) (raw bytes by default; users specialise for typed loading like CSV → DataFrame).

```julia
# Tight RAM:
run(plan; spill_threshold_bytes=1_000_000)        # 1 MB

# Pure in-memory (small data, benchmarks):
run(plan; auto_spill=false)

# Self-cleaning spill dir:
mktempdir() do dir
    results = run(plan; spill_dir=dir)
    df = materialize(last(results).result)         # spilled → DataFrame
    # …work with df…
end                                                # spill files gone here
```

A step that already returns a `FilePath` is left alone — the runtime only spills *unwrapped* big values.

## Mixing shell and Julia

Shell commands and Julia functions compose seamlessly:

```julia
prep = @step prep = () -> begin
    raw = read("raw.csv", String)
    cleaned = filter(line -> !isempty(strip(line)), split(raw, '\n'))
    write("clean.csv", join(cleaned, '\n'))
    return "Wrote $(length(cleaned)) lines"
end

external = @step tool = sh"wc -l clean.csv > result.txt"

post = @step post = () -> begin
    n = parse(Int, split(read("result.txt", String))[1])
    return "Line count: $n"
end

pipeline = prep >> external >> post
run(pipeline)
```

## Utilities

```julia
n = count_steps(pipeline)
all_steps = steps(pipeline)
print_dag(pipeline)
```

## Where to go next

- [Examples](../examples/basics.md) — runnable patterns in order of difficulty.
- [Quick reference](../reference/quickref.md) — one-page operator and API tables.
- [API](../api.md) — full docstrings.
