# run(pipeline) and run_pipeline. Included after run_node and Pipeline.

"""
    run(p::Pipeline; verbose=true, dry_run=false, force=false, jobs=8, keep_outputs=:last) -> Vector{AbstractStepResult}
    run(node::AbstractNode; kwargs...) -> Vector{AbstractStepResult}

Execute a pipeline or node, returning results for each step.

# Keywords
- `verbose=true`: Show colored progress output
- `dry_run=false`: If true, show DAG structure without executing
- `force=false`: If true, run all steps regardless of freshness
- `jobs=8`: Max concurrent branches for Parallel/ForEach/Map. All branches run; when `jobs > 0`, they run in rounds of `jobs` (each round waits for the previous). `jobs=0` = unbounded (all at once).
- `keep_outputs=:last`: What to retain in each result's `.output`. `:last` (default) keeps only the last result's output (others get `nothing`); `:all` keeps every step's output; `:none` drops all outputs.
- `memory_limit=nothing`: If set to an integer (bytes), we wait before starting each new batch of parallel tasks when `Base.gc_live_bytes()` exceeds this (best-effort backpressure).

# When output is kept vs dropped
`keep_outputs` only affects the **returned** results vector: after the run we replace `.output` with `nothing` for non-final steps. **During execution** all step outputs stay in memory (e.g. Reduce collects every branch output before calling the reducer). So to limit memory you must (1) have steps write to a file and return the path; (2) reducer streams paths (open one at a time); (3) use `keep_outputs=:last` so the returned vector doesn't hold intermediate data.

# Memory limit (best-effort)
- `memory_limit=nothing`: No limit (default).
- `memory_limit=N` (integer, bytes): Before starting each new batch of parallel tasks we check `Base.gc_live_bytes()`. If over `N`, we run `GC.gc()` and sleep briefly until below limit, then start the next batch. This backpressures concurrency when heap usage is high. Use together with `jobs` to cap both concurrency and memory.

# Output
With `verbose=true`, shows tree-structured output: `▶` running, `✓` success, `✗` failure, `⊳` up to date (not re-run).

# Examples
```julia
run(pipeline)                # Default: up to 8 branches at a time
run(pipeline, jobs=0)         # Unbounded (all branches at once)
run(pipeline, keep_outputs=:all)   # Keep every step's output (default is :last)
run(pipeline, memory_limit=2^31)   # Wait when heap > 2GB before starting next batch (best-effort)
run(pipeline, verbose=false)
run(pipeline, dry_run=true)
run(pipeline, force=true)
```

See also: [`is_fresh`](@ref), [`Force`](@ref), [`print_dag`](@ref)
"""
function Base.run(p::Pipeline; verbose::Bool=true, dry_run::Bool=false, force::Bool=false, jobs::Int=8, keep_outputs::Symbol=:last, memory_limit=nothing)
    prev_jobs = MAX_PARALLEL[]
    prev_mem = MEMORY_LIMIT[]
    MAX_PARALLEL[] = jobs
    MEMORY_LIMIT[] = memory_limit === nothing ? UInt64(0) : UInt64(memory_limit)
    results = run_pipeline(p, verbose, dry_run, force, keep_outputs)
    MAX_PARALLEL[] = prev_jobs
    MEMORY_LIMIT[] = prev_mem
    results
end

function run_pipeline(p::Pipeline, verbose::Bool, dry_run::Bool, force::Bool, keep_outputs::Symbol=:last)
    if verbose
        printstyled("═══ Pipeline: ", color=:blue, bold=true)
        printstyled(p.name, " ═══\n", color=:blue, bold=true)
    end

    if dry_run
        verbose && print_dag(p.root)
        return AbstractStepResult[]
    end

    run_depth[] += 1
    if run_depth[] == 1
        state_loaded[] = state_read(STATE_FILE[], STATE_LAYOUT)
        pending_completions[] = Set{UInt64}()
    end

    v = verbose ? Verbose() : Silent()
    start = time()
    results = run_node(p.root, v, force)

    if run_depth[] == 1
        save_state!(union(state_loaded[], pending_completions[]))
    end
    run_depth[] -= 1

    if keep_outputs === :none && !isempty(results)
        results = [StepResult(r.step, r.success, r.duration, r.inputs, nothing) for r in results]
    elseif keep_outputs === :last && !isempty(results)
        n = length(results)
        results = [StepResult(r.step, r.success, r.duration, r.inputs, i == n ? r.output : nothing) for (i, r) in enumerate(results)]
    end

    if verbose
        n = count(r -> r.success, results)
        total = length(results)
        success_color = n == total ? :green : :red
        printstyled("═══ Completed: ", color=:blue, bold=true)
        printstyled("$n/$total", color=success_color, bold=true)
        printstyled(" steps in $(round(time() - start, digits=2))s ═══\n", color=:blue, bold=true)
    end
    results
end

Base.run(node::AbstractNode; kwargs...) = run(Pipeline(node); kwargs...)
