# run(pipeline) and run_pipeline. Included after run_node and Pipeline.

"""
    run(p::Pipeline; verbose=true, dry_run=false, force=false, jobs=8, keep_outputs=:last) -> Vector{AbstractStepResult}
    run(node::AbstractNode; kwargs...) -> Vector{AbstractStepResult}

Execute a pipeline or node, returning results for each step.

# Keywords
- `verbose=true`: Show colored progress output; when true, prints each shell command (for steps that run external tools) before running it
- `dry_run=false`: If true, show DAG structure without executing
- `force=false`: If true, run all steps regardless of freshness
- `jobs=8`: Max concurrent branches for Parallel/ForEach. All branches run; when `jobs > 0`, they run in rounds of `jobs` (each round waits for the previous). `jobs=0` = unbounded (all at once).
- `keep_outputs=:last`: What to retain in each result's `.result`. `:last` (default) keeps only the last step's result (others get `nothing`); `:all` keeps every step's result; `:none` drops all.

# When result is kept vs dropped
`keep_outputs` only affects the **returned** results vector: after the run we replace `.result` with `nothing` for non-final steps. **During execution** all step outputs stay in memory until the run finishes.

# Memory: path-based I/O and streaming reducers
To avoid holding large data in memory:
1. **Steps**: Write large results to files and **return only the path** (or a small summary). Then the runner only holds path strings, not file contents.
2. **Reduce**: The reducer receives a **vector of all branch outputs**. If those are paths, implement a **streaming reducer**: open one path at a time, read/aggregate, then close. Do not load all files into memory inside the reducer. Example: `reducer(paths) = (acc = init; for p in paths; acc = merge(acc, read_stats(p)); end; acc)`.
3. Use `keep_outputs=:last` so the returned result vector does not retain every step's result.

If you follow (1) and (2), you only hold paths and small aggregates; full file contents are never all in memory. If a step returns a large object (e.g. a DataFrame of the whole file), that object is held until the run ends.

# Output
With `verbose=true`, shows tree-structured output: `▶` running, `✓` success, `✗` failure, `⊳` up to date (not re-run). Shell commands are printed so you see exactly what is run.

# Examples
```julia
run(pipeline)
run(pipeline, jobs=0)
run(pipeline, keep_outputs=:all)
run(pipeline, verbose=false)
run(pipeline, dry_run=true)
run(pipeline, force=true)
```

See also: [`is_fresh`](@ref), [`Force`](@ref), [`print_dag`](@ref)
"""
function Base.run(p::Pipeline; verbose::Bool=true, dry_run::Bool=false, force::Bool=false, jobs::Int=8, keep_outputs::Symbol=:last)
    prev_jobs = MAX_PARALLEL[]
    MAX_PARALLEL[] = jobs
    results = run_pipeline(p, verbose, dry_run, force, keep_outputs)
    MAX_PARALLEL[] = prev_jobs
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
        results = [StepResult(r.step, r.success, r.duration, r.inputs, r.outputs, nothing) for r in results]
    elseif keep_outputs === :last && !isempty(results)
        n = length(results)
        results = [StepResult(r.step, r.success, r.duration, r.inputs, r.outputs, i == n ? r.result : nothing) for (i, r) in enumerate(results)]
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
