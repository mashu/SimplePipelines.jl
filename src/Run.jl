# Public run() entry point. Builds a RunContext per call (no mutable globals), runs
# the root node, persists state, and returns the per-step results.

"""
    run(p::Pipeline; verbose=true, dry_run=false, force=false, jobs=8,
        memory_budget_mb=0, thread_budget=0) -> Vector{AbstractStepResult}
    run(node::AbstractNode; kwargs...) -> Vector{AbstractStepResult}

Execute a pipeline or node, returning a `StepResult` for every step that ran.

# Keywords
- `verbose=true`: Show colored progress output.
- `dry_run=false`: Show DAG structure without executing.
- `force=false`: Run all steps regardless of freshness.
- `jobs=8`: Max concurrent branches for Parallel/ForEach (`jobs=0` = unbounded).
- `memory_budget_mb=0`: Soft cap (megabytes) on concurrent memory across nodes wrapped
  with [`with_resources`](@ref); `0` disables the cap.
- `thread_budget=0`: Soft cap on concurrent CPU threads across nodes that declare
  `threads`; `0` disables the cap.

# Memory
Step results live in the per-run memo for the duration of the run (so DAG sharing
works). For pipelines that produce large in-memory data per step, write the value
to disk and return a [`FilePath`](@ref); only the path travels between steps.

The returned vector contains every step's result. If you only want the terminal
value, take `last(results)`; if you want successes, `filter(r -> r.success, results)`.

See also: [`is_fresh`](@ref), [`Force`](@ref), [`print_dag`](@ref),
[`with_resources`](@ref), [`FilePath`](@ref).
"""
function Base.run(p::Pipeline; verbose::Bool=true, dry_run::Bool=false, force::Bool=false,
                  jobs::Int=8, memory_budget_mb::Int=0, thread_budget::Int=0)
    print_pipeline_header(verbose, p)
    if dry_run
        verbose && print_dag(p.root)
        return AbstractStepResult[]
    end
    ctx = RunContext(verbose=verbose, jobs=jobs, state_path=STATE_FILE[],
                     memory_budget_mb=memory_budget_mb,
                     thread_budget=thread_budget)
    start = time()
    results = run_node(p.root, ctx, force, nothing)
    save_state!(merge_state(ctx), ctx.state_path)
    print_pipeline_footer(verbose, results, start)
    results
end

Base.run(node::AbstractNode; kwargs...) = run(Pipeline(node); kwargs...)

function print_pipeline_header(verbose::Bool, p::Pipeline)
    verbose || return
    printstyled("═══ Pipeline: ", color=:blue, bold=true)
    printstyled(p.name, " ═══\n", color=:blue, bold=true)
end

function print_pipeline_footer(verbose::Bool, results, start::Float64)
    verbose || return
    n = count(r -> r.success, results)
    total = length(results)
    color = n == total ? :green : :red
    printstyled("═══ Completed: ", color=:blue, bold=true)
    printstyled("$n/$total", color=color, bold=true)
    printstyled(" steps in $(round(time() - start, digits=2))s ═══\n", color=:blue, bold=true)
end

merge_state(ctx::RunContext) = union!(copy(ctx.persisted), ctx.state)
