# Public run() entry point. Builds a RunContext per call (no mutable globals), runs
# the root node, persists state, and returns the per-step results.

"""
    run(p::Pipeline; verbose=true, dry_run=false, force=false,
        jobs=default_jobs(), memory_budget_mb=default_memory_budget_mb(),
        thread_budget=0) -> Vector{AbstractStepResult}
    run(node::AbstractNode; kwargs...) -> Vector{AbstractStepResult}

Execute a pipeline or node, returning a `StepResult` for every step that ran.

# Keywords
- `verbose=true`: Show colored progress output.
- `dry_run=false`: Show DAG structure without executing.
- `force=false`: Run all steps regardless of freshness.
- `jobs`: Max concurrent branches for Parallel/ForEach. Defaults to
  `min(Threads.nthreads(), 8)` so a fan-out with N threads cannot oversubscribe
  the machine. Pass `jobs=0` to disable (unbounded — only do this if every
  branch is annotated with [`with_resources`](@ref)).
- `memory_budget_mb`: Soft cap (MB) on concurrent memory across nodes wrapped with
  [`with_resources`](@ref). Defaults to **50% of total system RAM** so a default
  run is memory-safe by construction. Branches with declared `mem_mb` block on a
  semaphore until enough budget is free. Pass `0` to disable.
- `thread_budget=0`: Soft cap on concurrent CPU threads across nodes that declare
  `threads`; `0` disables the cap.

# Memory safety
The package treats **disk as effectively infinite, RAM as finite**. The
combination of `jobs` and `memory_budget_mb` defaults is chosen so an
unannotated pipeline cannot fan out beyond the host's thread count, and
annotated heavy steps cannot collectively exceed 50% of RAM. If a default-run
pipeline still OOMs your box, the cause is usually un-annotated heavy steps in
a tight `jobs` window — annotate them with [`with_resources`](@ref) (declared
`mem_mb`) and the budget will serialise them.

Step results live in the per-run memo for the duration of the run (so DAG
sharing works). For pipelines that produce large in-memory data per step,
write the value to disk and return a [`FilePath`](@ref); only the path travels
between steps.

The returned vector contains every step's result. If you only want the terminal
value, take `last(results)`; if you want successes, `filter(r -> r.success, results)`.

See also: [`is_fresh`](@ref), [`Force`](@ref), [`print_dag`](@ref),
[`with_resources`](@ref), [`FilePath`](@ref), [`default_jobs`](@ref),
[`default_memory_budget_mb`](@ref).
"""
function Base.run(p::Pipeline; verbose::Bool=true, dry_run::Bool=false, force::Bool=false,
                  jobs::Int=default_jobs(),
                  memory_budget_mb::Int=default_memory_budget_mb(),
                  thread_budget::Int=0)
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
