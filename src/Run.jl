maybe_report(::Nothing, ::AbstractVector{<:AbstractStepResult}, ::AbstractString) = nothing
maybe_report(f, results::AbstractVector{<:AbstractStepResult}, name::AbstractString) =
    f(results; pipeline=name)

"""
    run(p::Pipeline; verbose=true, dry_run=false, force=false,
        jobs=default_jobs(), memory_budget_mb=default_memory_budget_mb(),
        thread_budget=0,
        auto_spill=true, spill_threshold_bytes=default_spill_threshold_bytes(),
        spill_dir=tempdir(), report=nothing) -> Vector{AbstractStepResult}
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
  [`with_resources`](@ref). Defaults to **50% of total system RAM**. Branches
  with declared `mem_mb` block on a semaphore until enough budget is free. Pass
  `0` to disable.
- `thread_budget=0`: Soft cap on concurrent CPU threads across nodes that declare
  `threads`; `0` disables the cap.
- `auto_spill=true`: Two complementary behaviours.
  *(1) Shell steps:* stdout streams **directly** to a tempfile in `spill_dir`
  while the command runs, so peak RAM is bounded by the OS pipe buffer rather
  than the command's total output. After the process exits, if the file is
  below `spill_threshold_bytes` it is read back as a `String` and the tempfile
  is deleted (preserving in-RAM semantics for typical short outputs); above
  threshold, `r.result` becomes a [`SpilledStdout`](@ref) wrapper that
  `materialize` reads back as a `String` on demand.
  *(2) Function steps:* after the step returns, if
  `Base.summarysize(r.result) > spill_threshold_bytes`, the value is
  serialised to a tempfile and `r.result` is replaced with a [`SpilledValue`](@ref).
  Together these keep the per-run memo's RAM footprint bounded regardless of
  step count or command output size. Pass `auto_spill=false` to keep every
  result in RAM (shell steps revert to the legacy `IOBuffer` capture).
- `spill_threshold_bytes`: Spill threshold; defaults to 10 MB.
- `spill_dir`: Where the spill tempfiles live; defaults to `tempdir()`. Pass a
  `mktempdir()` if you want auto-cleanup at scope exit.

# Memory safety
The package treats **disk as effectively infinite, RAM as finite**. Three
defaults combine to make a default-run pipeline memory-safe by construction:

1. `jobs` caps the live-execution set at the host's thread count.
2. `auto_spill` swaps each finished step's big result for a tiny `SpilledValue`
   so the memo doesn't accumulate raw values.
3. `memory_budget_mb` serialises declared-heavy steps via
   [`with_resources`](@ref).

Downstream consumers call [`materialize`](@ref) to retrieve a `SpilledValue`
or `FilePath`. Small step results (below threshold) stay in RAM with no I/O cost.

The returned vector contains every step's result. If you only want the terminal
value, take `last(results)`; if you want successes, `filter(r -> r.success, results)`.

- `report`: optional `report(results; pipeline::String)` called after the DAG finishes
  and before state is saved. Use for summaries, artifact registries, or CI badges.

See also: [`is_fresh`](@ref), [`Force`](@ref), [`print_dag`](@ref),
[`with_resources`](@ref), [`FilePath`](@ref), [`SpilledValue`](@ref),
[`materialize`](@ref), [`default_jobs`](@ref),
[`default_memory_budget_mb`](@ref).
"""
function Base.run(p::Pipeline; verbose::Bool=true, dry_run::Bool=false, force::Bool=false,
                  jobs::Int=default_jobs(),
                  memory_budget_mb::Int=default_memory_budget_mb(),
                  thread_budget::Int=0,
                  auto_spill::Bool=true,
                  spill_threshold_bytes::Int=default_spill_threshold_bytes(),
                  spill_dir::String=tempdir(),
                  state_path::String=STATE_FILE[],
                  report=nothing)
    print_pipeline_header(verbose, p)
    if dry_run
        verbose && print_dag(p.root)
        return AbstractStepResult[]
    end
    ctx = RunContext(verbose=verbose, jobs=jobs, state_path=state_path,
                     memory_budget_mb=memory_budget_mb,
                     thread_budget=thread_budget,
                     auto_spill=auto_spill,
                     spill_threshold_bytes=spill_threshold_bytes,
                     spill_dir=spill_dir)
    start = time()
    results = run_node(p.root, ctx, force, nothing)
    maybe_report(report, results, p.name)
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
