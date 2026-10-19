# Entry points: run executes, check plans without executing.

@nospecialize   # see runtime.jl

"""CPUs this process may use (affinity and cgroup quota); the default for `jobs`."""
default_jobs() = effective_cpus()

"""80% of the memory this process may use (MiB, cgroup limits included); the default budget."""
default_budget() = round(Int, 0.8 * last(memory_snapshot()))

"""
    run(node; jobs, mem_mb, min_free_mb=0, nice=10, guard=true, force=false, verbose=true,
        state_path=".pipeline_state") -> RunResult
    run(rules...; targets, filter, kwargs...) -> RunResult

Run a pipeline and return a [`RunResult`](@ref): one [`StepResult`](@ref) per step,
the overall `success`, and the pipeline's `value`.

- `jobs`: CPU slots (default: the CPUs this process may use). A step occupies as many
  slots as its `threads`, so at most `jobs` single-threaded steps run at once, however
  the pipeline is nested.
- `mem_mb`: memory budget in MiB for the steps running at once (default: 80% of the
  memory this process may use; 0 = no budget). A step is charged its `mem_mb`, or
  $(UNDECLARED_CHARGE_MB) MiB when it declares none; a larger declaration is clamped to the budget.
- `min_free_mb`: memory to keep free (default 0: automatic, 5% of memory between 256 and
  2048 MiB). Below twice this, new steps wait; below it, the pipeline's own commands are
  stopped and queued again before the computer starts swapping.
- `nice`: scheduling priority of the commands (0–19, default 10), so the computer stays
  responsive.
- `guard`: enforce limits with the resource guard (default). `guard=false` runs without
  memory limits, timeouts or cleanup.
- `force`: run every step even when it is up to date.
- `verbose`: print progress.
- `state_path`: where recipes of built outputs are remembered.

Steps with declared outputs are skipped when up to date; the others always run. A
failing step stops its branch; independent branches keep going, so one bad sample
does not stop the others. See [Resource limits](@ref "Resource limits and safety") for
what the guard enforces.
"""
Base.run(node::Union{AbstractNode,Shell}; kwargs...) = run(Pipeline(node); kwargs...)
Base.run(wf::Workflow; targets=nothing, filter=nothing, kwargs...) = run(Pipeline(retarget(wf, targets, filter); name=wf.name); kwargs...)
Base.run(r::Rule, more::Union{Rule,Step}...; targets=String[], filter=nothing, name::AbstractString="workflow", kwargs...) =
    run(Workflow(r, more...; targets, filter, name); kwargs...)
Base.run(p::Pipeline; kwargs...) = execute(p, false; kwargs...)

"""
    check(node; kwargs...) -> RunResult
    check(rules...; targets, filter, kwargs...) -> RunResult

Work out what [`run`](@ref) would do without running anything: expand ForEach nodes,
resolve workflows, and report every step as `:planned` (with the reason it would
run), `:skipped` (up to date) or `:failed` (a problem such as an input that does not
exist and that no step produces). Takes the same keywords as `run`.
"""
check(node::Union{AbstractNode,Shell}; kwargs...) = check(Pipeline(node); kwargs...)
check(wf::Workflow; targets=nothing, filter=nothing, kwargs...) = check(Pipeline(retarget(wf, targets, filter); name=wf.name); kwargs...)
check(r::Rule, more::Union{Rule,Step}...; targets=String[], filter=nothing, name::AbstractString="workflow", kwargs...) =
    check(Workflow(r, more...; targets, filter, name); kwargs...)
check(p::Pipeline; kwargs...) = execute(p, true; kwargs...)

retarget(wf::Workflow, ::Nothing, ::Nothing) = wf
retarget(wf::Workflow, targets, filter) =
    Workflow(wf.rules; name=wf.name, targets=something(targets, wf.targets), filter=something(filter, Some(wf.filter)))

function execute(p::Pipeline, planning::Bool; verbose::Bool=true, force::Bool=false,
                 jobs::Integer=default_jobs(), mem_mb::Integer=default_budget(), min_free_mb::Integer=0,
                 nice::Integer=10, guard::Bool=true, state_path::AbstractString=STATE_FILE)
    link = planning ? NO_GUARD : connect(guard)
    rt = Runtime(; verbose, planning, jobs, mem_mb, min_free_mb, nice, state_path, link)
    scope = Scope(force, Resources(), rt.root, Int[], "")
    log_header(rt, p.name)
    open_run!(rt)
    started = time()
    out = run_safely(Call(run_node, (p.root, rt, scope, nothing)); interrupted=() -> (interrupt_all!(); close_run!(rt)))
    close_run!(rt)
    rt.interrupted[] && throw(InterruptException())    # Ctrl-C reached only the guard
    top = out.ok ? out.value : internal_error!(rt, out.value)
    save_run_state!(rt)
    result = RunResult(p.name, with_lock(copy, rt.lock, rt.results), top.ok, top.value, time() - started, planning)
    log_footer(rt, result)
    result
end

@specialize
