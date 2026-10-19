# Entry points: run executes, check plans without executing.


"""Number of CPU threads; the default for `jobs`."""
default_jobs() = Sys.CPU_THREADS

"""Half of the machine's memory in MiB; the default memory budget for declared `mem_mb`."""
default_mem_mb() = floor(Int, Sys.total_memory() / 2^20 / 2)

"""
    run(node; jobs, mem_mb, force=false, verbose=true, state_path=".pipeline_state") -> RunResult
    run(rules...; targets, filter, kwargs...) -> RunResult

Run a pipeline and return a [`RunResult`](@ref): one [`StepResult`](@ref) per step,
the overall `success`, and the pipeline's `value`.

- `jobs`: CPU slots (default: number of CPU threads). A step occupies as many slots
  as its `threads`, so at most `jobs` single-threaded steps run at once, however the
  pipeline is nested.
- `mem_mb`: memory budget for steps that declare `mem_mb` (default: half the RAM).
- `force`: run every step even when it is up to date.
- `verbose`: print progress.
- `state_path`: where recipes of built outputs are remembered.

Steps with declared outputs are skipped when up to date; the others always run. A
failing step stops its branch; independent branches keep going, so one bad sample
does not stop the others.
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
                 jobs::Integer=default_jobs(), mem_mb::Integer=default_mem_mb(),
                 state_path::AbstractString=STATE_FILE)
    rt = Runtime(; verbose, planning, jobs, mem_mb, state_path)
    scope = Scope(force, Resources(), rt.root, Int[], "")
    log_header(rt, p.name)
    started = time()
    out = run_safely(() -> run_node(p.root, rt, scope, nothing); interrupted=() -> expire!(rt.root))
    top = top_outcome(rt, out)
    planning || save_state!(rt.state)
    result = RunResult(p.name, with_lock(copy, rt.lock, rt.results), top.ok, top.value, time() - started, planning)
    log_footer(rt, result)
    result
end

# run_node returns an Outcome; an exception escaping it is a bug, reported as a failure.
top_outcome(rt::Runtime, out::Outcome) = out.ok ? out.value :
    node_failed!(rt, :pipeline, "internal error", StepFailure(:exception, "internal error: " * out.value.message;
                                                              detail=out.value.detail))
