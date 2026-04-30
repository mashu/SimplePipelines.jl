# Public run() entry point. Builds a RunContext per call (no mutable globals), runs
# the root node, persists state, and post-processes the kept results.

"""
    run(p::Pipeline; verbose=true, dry_run=false, force=false, jobs=8, keep_outputs=:last) -> Vector{AbstractStepResult}
    run(node::AbstractNode; kwargs...) -> Vector{AbstractStepResult}

Execute a pipeline or node, returning results for each step.

# Keywords
- `verbose=true`: Show colored progress output.
- `dry_run=false`: Show DAG structure without executing.
- `force=false`: Run all steps regardless of freshness.
- `jobs=8`: Max concurrent branches for Parallel/ForEach (`jobs=0` = unbounded).
- `keep_outputs=:last`: Retain only the last step's `result` (`:all` keeps every step's;
  `:none` drops all).

# Memory
`keep_outputs` only affects the **returned** vector; during execution all step outputs
stay in memory. Write large data to files and return paths to keep memory bounded.

See also: [`is_fresh`](@ref), [`Force`](@ref), [`print_dag`](@ref)
"""
function Base.run(p::Pipeline; verbose::Bool=true, dry_run::Bool=false, force::Bool=false,
                  jobs::Int=8, keep_outputs::Symbol=:last)
    print_pipeline_header(verbose, p)
    if dry_run
        verbose && print_dag(p.root)
        return AbstractStepResult[]
    end
    ctx = RunContext(verbose=verbose, jobs=jobs, state_path=STATE_FILE[])
    start = time()
    results = run_node(p.root, ctx, force, nothing)
    save_state!(merge_state(ctx), ctx.state_path)
    results = post_process_results(results, keep_outputs, p.root)
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

function post_process_results(results::Vector{<:AbstractStepResult}, keep::Symbol, root::AbstractNode)
    isempty(results) && return results
    keep === :all && return results
    keep === :none && return [drop_result(r) for r in results]
    keep === :last && return terminal_only(results, root)
    throw(ArgumentError("keep_outputs must be :all, :last, or :none; got $(repr(keep))"))
end

drop_result(r::StepResult) = StepResult(r.step, r.success, r.duration, r.inputs, r.outputs, nothing)

function last_only(results::Vector{<:AbstractStepResult})
    n = length(results)
    [i == n ? r : drop_result(r) for (i, r) in enumerate(results)]
end

merge_state(ctx::RunContext) = union!(copy(ctx.persisted), ctx.state)

terminal_steps(s::Step) = Step[s]
terminal_steps(s::Step{N}) where {N<:AbstractNode} = terminal_steps(s.work)
terminal_steps(s::Sequence) = terminal_steps(s.nodes[end])
terminal_steps(p::Parallel) = unique_by_id(reduce(vcat, terminal_steps.(p.nodes)))
terminal_steps(r::Retry) = terminal_steps(r.node)
terminal_steps(f::Fallback) = unique_by_id(vcat(terminal_steps(f.primary), terminal_steps(f.fallback)))
terminal_steps(b::Branch) = unique_by_id(vcat(terminal_steps(b.if_true), terminal_steps(b.if_false)))
terminal_steps(t::Timeout) = terminal_steps(t.node)
terminal_steps(f::Force) = terminal_steps(f.node)
terminal_steps(r::Reduce) = Step[Step(r.name, r.reducer)]
terminal_steps(p::Pipe) = terminal_steps(p.second)
terminal_steps(sip::SameInputPipe) = terminal_steps(sip.second)
terminal_steps(bp::BroadcastPipe) = terminal_steps(bp.second)
terminal_steps(::ForEach) = Step[]

function terminal_only(results::Vector{<:AbstractStepResult}, root::AbstractNode)
    terminals = terminal_steps(root)
    terminal_hashes = Set(step_hash(s) for s in terminals)
    [step_hash(r.step) in terminal_hashes ? r : drop_result(r) for r in results]
end
