# run_node: recursive execution on RunContext. Shared Step instances run once per run
# (memo + in-flight channel). Optional context_input is data from Sequence / Pipe.

# Broadcast of >> → BroadcastPipe.
function Base.Broadcast.broadcasted(::typeof(>>), left::AbstractNode, right::AbstractNode)
    BroadcastPipe(left, right)
end

# Internal: run a set of (node, context) pairs in parallel.
struct ParallelBranches{N,C}
    nodes::Vector{N}
    contexts::Vector{C}
end

# Claim outcome token: caller is responsible for executing the step (see `claim_step!`).
struct ExecuteClaim end

function run_node(pb::ParallelBranches, ctx::RunContext, forced::Bool=false, context_input=nothing)
    nodes, contexts = pb.nodes, pb.contexts
    n = length(nodes)
    length(contexts) == n || throw(ArgumentError("nodes and contexts length mismatch"))
    n == 0 && return AbstractStepResult[]
    n == 1 && return run_node(nodes[1], ctx, forced, contexts[1])
    log_parallel(ctx, n)
    chunk_size = ctx.jobs == 0 ? n : ctx.jobs
    results = AbstractStepResult[]
    for i in 1:chunk_size:n
        rng = i:min(i + chunk_size - 1, n)
        tasks = [@spawn run_node(nodes[k], ctx, forced, contexts[k]) for k in rng]
        for t in tasks
            append!(results, fetch(t))
        end
        ctx.jobs > 0 && log_progress(ctx, length(results), n)
    end
    results
end

# Step wrapping another node (e.g. @step name = a >> b): delegate when work is a node.
function run_node(step::Step{N}, ctx::RunContext, forced::Bool=false, context_input=nothing) where {N<:AbstractNode}
    log_start(ctx, step)
    run_node(step.work, ctx, forced, context_input)
end

# Resourced wrapper: budget is acquired only when the wrapped node is actually going
# to execute. If the inner is a memoized Step that another task is already running, we
# do not acquire — otherwise waiting visitors would double-count the budget.
run_node(r::Resourced, ctx::RunContext, forced::Bool=false, context_input=nothing) =
    run_resourced(r.node, r.resources, ctx, forced, context_input)

# Step with no context_input: claim, and only acquire on ExecuteClaim.
function run_resourced(step::Step, res::Resources, ctx::RunContext, forced::Bool, ::Nothing)
    handle_resourced_claim(claim_step!(ctx, step), step, res, ctx, forced)
end

# Anything else (or Step with context_input): acquire around the run unconditionally;
# such nodes are not deduplicated by the runtime, so each visit performs work.
function run_resourced(node::AbstractNode, res::Resources, ctx::RunContext, forced::Bool, context_input)
    with_acquired_resources(res, ctx) do
        run_node(node, ctx, forced, context_input)
    end
end

handle_resourced_claim(cached::Vector{AbstractStepResult}, ::Step, ::Resources, ::RunContext, ::Bool) = cached
handle_resourced_claim(ch::Channel{Vector{AbstractStepResult}}, ::Step, ::Resources, ::RunContext, ::Bool) = fetch(ch)
function handle_resourced_claim(::ExecuteClaim, step::Step, res::Resources, ctx::RunContext, forced::Bool)
    with_acquired_resources(res, ctx) do
        out = execute_with_freshness(step, ctx, forced, nothing)
        publish_step!(ctx, step, out)
        out
    end
end

# Acquire memory/thread budget around `f()`. If `f()` throws, slots stay charged
# until this RunContext is dropped; avoid throwing from inside `with_resources`
# subtrees when parallel branches share one `run()` or waiters can stall.
function with_acquired_resources(f, res::Resources, ctx::RunContext)
    mem_taken = acquire!(ctx.memory_budget, res.mem_mb)
    threads_taken = acquire!(ctx.thread_budget, res.threads)
    out = f()
    release!(ctx.thread_budget, threads_taken)
    release!(ctx.memory_budget, mem_taken)
    out
end

# Leaf step. The first task to encounter `step` in a run executes it; concurrent and
# subsequent visitors receive the cached result. Identity is by `objectid`, so a Step
# instance referenced from multiple branches of the DSL runs exactly once per run.
function run_node(step::Step, ctx::RunContext, forced::Bool=false, context_input=nothing)
    context_input === nothing ||
        return execute_with_freshness(step, ctx, forced, context_input)
    handle_claim(claim_step!(ctx, step), step, ctx, forced)
end

# Claim outcomes — dispatch instead of `isa` branching.
handle_claim(cached::Vector{AbstractStepResult}, ::Step, ::RunContext, ::Bool) = cached
handle_claim(ch::Channel{Vector{AbstractStepResult}}, ::Step, ::RunContext, ::Bool) = fetch(ch)
function handle_claim(::ExecuteClaim, step::Step, ctx::RunContext, forced::Bool)
    out = execute_with_freshness(step, ctx, forced, nothing)
    publish_step!(ctx, step, out)
    out
end

function execute_with_freshness(step::Step, ctx::RunContext, forced::Bool, context_input)
    if !forced && is_fresh(step, ctx)
        log_skip(ctx, step)
        return AbstractStepResult[StepResult(step, true, 0.0, step.inputs, step.outputs,
                                             "up to date (not re-run)")]
    end
    log_start(ctx, step)
    result = run_step_work(step, ctx, context_input)
    log_result(ctx, result)
    result.success && mark_complete!(ctx, step)
    AbstractStepResult[maybe_spill_result(result, ctx)]
end

# After a successful step, if its in-memory `.result` is bigger than the spill
# threshold, serialise it to a tempfile and replace the field with a tiny
# `SpilledValue`. Keeps the per-run memo's RAM footprint bounded regardless of
# how many step results accumulate. Already-on-disk wrappers (FilePath /
# SpilledValue) and failure error strings are passed through unchanged.
function maybe_spill_result(r::StepResult, ctx::RunContext)
    ctx.auto_spill || return r
    r.success || return r
    spill_candidate(r.result) || return r
    sz = Base.summarysize(r.result)
    sz > ctx.spill_threshold_bytes || return r
    spilled = spill_to_disk(r.result, ctx.spill_dir)
    StepResult(r.step, r.success, r.duration, r.inputs, r.outputs, spilled)
end

# Don't spill values that are already on disk (FilePath, SpilledValue,
# SpilledStdout) or nothing.
spill_candidate(::Nothing) = false
spill_candidate(::FilePath) = false
spill_candidate(::SpilledValue) = false
spill_candidate(::SpilledStdout) = false
spill_candidate(_) = true

# Dispatch the actual work. First arg specificity orders these unambiguously.
run_step_work(step::Step{F}, ctx::RunContext, ::Nothing) where {F<:Function} = execute(step, ctx)
run_step_work(step::Step{F}, ctx::RunContext, input) where {F<:Function} = execute(step, ctx, input)
run_step_work(step::Step, ctx::RunContext, _) = execute(step, ctx)

# Atomic claim: caller may receive cached results, a channel to wait on, or an
# `ExecuteClaim` token signalling that the caller is now responsible for execution.
function claim_step!(ctx::RunContext, step::Step)
    lock(ctx.state_lock) do
        haskey(ctx.memo, step) && return ctx.memo[step]
        haskey(ctx.in_flight, step) && return ctx.in_flight[step]
        ctx.in_flight[step] = Channel{Vector{AbstractStepResult}}(1)
        ExecuteClaim()
    end
end

# Publish a step's results: memoise (success or failure), pop in_flight, and put
# on the channel so any waiters see the result. The memo write, in_flight pop, and
# put! all happen under the same lock so a late visitor either sees the cached
# result OR is in the channel's wait set — never racing into a fresh execution.
# Memoising failures means a `Retry` must explicitly invalidate the memo before
# re-attempting; see `invalidate_memo!` and `run_node(::Retry, ...)`.
function publish_step!(ctx::RunContext, step::Step, results::Vector{AbstractStepResult})
    lock(ctx.state_lock) do
        ctx.memo[step] = results
        ch = pop!(ctx.in_flight, step)
        put!(ch, results)            # capacity-1, never blocks on an empty buffer
    end
    nothing
end

# Drop memo entries (and any stale in_flight markers) for every Step reachable
# from `node`. Used by `Retry` between attempts so a re-attempt actually re-runs.
function invalidate_memo!(ctx::RunContext, node::AbstractNode)
    lock(ctx.state_lock) do
        for s in steps(node)
            delete!(ctx.memo, s)
            delete!(ctx.in_flight, s)
        end
    end
    nothing
end

# Sequence (>>): pass the previous step's output to the next step; deterministic rule —
# if the previous step has declared outputs, pass that vector; otherwise pass the result.
sequence_output_for_next(r::AbstractStepResult) = isempty(r.outputs) ? r.result : r.outputs

function run_node(seq::Sequence, ctx::RunContext, forced::Bool=false, context_input=nothing)
    results = AbstractStepResult[]
    out = context_input
    for node in seq.nodes
        node_results = run_node(node, ctx, forced, out)
        append!(results, node_results)
        any(r -> !r.success, node_results) && return results
        out = sequence_output_for_next(node_results[end])
    end
    results
end

function run_node(par::Parallel, ctx::RunContext, forced::Bool=false, context_input=nothing)
    n = length(par.nodes)
    run_node(ParallelBranches(par.nodes, fill(nothing, n)), ctx, forced, context_input)
end

function run_node(r::Retry, ctx::RunContext, forced::Bool=false, context_input=nothing)
    local results::Vector{AbstractStepResult}
    for attempt in 1:r.max_attempts
        log_retry(ctx, attempt, r.max_attempts)
        # Drop the previous attempt's cached results for the inner node so the
        # next call actually re-executes (without this, the success+failure-aware
        # publish would short-circuit to the cached failure).
        attempt > 1 && invalidate_memo!(ctx, r.node)
        results = run_node(r.node, ctx, forced, context_input)
        all(r -> r.success, results) && return results
        attempt < r.max_attempts && r.delay > 0 && sleep(r.delay)
    end
    results
end

function run_node(f::Fallback, ctx::RunContext, forced::Bool=false, context_input=nothing)
    results = run_node(f.primary, ctx, forced, context_input)
    all(r -> r.success, results) && return results
    log_fallback(ctx)
    run_node(f.fallback, ctx, forced, context_input)
end

branch_eval(b::Branch{C,T,F}, ::Nothing) where {C<:Function,T<:AbstractNode,F<:AbstractNode} =
    b.condition()
branch_eval(b::Branch{C,T,F}, ctx) where {C<:Function,T<:AbstractNode,F<:AbstractNode} =
    b.condition(ctx)

function run_node(b::Branch, ctx::RunContext, forced::Bool=false, context_input=nothing)
    cond = branch_eval(b, context_input)
    log_branch(ctx, cond)
    run_node(cond ? b.if_true : b.if_false, ctx, forced, context_input)
end

# Timeout: inner node on a task; one-shot Timer interrupts at the deadline.
# Worker wraps `run_node` in `run_safely` so throws become results (see `:inner_exception`).
# Only one of worker or timer puts on the channel. Subprocesses from Base.run are not
# reliably killed on interrupt.
function run_node(t::Timeout, ctx::RunContext, forced::Bool=false, context_input=nothing)
    log_timeout(ctx, t.seconds)
    ch = Channel{Vector{AbstractStepResult}}(1)
    winner = Threads.Atomic{UInt8}(0)
    worker_ref = Ref{Union{Nothing,Task}}(nothing)
    worker = Threads.@spawn begin
        outcome = run_safely(() -> run_node(t.node, ctx, forced, context_input))
        if Threads.atomic_cas!(winner, 0x00, 0x01) === 0x00
            if outcome.ok
                put!(ch, outcome.value)
            else
                put!(ch, timeout_inner_results(t, outcome.value))
            end
        end
    end
    worker_ref[] = worker
    tim = Timer(t.seconds) do timer
        close(timer)
        if Threads.atomic_cas!(winner, 0x00, 0x02) === 0x00
            w = worker_ref[]
            w !== nothing && !istaskdone(w) && schedule(w, InterruptException(); error=true)
            put!(ch, timeout_step_results(t))
        end
    end
    out = take!(ch)
    close(tim)
    out
end

function timeout_step_results(t::Timeout)
    timeout_step = Step(:timeout, `true`)
    fail = StepFailure(:timed_out, "Timeout after $(t.seconds)s";
                       detail="The inner node did not finish before the deadline. External processes may keep running.")
    AbstractStepResult[StepResult(timeout_step, false, t.seconds, timeout_step.inputs, timeout_step.outputs, fail)]
end

function timeout_inner_results(t::Timeout, err::StepFailure)
    timeout_step = Step(:timeout, `true`)
    inner = StepFailure(:inner_exception, "Inner node failed inside Timeout";
                        detail=sprint(showerror, err))
    AbstractStepResult[StepResult(timeout_step, false, t.seconds, timeout_step.inputs, timeout_step.outputs, inner)]
end

function run_node(f::Force, ctx::RunContext, ::Bool=false, context_input=nothing)
    log_force(ctx)
    run_node(f.node, ctx, true, context_input)
end

function run_node(r::Reduce, ctx::RunContext, forced::Bool=false, context_input=nothing)
    start = time()
    results = run_node(r.node, ctx, forced, context_input)
    outputs = [res.result for res in results if res.success]
    rs = r.reduce_step
    reduce_step_result(payload, success) =
        StepResult(rs, success, time() - start, rs.inputs, rs.outputs, payload)
    length(outputs) < length(results) &&
        return push!(results, reduce_step_result("Reduce aborted: upstream failed", false))
    log_reduce(ctx, r.name)
    outcome = run_safely(() -> r.reducer(outputs))
    outcome.ok ||
        return push!(results, reduce_step_result("Reduce error: $(outcome.value)", false))
    mark_complete!(ctx, rs)
    push!(results, reduce_step_result(outcome.value, true))
end

# Cycle check for ForEach expansion.
contains_node(needle::ForEach, x::ForEach) = x === needle
contains_node(needle::ForEach, s::Step) = step_contains(needle, s)
contains_node(needle::ForEach, n::AbstractNode) = any(c -> contains_node(needle, c), node_children(n))
step_contains(needle::ForEach, s::Step{N}) where {N<:AbstractNode} = s.work === needle || contains_node(needle, s.work)
step_contains(::ForEach, ::Step) = false
node_children(n::Sequence) = n.nodes
node_children(n::Parallel) = n.nodes
node_children(n::Retry) = [n.node]
node_children(n::Fallback) = [n.primary, n.fallback]
node_children(n::Branch) = [n.if_true, n.if_false]
node_children(n::Timeout) = [n.node]
node_children(n::Force) = [n.node]
node_children(n::Reduce) = [n.node]
node_children(p::Pipe) = [p.first, p.second]
node_children(sip::SameInputPipe) = [sip.first, sip.second]
node_children(bp::BroadcastPipe) = [bp.first, bp.second]
node_children(r::Resourced) = [r.node]
node_children(::Step) = AbstractNode[]
node_children(::ForEach) = AbstractNode[]
node_children(::NoWork) = AbstractNode[]

function run_node(pipe::Pipe, ctx::RunContext, forced::Bool=false, context_input=nothing)
    r1 = run_node(pipe.first, ctx, forced, context_input)
    any(!r.success for r in r1) && return r1
    out = length(r1) == 1 ? sequence_output_for_next(r1[1]) :
                            [sequence_output_for_next(r) for r in r1 if r.success]
    append!(r1, run_node(pipe.second, ctx, forced, out))
end

function run_node(sip::SameInputPipe, ctx::RunContext, forced::Bool=false, context_input=nothing)
    r1 = run_node(sip.first, ctx, forced, context_input)
    any(!r.success for r in r1) && return r1
    append!(r1, run_node(sip.second, ctx, forced, context_input))
end

function run_node(bp::BroadcastPipe{<:ForEach, <:AbstractNode}, ctx::RunContext, forced::Bool=false, context_input=nothing)
    nodes, contexts = expand_foreach(bp.first)
    paired = AbstractNode[n >> bp.second for n in nodes]
    run_node(ParallelBranches(paired, contexts), ctx, forced, nothing)
end
function run_node(bp::BroadcastPipe{<:Parallel, <:AbstractNode}, ctx::RunContext, forced::Bool=false, context_input=nothing)
    run_node(Parallel(AbstractNode[n >> bp.second for n in bp.first.nodes]), ctx, forced, context_input)
end
function run_node(bp::BroadcastPipe, ctx::RunContext, forced::Bool=false, context_input=nothing)
    run_node(bp.first >> bp.second, ctx, forced, context_input)
end

# Expand a ForEach into (nodes, per-branch contexts). The pattern variant
# always returns `matches::Vector{Vector{String}}` as contexts so the contexts
# vector is type-stable across branches; the user block still sees unwrapped
# scalars (via `fe.f(c[1])` or `fe.f(c...)`).
function expand_foreach(fe::ForEach{F, String}) where F
    matches = find_matches(fe.source, for_each_regex(fe.source))
    nodes = AbstractNode[ensure_foreach_node(fe, length(c) == 1 ? fe.f(c[1]) : fe.f(c...)) for c in matches]
    nodes, matches
end
function expand_foreach(fe::ForEach{F, Vector{T}}) where {F, T}
    nodes = AbstractNode[ensure_foreach_node(fe, fe.f(item)) for item in fe.source]
    nodes, fe.source
end

function ensure_foreach_node(fe::ForEach, r)
    r === nothing && error("ForEach block must return a Step or node, not nothing.")
    node = as_node(r)
    contains_node(fe, node) && error("Pipeline cycle: ForEach block returned a node containing this ForEach.")
    node
end

# Surface "no files matched" as a structured StepFailure rather than a raw
# exception, so callers see it in the result vector like any other step failure.
function foreach_no_matches_result(fe::ForEach{F, String}) where F
    step = Step(Symbol("foreach[", fe.source, "]"), `true`)
    failure = StepFailure(:no_matches, "ForEach: no files match '$(fe.source)'")
    AbstractStepResult[StepResult(step, false, 0.0, step.inputs, step.outputs, failure)]
end

function run_node(fe::ForEach, ctx::RunContext, forced::Bool=false, context_input=nothing)
    nodes, contexts = expand_foreach(fe)
    isempty(nodes) && return foreach_no_matches_result(fe)
    run_node(ParallelBranches(nodes, contexts), ctx, forced, nothing)
end

# No-op node — produces no step results.
run_node(::NoWork, ::RunContext, ::Bool=false, _=nothing) = AbstractStepResult[]

"""
    steps(node) -> Vector{Step}

Return all leaf steps in the DAG (flattened). Shared sub-graphs are listed once.
Composite nodes recurse via `node_children`; leaf nodes specialise.
"""
steps(s::Step) = Step[s]
steps(::ForEach) = Step[]   # lazy: not discovered until run
steps(::NoWork) = Step[]
steps(node::AbstractNode) =
    unique_by_id(mapreduce(steps, vcat, node_children(node); init=Step[]))

unique_by_id(v::AbstractVector{<:Step}) = unique(objectid, v)

"""
    count_steps(node) -> Int

Number of distinct leaf steps that may execute. Shared sub-DAGs are counted once.
For `Branch`, returns the maximum of the two branches (only one runs); for `Reduce`,
includes the synthetic reducer step.
"""
count_steps(n::AbstractNode) = length(steps(n))
count_steps(b::Branch) = max(length(steps(b.if_true)), length(steps(b.if_false)))
count_steps(r::Reduce) = length(steps(r.node)) + 1
count_steps(::ForEach) = 0
count_steps(::NoWork) = 0
