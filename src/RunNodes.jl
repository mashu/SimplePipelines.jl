# run_node dispatch, contains_node, node_children, steps, count_steps. Requires Types, State, Execute, Logging, ForEach.
# run_node(node, v, forced, context_input=nothing): context_input is passed to the first step when set (e.g. branch id from ForEach).
# Sequence (>>): first step gets context_input; each next step gets the previous step's output (data passing).

# Broadcast of >> (a .>> b): attach second to each branch of first; result is BroadcastPipe, run expands and runs.
function Base.Broadcast.broadcasted(::typeof(>>), left::AbstractNode, right::AbstractNode)
    BroadcastPipe(left, right)
end

# Internal: run a set of (node, context) pairs in parallel. Dispatched via run_node, not a private helper.
struct ParallelBranches{N,C}
    nodes::Vector{N}
    contexts::Vector{C}
end

function run_node(pb::ParallelBranches, v, forced::Bool, context_input=nothing)
    nodes = pb.nodes
    contexts = pb.contexts
    n = length(nodes)
    length(contexts) == n || throw(ArgumentError("nodes and contexts length mismatch"))
    log_parallel(v, n)
    max_p = MAX_PARALLEL[]
    if max_p > 0 && n > 0
        results = AbstractStepResult[]
        for i in 1:max_p:n
            chunk_inds = i:min(i + max_p - 1, n)
            chunk_nodes = [nodes[j] for j in chunk_inds]
            chunk_ctx = [contexts[j] for j in chunk_inds]
            append!(results, reduce(vcat, [fetch(@spawn run_node(chunk_nodes[k], v, forced, chunk_ctx[k])) for k in eachindex(chunk_nodes)]))
            log_progress(v, length(results), n)
        end
        return results
    else
        return n == 1 ? run_node(nodes[1], v, forced, contexts[1]) : reduce(vcat, [fetch(@spawn run_node(nodes[j], v, forced, contexts[j])) for j in 1:n])
    end
end

# Step wrapping another node (e.g. @step name = a >> b): delegate when work is AbstractNode
function run_node(step::Step{N}, v, forced::Bool=false, context_input=nothing) where {N<:AbstractNode}
    log_start(v, step)
    run_node(step.work, v, forced, context_input)
end

# Leaf step: when context_input is set and work is Function, run with that input; else run normally
function run_node(step::Step{F}, v, forced::Bool=false, context_input=nothing) where F
    if !forced && is_fresh(step)
        log_skip(v, step)
        return [StepResult(step, true, 0.0, step.inputs, step.outputs, "up to date (not re-run)")]
    end
    log_start(v, step)
    result = if F <: Function && context_input !== nothing
        execute(step, context_input)
    else
        execute(step)
    end
    log_result(v, result)
    result.success && mark_complete!(step)
    [result]
end

# Sequence (>>): pass data — first step gets context_input, each next gets previous step's output
function run_node(seq::Sequence, v, forced::Bool, context_input=nothing)
    results = AbstractStepResult[]
    out = context_input
    for node in seq.nodes
        node_results = run_node(node, v, forced, out)
        append!(results, node_results)
        any(r -> !r.success, node_results) && return results
        out = node_results[end].result
    end
    results
end

function run_node(par::Parallel, v, forced::Bool, context_input=nothing)
    n = length(par.nodes)
    log_parallel(v, n)
    max_p = MAX_PARALLEL[]
    if max_p > 0 && n > 0
        results = AbstractStepResult[]
        for i in 1:max_p:n
            chunk = par.nodes[i:min(i + max_p - 1, end)]
            append!(results, reduce(vcat, fetch.([@spawn run_node(node, v, forced, nothing) for node in chunk])))
            log_progress(v, length(results), n)
        end
        results
    else
        reduce(vcat, fetch.([@spawn run_node(node, v, forced, nothing) for node in par.nodes]))
    end
end

function run_node(r::Retry, v, forced::Bool, context_input=nothing)
    local results::Vector{AbstractStepResult}
    for attempt in 1:r.max_attempts
        log_retry(v, attempt, r.max_attempts)
        results = run_node(r.node, v, forced, context_input)
        all(r -> r.success, results) && return results
        attempt < r.max_attempts && r.delay > 0 && sleep(r.delay)
    end
    results
end

function run_node(f::Fallback, v, forced::Bool, context_input=nothing)
    results = run_node(f.primary, v, forced, context_input)
    all(r -> r.success, results) && return results
    log_fallback(v)
    run_node(f.fallback, v, forced, context_input)
end

function run_node(b::Branch, v, forced::Bool, context_input=nothing)
    cond = b.condition()
    log_branch(v, cond)
    run_node(cond ? b.if_true : b.if_false, v, forced, context_input)
end

function run_node(t::Timeout, v, forced::Bool, context_input=nothing)
    log_timeout(v, t.seconds)
    ch = Channel{Vector{AbstractStepResult}}(1)
    @async put!(ch, run_node(t.node, v, forced, context_input))

    deadline = time() + t.seconds
    while !isready(ch) && time() < deadline
        sleep(0.01)
    end

    timeout_step = Step(:timeout, `true`)
    isready(ch) ? take!(ch) : [StepResult(timeout_step, false, t.seconds, timeout_step.inputs, timeout_step.outputs, "Timeout after $(t.seconds)s")]
end

function run_node(f::Force, v, ::Bool, context_input=nothing)
    log_force(v)
    run_node(f.node, v, true, context_input)
end

function run_node(r::Reduce, v, forced::Bool, context_input=nothing)
    start = time()
    results = run_node(r.node, v, forced, context_input)
    outputs = [res.result for res in results if res.success]

    reduce_step = Step(r.name, r.reducer)
    if length(outputs) < length(results)
        return vcat(results, [StepResult(reduce_step, false, time() - start, reduce_step.inputs, reduce_step.outputs, "Reduce aborted: upstream failed")])
    end

    log_reduce(v, r.name)
    outcome = run_safely() do
        r.reducer(outputs)
    end
    if !outcome.ok
        return vcat(results, [StepResult(reduce_step, false, time() - start, reduce_step.inputs, reduce_step.outputs, "Reduce error: $(outcome.value)")])
    end
    vcat(results, [StepResult(reduce_step, true, time() - start, reduce_step.inputs, reduce_step.outputs, outcome.value)])
end

# Cycle check: does node contain needle? (ForEach expansion only)
contains_node(needle::ForEach, x::ForEach) = x === needle
contains_node(needle::ForEach, s::Step) = step_contains(needle, s)
contains_node(needle::ForEach, n::AbstractNode) = any(c -> contains_node(needle, c), node_children(n))
step_contains(needle::ForEach, s::Step{N}) where {N<:AbstractNode} = s.work === needle || contains_node(needle, s.work)
step_contains(::ForEach, ::Step) = false
node_children(n::Sequence) = collect(n.nodes)
node_children(n::Parallel) = collect(n.nodes)
node_children(n::Retry) = [n.node]
node_children(n::Fallback) = [n.primary, n.fallback]
node_children(n::Branch) = [n.if_true, n.if_false]
node_children(n::Timeout) = [n.node]
node_children(n::Force) = [n.node]
node_children(n::Reduce) = [n.node]
node_children(p::Pipe) = [p.first, p.second]
node_children(sip::SameInputPipe) = [sip.first, sip.second]
node_children(bp::BroadcastPipe) = [bp.first, bp.second]
node_children(::Union{Step,ForEach}) = AbstractNode[]

function run_node(pipe::Pipe, v, forced::Bool, context_input=nothing)
    r1 = run_node(pipe.first, v, forced, context_input)
    any(!r.success for r in r1) && return r1
    out = length(r1) == 1 ? r1[1].result : [r.result for r in r1 if r.success]
    r2 = run_node(pipe.second, v, forced, out)
    vcat(r1, r2)
end

function run_node(sip::SameInputPipe, v, forced::Bool, context_input=nothing)
    r1 = run_node(sip.first, v, forced, context_input)
    any(!r.success for r in r1) && return r1
    r2 = run_node(sip.second, v, forced, context_input)
    vcat(r1, r2)
end

# BroadcastPipe(ForEach, step): expand to one (branch >> step) per item, run via shared parallel helper
function run_node(bp::BroadcastPipe{<:ForEach, <:AbstractNode}, v, forced::Bool, context_input=nothing)
    fe = bp.first
    step = bp.second
    if fe.source isa String
        pattern = fe.source
        regex = for_each_regex(pattern)
        matches = find_matches(pattern, regex)
        isempty(matches) && error("ForEach: no files match '$pattern'")
        nodes = AbstractNode[]
        for c in matches
            r = length(c) == 1 ? fe.f(c[1]) : fe.f(c...)
            r === nothing && error("ForEach block must return a Step or node, not nothing.")
            push!(nodes, as_node(r) >> step)
        end
        contexts = [length(matches[i]) == 1 ? matches[i][1] : matches[i] for i in 1:length(nodes)]
        return run_node(ParallelBranches(nodes, contexts), v, forced, nothing)
    else
        items = collect(fe.source)
        nodes = [as_node(fe.f(item)) >> step for item in items]
        return run_node(ParallelBranches(nodes, items), v, forced, nothing)
    end
end
function run_node(bp::BroadcastPipe{<:Parallel, <:AbstractNode}, v, forced::Bool, context_input=nothing)
    run_node(Parallel([n >> bp.second for n in bp.first.nodes]...), v, forced, context_input)
end
function run_node(bp::BroadcastPipe, v, forced::Bool, context_input=nothing)
    run_node(bp.first >> bp.second, v, forced, context_input)
end

function run_node(fe::ForEach{F, String}, v, forced::Bool, context_input=nothing) where F
    pattern = fe.source
    regex = for_each_regex(pattern)
    matches = find_matches(pattern, regex)
    isempty(matches) && error("ForEach: no files match '$pattern'")
    nodes = AbstractNode[]
    for c in matches
        r = length(c) == 1 ? fe.f(c[1]) : fe.f(c...)
        r === nothing && error("ForEach block must return a Step or node, not nothing.")
        node = as_node(r)
        contains_node(fe, node) && error("Pipeline cycle: ForEach block returned a node containing this ForEach.")
        push!(nodes, node)
    end
    contexts = [length(matches[i]) == 1 ? matches[i][1] : matches[i] for i in 1:length(nodes)]
    run_node(ParallelBranches(nodes, contexts), v, forced, nothing)
end

function run_node(fe::ForEach{F, Vector{T}}, v, forced::Bool, context_input=nothing) where {F, T}
    nodes = AbstractNode[]
    items = collect(fe.source)
    for item in items
        node = as_node(fe.f(item))
        contains_node(fe, node) && error("Pipeline cycle: ForEach block returned a node containing this ForEach.")
        push!(nodes, node)
    end
    run_node(ParallelBranches(nodes, items), v, forced, nothing)
end

"""
    steps(node) -> Vector{Step}

Return all leaf steps in the DAG (flattened).
"""
steps(s::Step) = [s]
steps(s::Sequence) = reduce(vcat, steps.(s.nodes))
steps(p::Parallel) = reduce(vcat, steps.(p.nodes))
steps(r::Retry) = steps(r.node)
steps(f::Fallback) = vcat(steps(f.primary), steps(f.fallback))
steps(b::Branch) = vcat(steps(b.if_true), steps(b.if_false))
steps(t::Timeout) = steps(t.node)
steps(r::Reduce) = steps(r.node)
steps(f::Force) = steps(f.node)
steps(p::Pipe) = vcat(steps(p.first), steps(p.second))
steps(sip::SameInputPipe) = vcat(steps(sip.first), steps(sip.second))
steps(bp::BroadcastPipe) = vcat(steps(bp.first), steps(bp.second))
steps(::ForEach) = Step[]  # lazy: not discovered until run

"""
    count_steps(node) -> Int

Return the number of steps in the DAG (leaf count for execution).
"""
count_steps(::Step) = 1
count_steps(s::Sequence) = sum(count_steps, s.nodes)
count_steps(p::Parallel) = sum(count_steps, p.nodes)
count_steps(r::Retry) = count_steps(r.node)
count_steps(f::Fallback) = count_steps(f.primary) + count_steps(f.fallback)
count_steps(b::Branch) = max(count_steps(b.if_true), count_steps(b.if_false))
count_steps(t::Timeout) = count_steps(t.node)
count_steps(r::Reduce) = count_steps(r.node) + 1
count_steps(f::Force) = count_steps(f.node)
count_steps(p::Pipe) = count_steps(p.first) + count_steps(p.second)
count_steps(sip::SameInputPipe) = count_steps(sip.first) + count_steps(sip.second)
count_steps(bp::BroadcastPipe) = count_steps(bp.first) + count_steps(bp.second)
count_steps(::ForEach) = 0  # lazy: not discovered until run
