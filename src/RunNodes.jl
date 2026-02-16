# run_node dispatch, contains_node, node_children, steps, count_steps. Requires Types, State, Execute, Logging, ForEachMap.

# Step wrapping another node (e.g. @step name = a >> b): delegate when work is AbstractNode
function run_node(step::Step{N}, v, forced::Bool=false) where {N<:AbstractNode}
    log_start(v, step)
    run_node(step.work, v, forced)
end

# Leaf step execution (Cmd, Function, Nothing): single path, dispatch in execute(step)
function run_node(step::Step{F}, v, forced::Bool=false) where F
    if !forced && is_fresh(step)
        log_skip(v, step)
        return [StepResult(step, true, 0.0, step.inputs, "up to date (not re-run)")]
    end
    log_start(v, step)
    result = execute(step)
    log_result(v, result)
    result.success && mark_complete!(step)
    [result]
end

function run_node(seq::Sequence, v, forced::Bool)
    results = AbstractStepResult[]
    for node in seq.nodes
        node_results = run_node(node, v, forced)
        append!(results, node_results)
        any(r -> !r.success, node_results) && break
    end
    results
end

function run_node(par::Parallel, v, forced::Bool)
    n = length(par.nodes)
    log_parallel(v, n)
    max_p = MAX_PARALLEL[]
    if max_p > 0 && n > 0
        results = AbstractStepResult[]
        for i in 1:max_p:n
            chunk = par.nodes[i:min(i + max_p - 1, end)]
            append!(results, reduce(vcat, fetch.([@spawn run_node(node, v, forced) for node in chunk])))
            log_progress(v, length(results), n)
        end
        results
    else
        reduce(vcat, fetch.([@spawn run_node(node, v, forced) for node in par.nodes]))
    end
end

function run_node(r::Retry, v, forced::Bool)
    local results::Vector{AbstractStepResult}
    for attempt in 1:r.max_attempts
        log_retry(v, attempt, r.max_attempts)
        results = run_node(r.node, v, forced)
        all(r -> r.success, results) && return results
        attempt < r.max_attempts && r.delay > 0 && sleep(r.delay)
    end
    results
end

function run_node(f::Fallback, v, forced::Bool)
    results = run_node(f.primary, v, forced)
    all(r -> r.success, results) && return results
    log_fallback(v)
    run_node(f.fallback, v, forced)
end

function run_node(b::Branch, v, forced::Bool)
    cond = b.condition()
    log_branch(v, cond)
    run_node(cond ? b.if_true : b.if_false, v, forced)
end

function run_node(t::Timeout, v, forced::Bool)
    log_timeout(v, t.seconds)
    ch = Channel{Vector{AbstractStepResult}}(1)
    @async put!(ch, run_node(t.node, v, forced))

    deadline = time() + t.seconds
    while !isready(ch) && time() < deadline
        sleep(0.01)
    end

    timeout_step = Step(:timeout, `true`)
    isready(ch) ? take!(ch) : [StepResult(timeout_step, false, t.seconds, timeout_step.inputs, "Timeout after $(t.seconds)s")]
end

function run_node(f::Force, v, ::Bool)
    log_force(v)
    run_node(f.node, v, true)  # Force always runs the inner node
end

function run_node(r::Reduce, v, forced::Bool)
    start = time()
    results = run_node(r.node, v, forced)
    outputs = [res.output for res in results if res.success]

    reduce_step = Step(r.name, r.reducer)
    if length(outputs) < length(results)
        return vcat(results, [StepResult(reduce_step, false, time() - start, reduce_step.inputs, "Reduce aborted: upstream failed")])
    end

    log_reduce(v, r.name)
    outcome = run_safely() do
        r.reducer(outputs)
    end
    if !outcome.ok
        return vcat(results, [StepResult(reduce_step, false, time() - start, reduce_step.inputs, "Reduce error: $(outcome.value)")])
    end
    vcat(results, [StepResult(reduce_step, true, time() - start, reduce_step.inputs, outcome.value)])
end

# Cycle check: does node contain needle? (ForEach/Map expansion only)
contains_node(needle::ForEach, x::ForEach) = x === needle
contains_node(needle::ForEach, s::Step) = step_contains(needle, s)
contains_node(needle::ForEach, n::AbstractNode) = any(c -> contains_node(needle, c), node_children(n))
contains_node(::ForEach, ::Map) = false
contains_node(needle::Map, x::Map) = x === needle
contains_node(needle::Map, s::Step) = step_contains(needle, s)
contains_node(needle::Map, n::AbstractNode) = any(c -> contains_node(needle, c), node_children(n))
step_contains(needle::Union{ForEach,Map}, s::Step{N}) where {N<:AbstractNode} = s.work === needle || contains_node(needle, s.work)
step_contains(::Union{ForEach,Map}, ::Step) = false
node_children(n::Sequence) = collect(n.nodes)
node_children(n::Parallel) = collect(n.nodes)
node_children(n::Retry) = [n.node]
node_children(n::Fallback) = [n.primary, n.fallback]
node_children(n::Branch) = [n.if_true, n.if_false]
node_children(n::Timeout) = [n.node]
node_children(n::Force) = [n.node]
node_children(n::Reduce) = [n.node]
node_children(::Union{Step,ForEach,Map}) = AbstractNode[]

function run_node(fe::ForEach, v, forced::Bool)
    regex = for_each_regex(fe.pattern)
    matches = find_matches(fe.pattern, regex)
    isempty(matches) && error("ForEach: no files match '$(fe.pattern)'")
    nodes = AbstractNode[]
    for c in matches
        r = length(c) == 1 ? fe.f(c[1]) : fe.f(c...)
        r === nothing && error("ForEach block must return a Step or node, not nothing.")
        node = as_node(r)
        contains_node(fe, node) && error("Pipeline cycle: ForEach block returned a node containing this ForEach.")
        push!(nodes, node)
    end
    n = length(nodes)
    log_parallel(v, n)
    max_p = MAX_PARALLEL[]
    if max_p > 0 && n > 0
        results = AbstractStepResult[]
        for i in 1:max_p:n
            chunk = nodes[i:min(i + max_p - 1, end)]
            append!(results, reduce(vcat, fetch.([@spawn run_node(node, v, forced) for node in chunk])))
            log_progress(v, length(results), n)
        end
        results
    else
        reduce(vcat, fetch.([@spawn run_node(n, v, forced) for n in nodes]))
    end
end

function run_node(m::Map, v, forced::Bool)
    nodes = AbstractNode[]
    for item in m.items
        node = as_node(m.f(item))
        contains_node(m, node) && error("Pipeline cycle: Map block returned a node containing this Map.")
        push!(nodes, node)
    end
    n = length(nodes)
    log_parallel(v, n)
    max_p = MAX_PARALLEL[]
    if max_p > 0 && n > 0
        results = AbstractStepResult[]
        for i in 1:max_p:n
            chunk = nodes[i:min(i + max_p - 1, end)]
            append!(results, reduce(vcat, fetch.([@spawn run_node(node, v, forced) for node in chunk])))
            log_progress(v, length(results), n)
        end
        results
    else
        length(nodes) == 1 ? run_node(nodes[1], v, forced) : reduce(vcat, fetch.([@spawn run_node(n, v, forced) for n in nodes]))
    end
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
steps(::ForEach) = Step[]  # lazy: not discovered until run
steps(::Map) = Step[]     # lazy: nodes built only when run

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
count_steps(::ForEach) = 0  # lazy: not discovered until run
count_steps(::Map) = 0      # lazy: nodes built only when run
