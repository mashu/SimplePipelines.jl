# Execution. `run_node` memoizes, `exec` dispatches on the node type. A node runs
# at most once per run for a given input; a step with declared outputs runs at most
# once per set of outputs, even when several distinct Step objects declare them.

# Arguments are not specialized: user functions and values have endless types, and
# compiling the runtime once keeps the first run fast.
@nospecialize

"""Settings inherited down the tree: force flag, default resources, watchdog, priority, log tag."""
struct Scope
    forced::Bool
    resources::Resources
    watchdog::Watchdog
    priority::Vector{Int}
    tag::String
end

branch(s::Scope, i::Int, tag::AbstractString) =
    Scope(s.forced, s.resources, s.watchdog, [s.priority; i], join_tags(s.tag, tag))
join_tags(a::AbstractString, b::AbstractString) = isempty(a) ? String(b) : isempty(b) ? String(a) : string(a, ",", b)

const Claim = Union{Outcome,Channel{Outcome}}

"""
Call `f(args...)` holding `l`. Locked sections only touch dictionaries and sets and
cannot throw, so no `finally` is needed; named functions (not closures) keep this
compiled once.
"""
function with_lock(f, l::ReentrantLock, args...)
    lock(l)
    result = f(args...)
    unlock(l)
    result
end

"""Everything one call of `run` or `check` shares."""
struct Runtime
    verbose::Bool
    planning::Bool                       # `check`: decide what would run, run nothing
    pool::Pool
    root::Watchdog
    lock::ReentrantLock                  # memo, results, state, pending
    memo::IdDict{Any,Claim}
    results::Vector{StepResult}
    state::State
    pending::Set{String}                 # outputs that planned steps will rebuild
    log_lock::ReentrantLock
end

Runtime(; verbose::Bool, planning::Bool, jobs::Integer, mem_mb::Integer, state_path::AbstractString) =
    Runtime(verbose, planning, Pool(jobs, mem_mb), Watchdog(), ReentrantLock(), IdDict{Any,Claim}(),
            StepResult[], State(state_path), Set{String}(), ReentrantLock())

#==============================================================================#
# Memoization
#==============================================================================#

memo_key(node::AbstractNode, input) = input === nothing ? node : (node, input)
memo_key(step::Step, input) = isempty(step.outputs) ? (input === nothing ? step : (step, input)) :
                                                      join(sort(step.outputs), '\n')

function run_node(node::AbstractNode, rt::Runtime, scope::Scope, input=nothing)
    key = memo_key(node, input)
    settle(with_lock(claim!, rt.lock, rt.memo, key), node, rt, scope, input, key)
end

# The first visitor gets `nothing` and must execute; later ones get the result or wait.
claim!(memo, key) = haskey(memo, key) ? memo[key] : (memo[key] = Channel{Outcome}(1); nothing)

settle(done::Outcome, _...) = done
settle(running::Channel{Outcome}, _...) = fetch(running)
function settle(::Nothing, node, rt::Runtime, scope::Scope, input, key)
    out = exec(node, rt, scope, input)
    with_lock(publish!, rt.lock, rt.memo, key, out)
    out
end

function publish!(memo, key, out::Outcome)
    waiting = memo[key]
    memo[key] = out
    put!(waiting, out)
end

"""Forget memoized failures so a Retry attempt runs them again."""
forget_failures!(rt::Runtime) = (with_lock(filter!, rt.lock, live_claim, rt.memo); nothing)
live_claim(entry::Pair) = live_claim(entry.second)
live_claim(o::Outcome) = o.ok
live_claim(::Channel) = true

#==============================================================================#
# Recording
#==============================================================================#

function record!(rt::Runtime, r::StepResult)
    with_lock(push!, rt.lock, rt.results, r)
    log_result(rt, r)
    r
end

"""Report a failure of a composite node (bad condition, ForEach block error, …)."""
node_failed!(rt::Runtime, name::Symbol, label::AbstractString, failure::StepFailure) =
    (record!(rt, StepResult(marker_step(name), String(label), :failed, 0.0, failure)); FAILED)

#==============================================================================#
# Steps
#==============================================================================#

flow_value(step::Step, value) = isempty(step.outputs) ? value : step.outputs

function demand(step::Step, scope::Scope)
    threads = step.resources.threads > 0 ? step.resources.threads : max(scope.resources.threads, 1)
    mem = step.resources.mem_mb > 0 ? step.resources.mem_mb : scope.resources.mem_mb
    threads, mem
end

function exec(step::Step, rt::Runtime, scope::Scope, input)
    label = step_label(step, scope.tag)
    reason = with_lock(staleness, rt.lock, step, rt.state, scope.forced, rt.pending)
    reason === nothing && return skip!(rt, step, label)
    rt.planning && return plan!(rt, step, label, reason)
    expired(scope.watchdog) && return fail!(rt, step, label,
        StepFailure(:timed_out, "not started: the enclosing Timeout expired"), 0.0)
    grant = acquire!(rt.pool, scope.priority, demand(step, scope)...)
    log_start(rt, step, label)
    started = time()
    outcome = unwrap(run_safely(Call(perform, (step, scope.watchdog, input))))
    out = finish!(rt, step, label, outcome, started)
    release!(rt.pool, grant)
    out.ok ? continue_with(out.value, step, rt, scope) : out
end

# perform returns an Outcome, which run_safely wraps in another one.
unwrap(out::Outcome) = out.ok ? out.value : out

function skip!(rt::Runtime, step::Step, label::String)
    with_lock(adopt!, rt.lock, rt.state, step)
    record!(rt, StepResult(step, label, :skipped, 0.0, nothing))
    succeeded(step.outputs)
end

function plan!(rt::Runtime, step::Step, label::String, reason::String)
    missing_input = with_lock(unavailable_input, rt.lock, step, rt.pending)
    missing_input === nothing || return fail!(rt, step, label, StepFailure(:missing_input,
        "missing input $missing_input, and no earlier step produces it"), 0.0)
    with_lock(union!, rt.lock, rt.pending, step.outputs)
    record!(rt, StepResult(step, label, :planned, 0.0, reason))
    succeeded(isempty(step.outputs) ? nothing : step.outputs)
end

function unavailable_input(step::Step, pending::Set{String})
    for i in step.inputs
        i in pending || ispath(i) || return i
    end
    nothing
end

function fail!(rt::Runtime, step::Step, label::String, failure::StepFailure, duration::Float64)
    with_lock(forget!, rt.lock, rt.state, step)
    record!(rt, StepResult(step, label, :failed, duration, failure))
    FAILED
end

"""Record the outcome of a step that ran; failed steps lose the outputs they wrote."""
function finish!(rt::Runtime, step::Step, label::String, out::Outcome, started::Float64)
    duration = time() - started
    out.ok || return fail!(rt, step, label, with_removed(out.value, discard_outputs(step, started)), duration)
    with_lock(remember!, rt.lock, rt.state, step)
    record!(rt, StepResult(step, label, :done, duration, out.value))
    out
end

with_removed(f::StepFailure, removed::Vector{String}) = isempty(removed) ? f :
    StepFailure(f.kind, f.message; detail=join(filter(!isempty, [f.detail; "removed incomplete output: " .* removed]), "\n"))

# A function step that returns a node runs that node next (a dynamic sub-pipeline).
continue_with(node::AbstractNode, ::Step, rt::Runtime, scope::Scope) = run_node(node, rt, scope, nothing)
continue_with(value, step::Step, ::Runtime, ::Scope) = succeeded(flow_value(step, value))

#==============================================================================#
# Composite nodes
#==============================================================================#

function exec(seq::Sequence, rt::Runtime, scope::Scope, input)
    value = input
    for node in seq.nodes
        out = run_node(node, rt, scope, value)
        out.ok || return out
        value = out.value
    end
    succeeded(value)
end

"""Run nodes concurrently (sequentially when planning); each branch gets its own priority."""
function run_branches(nodes::Vector{AbstractNode}, rt::Runtime, scope::Scope, input,
                      tags::Vector{String}=fill("", length(nodes)))
    n = length(nodes)
    if n > 1 && !rt.planning
        tasks = [Threads.@spawn run_node(nodes[i], rt, branch(scope, i, tags[i]), input) for i in 1:n]
        return Outcome[fetch(t) for t in tasks]
    end
    Outcome[run_node(nodes[i], rt, branch(scope, i, tags[i]), input) for i in 1:n]
end

combine(outs::Vector{Outcome}) = Outcome(all(o -> o.ok, outs), [o.value for o in outs])

exec(par::Parallel, rt::Runtime, scope::Scope, input) = combine(run_branches(par.nodes, rt, scope, input))

function exec(job::Job, rt::Runtime, scope::Scope, _)
    all(o -> o.ok, run_branches(job.deps, rt, scope, nothing)) || return FAILED
    run_node(job.step, rt, scope, nothing)
end

function exec(r::Retry, rt::Runtime, scope::Scope, input)
    out = run_node(r.node, rt, scope, input)
    for attempt in 2:r.max_attempts
        (out.ok || rt.planning) && return out
        log_retry(rt, attempt, r.max_attempts)
        r.delay > 0 && sleep(r.delay)
        forget_failures!(rt)
        out = run_node(r.node, rt, scope, input)
    end
    out
end

function exec(f::Fallback, rt::Runtime, scope::Scope, input)
    out = run_node(f.primary, rt, scope, input)
    (out.ok || rt.planning) && return out
    log_fallback(rt)
    run_node(f.fallback, rt, scope, input)
end

function exec(b::Branch, rt::Runtime, scope::Scope, input)
    rt.planning && return combine(run_branches(AbstractNode[b.if_true, b.if_false], rt, scope, input))
    condition = run_safely(Call(call_with, (b.condition, arguments(input))))
    condition.ok || return node_failed!(rt, :branch, "branch condition", condition.value)
    take_branch(condition.value, b, rt, scope, input)
end

function take_branch(choice::Bool, b::Branch, rt::Runtime, scope::Scope, input)
    log_branch(rt, choice)
    run_node(choice ? b.if_true : b.if_false, rt, scope, input)
end
take_branch(choice, _, rt::Runtime, _, _) = node_failed!(rt, :branch, "branch condition",
    StepFailure(:exception, "a Branch condition must return a Bool, got $(summary(choice))"))

function exec(t::Timeout, rt::Runtime, scope::Scope, input)
    rt.planning && return run_node(t.node, rt, scope, input)
    watchdog = Watchdog(scope.watchdog)
    inner = Scope(scope.forced, scope.resources, watchdog, scope.priority, scope.tag)
    first_done = Channel{Tuple{Bool,Outcome}}(2)
    Threads.@spawn put!(first_done, (false, run_node(t.node, rt, inner, input)))
    timer = Timer(_ -> (expire!(watchdog); put!(first_done, (true, FAILED))), t.seconds)
    timed_out, out = take!(first_done)
    close(timer)
    timed_out || return out
    node_failed!(rt, :timeout, "timeout", StepFailure(:timed_out, "did not finish within $(t.seconds) s"))
end

exec(f::Force, rt::Runtime, s::Scope, input) =
    run_node(f.node, rt, Scope(true, s.resources, s.watchdog, s.priority, s.tag), input)

exec(r::Resourced, rt::Runtime, s::Scope, input) =
    run_node(r.node, rt, Scope(s.forced, r.resources, s.watchdog, s.priority, s.tag), input)

#==============================================================================#
# Lazy nodes: expanded when they run
#==============================================================================#

function exec(fe::ForEach, rt::Runtime, scope::Scope, _)
    expanded = run_safely(() -> expand_foreach(fe))
    expanded.ok || return node_failed!(rt, :foreach, foreach_label(fe), expanded.value)
    nodes, tags = expanded.value
    isempty(nodes) && return no_branches(fe, rt)
    combine(run_branches(nodes, rt, scope, nothing, tags))
end

foreach_label(fe::ForEach{Pattern}) = "ForEach(\"$(fe.source.text)\")"
foreach_label(fe::ForEach) = "ForEach($(length(fe.source)) items)"

no_branches(fe::ForEach{Pattern}, rt::Runtime) = node_failed!(rt, :foreach, foreach_label(fe),
    StepFailure(:no_matches, "ForEach: no files match \"$(fe.source.text)\""))
no_branches(::ForEach, ::Runtime) = succeeded(Any[])

"""Branch nodes and log tags of a ForEach."""
function expand_foreach(fe::ForEach{Pattern})
    names = wildcard_names(fe.source)
    nodes, tags = AbstractNode[], String[]
    for (_, values) in glob(fe.source)
        push!(nodes, branch_node(fe, fe.f((values[n] for n in names)...)))
        push!(tags, join(("$n=$(values[n])" for n in names), ","))
    end
    nodes, tags
end

function expand_foreach(fe::ForEach)
    nodes = AbstractNode[branch_node(fe, fe.f(item)) for item in fe.source]
    nodes, String[item_tag(item, i) for (i, item) in enumerate(fe.source)]
end

item_tag(x::Union{AbstractString,Symbol,Number}, ::Int) = shorten(string(x), 30)
item_tag(_, i::Int) = "#$i"

function branch_node(fe::ForEach, x)
    node = as_node(x)
    contains_node(node, fe) && throw(ArgumentError("a ForEach block returned a node that contains the ForEach itself"))
    node
end

function exec(wf::Workflow, rt::Runtime, scope::Scope, _)
    planned = run_safely(Call(plan_workflow, (wf, with_lock(copy, rt.lock, rt.pending))))
    planned.ok || return node_failed!(rt, :workflow, "workflow $(wf.name)", planned.value)
    run_plan(planned.value, wf, rt, scope)
end

run_plan(u::Unresolved, wf::Workflow, rt::Runtime, _) =
    node_failed!(rt, :workflow, "workflow $(wf.name)", StepFailure(:unresolved, u.reason))
function run_plan(p::WorkflowPlan, wf::Workflow, rt::Runtime, scope::Scope)
    log_workflow(rt, wf, p)
    Outcome(run_node(p.node, rt, scope, nothing).ok, p.paths)
end

@specialize
