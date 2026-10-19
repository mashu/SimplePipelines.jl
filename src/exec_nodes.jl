# Executing composite and lazy nodes: sequences, parallel branches, retries, fallbacks,
# branches, timeouts, ForEach expansion and workflows.

@nospecialize   # see runtime.jl

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
    condition = run_safely(Call(call_with, (b.condition, arguments(input))); interrupted=interrupt_all!)
    condition.ok || return node_failed!(rt, :branch, "branch condition", condition.value)
    take_branch(condition.value, b, rt, scope, input)
end

function take_branch(choice::Bool, b::Branch, rt::Runtime, scope::Scope, input)
    log_branch(rt, choice)
    run_node(choice ? b.if_true : b.if_false, rt, scope, input)
end
take_branch(choice, _, rt::Runtime, _, _) = node_failed!(rt, :branch, "branch condition",
    StepFailure(:exception, "a Branch condition must return a Bool, got $(summary(choice))"))

const ABANDON_AFTER = 6.0     # seconds an expired Timeout waits for its node: the guard's TERM grace + 1

# At the deadline the node's commands are terminated (the guard also enforces the
# deadline on its own) and its steps that have not started never start. The node then
# gets a moment to report its stopped steps; Julia code still running after that is
# abandoned.
function exec(t::Timeout, rt::Runtime, scope::Scope, input)
    rt.planning && return run_node(t.node, rt, scope, input)
    watchdog = Watchdog(scope.watchdog, t.seconds)
    inner = Scope(scope.forced, scope.resources, watchdog, scope.priority, scope.tag)
    first_done = Channel{Tuple{Bool,Outcome}}(2)
    task = Threads.@spawn put!(first_done, (false, run_node(t.node, rt, inner, input)))
    bind(first_done, task)          # a Ctrl-C inside the node reaches take! as an exception
    timed_out, out = wait_first(first_done, t.seconds, () -> expire!(watchdog, rt))
    timed_out || return out
    wait_first(first_done, ABANDON_AFTER, () -> nothing)
    node_failed!(rt, :timeout, "timeout", StepFailure(:timed_out, "did not finish within $(t.seconds) s"))
end

"""The node's (false, outcome), or (true, FAILED) after `seconds`, when `on_expiry` runs first."""
function wait_first(first_done::Channel{Tuple{Bool,Outcome}}, seconds::Float64, on_expiry)
    timer = Timer(_ -> (on_expiry(); run_safely(Call(put!, (first_done, (true, FAILED))))), seconds)
    first = take!(first_done)
    close(timer)
    first
end

"""Terminate the commands running under `w` and refuse the steps waiting under it."""
function expire!(w::Watchdog, rt::Runtime)
    foreach(id -> terminate!(rt.link, id, :node_timeout), mark_expired!(w))
    refuse!(rt.pool, watchdog_expired, TIMEOUT_REFUSAL)
end

exec(f::Force, rt::Runtime, s::Scope, input) =
    run_node(f.node, rt, Scope(true, s.resources, s.watchdog, s.priority, s.tag), input)

exec(r::Resourced, rt::Runtime, s::Scope, input) =
    run_node(r.node, rt, Scope(s.forced, r.resources, s.watchdog, s.priority, s.tag), input)

#==============================================================================#
# Lazy nodes: expanded when they run
#==============================================================================#

function exec(fe::ForEach, rt::Runtime, scope::Scope, _)
    expanded = run_safely(Call(expand_foreach, (fe,)); interrupted=interrupt_all!)
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
    planned = run_safely(Call(plan_workflow, (wf, with_lock(copy, rt.lock, rt.pending))); interrupted=interrupt_all!)
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
