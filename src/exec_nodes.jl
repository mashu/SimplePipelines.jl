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
        (out.ok || rt.planning || stopped(rt)) && return out
        log_retry(rt, attempt, r.max_attempts, retry_hint(r.node, rt))
        r.delay > 0 && sleep(r.delay)
        forget_failures!(rt)
        out = run_node(r.node, rt, scope, input)
    end
    out
end

"""A note for the retry line when the step's last attempt hit a limit that a retry does not raise."""
retry_hint(step::Step, rt::Runtime) = failure_hint(with_lock(last_result, rt.lock, rt.results, step))
retry_hint(::AbstractNode, ::Runtime) = ""
last_result(results::Vector{StepResult}, step::Step) = (i = findlast(r -> r.step === step, results); i === nothing ? nothing : results[i])
failure_hint(r::StepResult) = failure_hint(r.result)
failure_hint(f::StepFailure) = kind_hint(Val(f.kind))
failure_hint(_) = ""
kind_hint(::Val{:memory_limit}) = " with the same mem_mb; the last attempt exceeded it"
kind_hint(::Val{:timed_out}) = " with the same timeout; the last attempt exceeded it"
kind_hint(::Val) = ""

function exec(f::Fallback, rt::Runtime, scope::Scope, input)
    out = run_node(f.primary, rt, scope, input)
    (out.ok || rt.planning || stopped(rt)) && return out
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

const ABANDON_AFTER = 6.0     # seconds an expired Timeout waits for its commands: the guard's TERM grace + 1

# At the deadline the node's commands are terminated (the guard also enforces the
# deadline on its own) and its steps that have not started never start. The node then
# gets a moment to report its stopped commands; Julia code still running is abandoned,
# and the resources it holds are given back.
function exec(t::Timeout, rt::Runtime, scope::Scope, input)
    rt.planning && return run_node(t.node, rt, scope, input)
    watchdog = Watchdog(scope.watchdog, t.seconds)
    inner = Scope(scope.forced, scope.resources, watchdog, scope.priority, scope.tag)
    first_done = Channel{Tuple{Bool,Outcome}}(2)
    task = Threads.@spawn put!(first_done, (false, run_node(t.node, rt, inner, input)))
    bind(first_done, task)          # a Ctrl-C inside the node reaches take! as an exception
    timed_out, out = wait_first(first_done, t.seconds, () -> expire!(watchdog, rt))
    timed_out || return out
    timedwait(() -> isready(first_done) || !running_commands(watchdog), ABANDON_AFTER; pollint=0.05)
    foreach(g -> release!(rt.pool, g), abandon!(watchdog))
    node_failed!(rt, :timeout, "timeout", StepFailure(:timed_out, "did not finish within $(t.seconds) s"))
end

"""The node's (false, outcome), or (true, FAILED) after `seconds`, when `on_expiry` runs first."""
function wait_first(first_done::Channel{Tuple{Bool,Outcome}}, seconds::Float64, on_expiry)
    timer = Timer(_ -> run_safely(Call(expiry!, (on_expiry, first_done)); interrupted=interrupt_all!), seconds)
    first = take!(first_done)
    close(timer)
    first
end

expiry!(on_expiry, first_done::Channel{Tuple{Bool,Outcome}}) =
    (on_expiry(); run_safely(Call(put!, (first_done, (true, FAILED)))))     # closed if the node finished first

"""Terminate the commands running under `w` and refuse the steps waiting under it."""
function expire!(w::Watchdog, rt::Runtime)
    foreach(id -> terminate!(rt.link, id, :node_timeout), mark_expired!(w))
    refuse!(rt.pool, watchdog_expired, TIMEOUT_REFUSAL)
end

exec(f::Force, rt::Runtime, s::Scope, input) =
    run_node(f.node, rt, Scope(true, s.resources, s.watchdog, s.priority, s.tag), input)

exec(r::Resourced, rt::Runtime, s::Scope, input) =
    run_node(r.node, rt, Scope(s.forced, inherit(r.resources, s.resources), s.watchdog, s.priority, s.tag), input)

"""Defaults of a nested with_resources: what it sets, and the enclosing defaults for the rest."""
inherit(inner::Resources, outer::Resources) =
    Resources(pick(inner.threads, outer.threads), pick(inner.mem_mb, outer.mem_mb), pick(inner.timeout, outer.timeout))
pick(inner, outer) = inner > 0 ? inner : outer

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
