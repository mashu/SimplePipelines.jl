# Executing one step: skip it when up to date, work out what it needs, wait for
# admission, run it, and record the result. A step stopped to free memory (evicted)
# is not a failure: it is queued again with a larger charge, and its last attempt runs
# alone.

@nospecialize   # see runtime.jl

const MAX_ATTEMPTS = 3      # attempts of a step that keeps being evicted

flow_value(step::Step, value) = isempty(step.outputs) ? value : step.outputs

"""
The resources of `step` in this run. A declared or inherited `mem_mb` larger than the
run's budget is clamped to it, so `{mem_mb}`, the kill limit and the charge agree.
"""
function need(step::Step, scope::Scope, pool::Pool)
    own, default = step.resources, scope.resources
    threads = min(own.threads > 0 ? own.threads : max(default.threads, 1), pool.jobs)
    declared, source = own.mem_mb > 0 ? (own.mem_mb, :declared) :
                       default.mem_mb > 0 ? (default.mem_mb, :scope) : (0, :none)
    limit = pool.budget_mb > 0 ? min(declared, pool.budget_mb) : declared
    timeout = own.timeout > 0 ? own.timeout : default.timeout
    Need(threads, limit > 0 ? limit : UNDECLARED_CHARGE_MB, limit, declared,
         limit < declared ? :clamped : source, timeout, false)
end

function exec(step::Step, rt::Runtime, scope::Scope, input)
    label = step_label(step, scope.tag)
    reason = with_lock(staleness, rt.lock, step, rt.state, scope.forced, rt.pending)
    reason === nothing && return skip!(rt, step, label)
    n = need(step, scope, rt.pool)
    n.source === :clamped && log_clamped(rt, label, n, rt.pool.budget_mb)
    rt.planning && return plan!(rt, step, label, reason, n)
    attempt!(rt, step, label, scope, input, n, 1)
end

attempt!(rt::Runtime, step::Step, label::String, scope::Scope, input, n::Need, k::Int) =
    admitted(acquire!(rt.pool, scope.priority, n, scope.watchdog), rt, step, label, scope, input, n, k)

admitted(r::Refusal, rt::Runtime, step::Step, label::String, _...) = fail!(rt, step, label, r.failure, 0.0)

function admitted(g::Grant, rt::Runtime, step::Step, label::String, scope::Scope, input, n::Need, k::Int)
    in_process(step.work) && n.limit_mb > 0 && first_note!(rt, :function_memory) && log_function_memory(rt)
    started = time()
    out = unwrap(run_safely(Call(perform, (step, Launch(label, n, g, scope.watchdog, rt), input));
                            interrupted=interrupt_all!))
    release!(rt.pool, g)                    # before finish!, which does I/O
    conclude(out, rt, step, label, scope, input, n, k, started, g.peak_mb)
end

in_process(::Union{Function,Call}) = true
in_process(_) = false

# perform returns an Outcome (or Evicted), which run_safely wraps in another one.
unwrap(out::Outcome) = out.ok ? out.value : out

conclude(out::Outcome, rt, step, label, scope, _, _, _, started, peak) =
    finished(rt, step, label, scope, out, started, peak)
conclude(e::Evicted, rt, step, label, scope, input, n, k, started, _) =
    e.alone || k >= MAX_ATTEMPTS ?
        finished(rt, step, label, scope, Outcome(false, out_of_memory(e)), started, e.peak_mb) :
        requeue(e, rt, step, label, scope, input, n, k, started)

"""Queue an evicted step again, charged for what it really used; the last attempt runs alone."""
function requeue(e::Evicted, rt::Runtime, step::Step, label::String, scope::Scope, input, n::Need, k::Int, started)
    discard_outputs(step, started)
    charge = n.limit_mb > 0 ? n.limit_mb : max(cld(3n.charge_mb, 2), cld(6e.peak_mb, 5))
    next = Need(n.threads, charge, n.limit_mb, n.declared_mb, n.source, n.timeout, k + 1 == MAX_ATTEMPTS)
    log_evicted(rt, label, e, next, k + 1)
    attempt!(rt, step, label, scope, input, next, k + 1)
end

out_of_memory(e::Evicted) = StepFailure(:out_of_memory,
    "the computer ran out of memory while this step ran $(e.alone ? "alone" : "on its last attempt"): it used " *
    "$(e.usage_mb) MiB and only $(e.available_mb) MiB were left ($(e.reserve_mb) MiB must stay free); it needs more " *
    "memory than is free — close other programs or run it on a larger machine")

function finished(rt::Runtime, step::Step, label::String, scope::Scope, out::Outcome, started, peak)
    done = finish!(rt, step, label, out, started, peak)
    done.ok ? continue_with(done.value, step, rt, scope) : done
end

function skip!(rt::Runtime, step::Step, label::String)
    with_lock(adopt!, rt.lock, rt.state, step)
    record!(rt, StepResult(step, label, :skipped, 0.0, nothing))
    succeeded(step.outputs)
end

function plan!(rt::Runtime, step::Step, label::String, reason::String, n::Need)
    rendered = run_safely(Call(launch_work, (step.work, n)))
    rendered.ok || return fail!(rt, step, label, rendered.value, 0.0)
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

function fail!(rt::Runtime, step::Step, label::String, failure::StepFailure, duration::Float64,
               peak::Union{Nothing,Int}=nothing)
    with_lock(forget!, rt.lock, rt.state, step)
    record!(rt, StepResult(step, label, :failed, duration, failure, peak))
    FAILED
end

"""Record the outcome of a step that ran; failed steps lose the outputs they wrote."""
function finish!(rt::Runtime, step::Step, label::String, out::Outcome, started::Float64, peak::Union{Nothing,Int})
    duration = time() - started
    out.ok || return fail!(rt, step, label, with_removed(out.value, discard_outputs(step, started)), duration, peak)
    with_lock(remember!, rt.lock, rt.state, step)
    record!(rt, StepResult(step, label, :done, duration, out.value, peak))
    out
end

with_removed(f::StepFailure, removed::Vector{String}) = isempty(removed) ? f :
    StepFailure(f.kind, f.message; detail=join(filter(!isempty, [f.detail; "removed incomplete output: " .* removed]), "\n"))

# A function step that returns a node runs that node next (a dynamic sub-pipeline).
continue_with(node::AbstractNode, ::Step, rt::Runtime, scope::Scope) = run_node(node, rt, scope, nothing)
continue_with(value, step::Step, ::Runtime, ::Scope) = succeeded(flow_value(step, value))

@specialize
