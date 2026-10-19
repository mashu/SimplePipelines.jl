# The state one call of `run` or `check` shares, its lifecycle (open, close, stop on
# interrupt or guard loss) and memoization. `run_node` memoizes; `exec` (exec_step.jl,
# exec_nodes.jl) dispatches on the node type. A node runs at most once per run for a
# given input; a step with declared outputs runs at most once per set of outputs, even
# when several distinct Step objects declare them.

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
const RUN_IDS = Threads.Atomic{Int}(0)

"""Everything one call of `run` or `check` shares."""
struct Runtime
    id::Int
    verbose::Bool
    planning::Bool                       # `check`: decide what would run, run nothing
    pool::Pool
    root::Watchdog
    link::Union{GuardLink,NoGuard}
    min_free_mb::Int
    nice::Int
    lock::ReentrantLock                  # memo, results, state, pending, notes
    memo::IdDict{Any,Claim}
    results::Vector{StepResult}
    state::State
    pending::Set{String}                 # outputs that planned steps will rebuild
    notes::Set{Symbol}                   # one-time messages already shown
    log_lock::ReentrantLock
    interrupted::Threads.Atomic{Bool}
end

function Runtime(; verbose::Bool, planning::Bool, jobs::Integer, mem_mb::Integer, min_free_mb::Integer,
                 nice::Integer, state_path::AbstractString, link::Union{GuardLink,NoGuard})
    min_free_mb >= 0 || throw(ArgumentError("min_free_mb must be ≥ 0 (0 = automatic), got $min_free_mb"))
    0 <= nice <= 19 || throw(ArgumentError("nice must be between 0 and 19, got $nice"))
    pool = Pool(jobs, mem_mb, measures(link), latest_sample(link))
    Runtime(Threads.atomic_add!(RUN_IDS, 1) + 1, verbose, planning, pool, Watchdog(), link, min_free_mb, nice,
            ReentrantLock(), IdDict{Any,Claim}(), StepResult[], State(state_path), Set{String}(), Set{Symbol}(),
            ReentrantLock(), Threads.Atomic{Bool}(false))
end

#==============================================================================#
# Lifecycle. Active runs receive memory samples from the guard and are stopped by
# Ctrl-C or a lost guard (link.jl).
#==============================================================================#

const ACTIVE_LOCK = ReentrantLock()
const ACTIVE = Runtime[]

active_runs() = with_lock(copy, ACTIVE_LOCK, ACTIVE)

function open_run!(rt::Runtime)
    with_lock(push!, ACTIVE_LOCK, ACTIVE, rt)
    begin_run!(rt.link, rt.id, rt.min_free_mb, rt.nice)
    rt.planning || log_protection(rt, rt.link)
end

"""The run returned: nothing more starts, and commands abandoned by a Timeout are terminated."""
function close_run!(rt::Runtime)
    close_pool!(rt.pool, StepFailure(:stopped, "not started: the run had already ended"))
    end_run!(rt.link, rt.id)
    with_lock(remove_run!, ACTIVE_LOCK, ACTIVE, rt)
    nothing
end
remove_run!(runs::Vector{Runtime}, rt::Runtime) = filter!(r -> r !== rt, runs)

"""Refuse every waiting step and wake every task waiting for another node's result."""
function stop_run!(rt::Runtime, failure::StepFailure)
    close_pool!(rt.pool, failure)
    with_lock(fail_claims!, rt.lock, rt.memo)
end

"""Save the recipes of the steps that finished; the file is written outside the lock."""
save_run_state!(rt::Runtime) = rt.planning || save_state(rt.state.path, with_lock(take_changes!, rt.lock, rt.state))

"""Memory sample from the guard: update the pools of the runs using `link`."""
function on_sample(link::GuardLink, s::GuardSample)
    @atomic link.sample = s
    now = time()
    for rt in active_runs()
        rt.link === link && update_memory!(rt.pool, s, now) && log_paused(rt, s)
    end
end

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
    done = run_safely(Call(exec, (node, rt, scope, input)); interrupted=interrupt_all!)
    out = done.ok ? done.value : internal_error!(rt, done.value)
    with_lock(publish!, rt.lock, rt.memo, key, out)
    out
end

publish!(memo, key, out::Outcome) = fill_claim!(memo[key], memo, key, out)
fill_claim!(waiting::Channel{Outcome}, memo, key, out::Outcome) = (memo[key] = out; put!(waiting, out))
fill_claim!(::Outcome, _, _, ::Outcome) = nothing     # already settled by stop_run!

fail_claims!(memo) = foreach(key -> fill_claim!(memo[key], memo, key, FAILED), collect(keys(memo)))

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

# An exception escaping `exec` is a bug in this package; report it instead of hanging.
internal_error!(rt::Runtime, f::StepFailure) = node_failed!(rt, :pipeline, "internal error",
    StepFailure(:exception, "internal error: " * f.message; detail=f.detail))

"""True the first time `note` is asked for in this run (for messages shown once)."""
first_note!(rt::Runtime, note::Symbol) = with_lock(add_new!, rt.lock, rt.notes, note)
add_new!(set::Set{Symbol}, x::Symbol) = x in set ? false : (push!(set, x); true)

@specialize
