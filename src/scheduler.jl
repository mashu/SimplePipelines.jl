# Admission control. A step starts only when its threads fit the run's `jobs` CPU
# slots, its memory charge fits the run's budget and, with the resource guard, the
# memory that is actually free. Waiting steps are admitted in priority order: the
# priority of a step is its position in the DAG, which makes the scheduler finish one
# branch (sample) before starting the next instead of running every first step first.
# A step that does not fit blocks the ones behind it, so large steps are never starved.

@nospecialize   # see runtime.jl

"""
Call `f(args...)` holding `l`. Locked sections only touch in-memory state and do not
block, so they defer Ctrl-C instead of needing a `finally`: an interrupt is delivered
after the lock is released. Named functions (not closures) keep this compiled once.
"""
function with_lock(f, l::ReentrantLock, args...)
    lock(l)
    Base.sigatomic_begin()
    result = f(args...)
    unlock(l)
    Base.sigatomic_end()
    result
end

const UNDECLARED_CHARGE_MB = 256    # admission charge of a step that declares no mem_mb
const STALE_SAMPLE = 5.0            # seconds without a memory sample before admission pauses
const MEMORY_WAIT_LIMIT = 120.0     # seconds paused with nothing running before waiters are refused
const NOTICE_INTERVAL = 30.0        # seconds between repeated "waiting for memory" lines

"""
What a step needs, computed when it is about to start. `limit_mb` is the one memory
number of the step: the guard's kill limit and the value of `{mem_mb}` (0: none).
`charge_mb` is what admission reserves for it.
"""
struct Need
    threads::Int
    charge_mb::Int
    limit_mb::Int
    declared_mb::Int                # before clamping to the budget, for messages
    source::Symbol                  # :declared, :scope (with_resources), :clamped or :none
    timeout::Float64                # seconds; 0 = none
    exclusive::Bool                 # run with nothing else (last attempt after evictions)
end

const GRANT_IDS = Threads.Atomic{Int}(0)

"""
Resources held by a running step. The id is unique in this Julia process and is also
the step's id in the resource guard.
"""
mutable struct Grant
    const id::Int
    const threads::Int
    const charge_mb::Int
    const exclusive::Bool
    usage_mb::Int                   # last measured memory use of its command (0: none)
    peak_mb::Union{Nothing,Int}     # peak memory of its command; nothing when not measured
end

"""Admission refused: the run was interrupted or ended, a Timeout expired, or memory never became free."""
struct Refusal
    failure::StepFailure
end

#==============================================================================#
# Watchdog: the deadline of a Timeout node and the commands running under it.
# Watchdogs nest; a command is registered with every enclosing one.
#==============================================================================#

mutable struct Watchdog
    const lock::ReentrantLock
    @atomic expired::Bool
    const deadline::Float64         # absolute time(); Inf at the root
    const seconds::Float64
    const steps::Vector{Int}        # grant ids of commands running under it
    const parent::Union{Nothing,Watchdog}
end
Watchdog() = Watchdog(ReentrantLock(), false, Inf, 0.0, Int[], nothing)
Watchdog(parent::Watchdog, seconds::Float64) =
    Watchdog(ReentrantLock(), false, time() + seconds, seconds, Int[], parent)

expired(::Nothing) = false
expired(w::Watchdog) = (@atomic w.expired) || expired(w.parent)

"""The Timeout with the earliest deadline among `w` and its parents (`nothing` at the root)."""
tightest(::Nothing) = nothing
function tightest(w::Watchdog)
    outer = tightest(w.parent)
    w.deadline < Inf && (outer === nothing || w.deadline <= outer.deadline) ? w : outer
end

"""Milliseconds until the earliest enclosing deadline; 0 when there is none."""
remaining_ms(w::Watchdog) = milliseconds_left(tightest(w))
milliseconds_left(::Nothing) = 0
milliseconds_left(t::Watchdog) = max(1, round(Int, (t.deadline - time()) * 1000))

"""Register command `id` with `w` and its parents; true when one of them has already expired."""
watch!(::Nothing, ::Int) = false
watch!(w::Watchdog, id::Int) = with_lock(add_step!, w.lock, w, id) | watch!(w.parent, id)
add_step!(w::Watchdog, id::Int) = (push!(w.steps, id); @atomic w.expired)

unwatch!(::Nothing, ::Int) = nothing
unwatch!(w::Watchdog, id::Int) = (with_lock(remove_step!, w.lock, w, id); unwatch!(w.parent, id))
remove_step!(w::Watchdog, id::Int) = (filter!(!=(id), w.steps); nothing)

"""Mark `w` expired and return the commands that were running under it."""
mark_expired!(w::Watchdog) = with_lock(expire_steps!, w.lock, w)
expire_steps!(w::Watchdog) = (@atomic w.expired = true; copy(w.steps))

#==============================================================================#
# Pool
#==============================================================================#

mutable struct Waiter
    const priority::Vector{Int}
    const need::Need
    const watchdog::Watchdog
    const ready::Threads.Condition
    outcome::Union{Nothing,Grant,Refusal}
end

"""The memory picture the guard reports every tick (MiB)."""
struct GuardSample
    available_mb::Int
    reserve_mb::Int                 # min_free_mb in effect
    paused::Bool                    # the guard says: start nothing new
    level::Symbol                   # :ok, :tight, :low, :pressure or :critical
    usage::Dict{Int,Int}            # grant id => memory use of its command
end
GuardSample() = GuardSample(0, 0, false, :ok, Dict{Int,Int}())

mutable struct Pool
    const lock::ReentrantLock
    const jobs::Int
    const budget_mb::Int            # limit on the sum of charges; 0 = none
    const gated::Bool               # admission follows measured memory (guard measures memory)
    used_jobs::Int
    promised_mb::Int                # sum of charges of held grants
    const held::Vector{Grant}
    exclusive::Bool
    const queue::Vector{Waiter}     # sorted by priority, FIFO among equals
    available_mb::Int
    reserve_mb::Int
    paused::Bool
    sampled_at::Float64
    starved_since::Float64          # paused with waiters and no command running since; 0 = not
    noticed_at::Float64             # last "waiting for memory" line; 0 = not shown
    refusal::Union{Nothing,StepFailure}   # set when the run stops: refuse everything
end

function Pool(jobs::Integer, budget_mb::Integer, gated::Bool, s::GuardSample=GuardSample())
    jobs >= 1 || throw(ArgumentError("jobs must be at least 1, got $jobs"))
    budget_mb >= 0 || throw(ArgumentError("mem_mb must be ≥ 0, got $budget_mb"))
    Pool(ReentrantLock(), Int(jobs), Int(budget_mb), gated, 0, 0, Grant[], false, Waiter[],
         s.available_mb, s.reserve_mb, s.paused, time(), 0.0, 0.0, nothing)
end

idle(pool::Pool) = isempty(pool.held)

# Admission. A pool with nothing running admits the head waiter even when it exceeds
# the budget or the headroom (otherwise it could never run), but never while paused.
fits(pool::Pool, n::Need, now::Float64) =
    !pool.exclusive && (n.exclusive ? idle(pool) && memory_ok(pool, n, now) :
        pool.used_jobs + n.threads <= pool.jobs &&
        (idle(pool) || pool.budget_mb == 0 || pool.promised_mb + n.charge_mb <= pool.budget_mb) &&
        memory_ok(pool, n, now))

memory_ok(pool::Pool, n::Need, now::Float64) =
    !pool.gated || (!paused(pool, now) && (idle(pool) || n.charge_mb <= headroom(pool)))

paused(pool::Pool, now::Float64) = pool.paused || now - pool.sampled_at > STALE_SAMPLE

"""Free memory not yet promised: what is available, minus twice the reserve, minus the
part of running steps' charges they have not used yet."""
headroom(pool::Pool) =
    pool.available_mb - 2pool.reserve_mb - sum(g -> max(0, g.charge_mb - g.usage_mb), pool.held; init=0)

function occupy!(pool::Pool, n::Need)
    g = Grant(Threads.atomic_add!(GRANT_IDS, 1) + 1, n.threads, n.charge_mb, n.exclusive, 0, nothing)
    pool.used_jobs += g.threads
    pool.promised_mb += g.charge_mb
    pool.exclusive |= g.exclusive
    push!(pool.held, g)
    g
end

# Admit waiters from the head of the queue while they fit. Caller holds the lock.
function dispatch!(pool::Pool, now::Float64)
    while !isempty(pool.queue)
        w = first(pool.queue)
        fits(pool, w.need, now) || break
        popfirst!(pool.queue)
        w.outcome = occupy!(pool, w.need)
        notify(w.ready)
    end
end

"""Wait until `n` is admitted: a Grant, or a Refusal when the run stops or `watchdog` expires."""
function acquire!(pool::Pool, priority::Vector{Int}, n::Need, watchdog::Watchdog)
    expired(watchdog) && return Refusal(TIMEOUT_REFUSAL)
    w = Waiter(priority, n, watchdog, Threads.Condition(pool.lock), nothing)
    # Base `lock(f, l)`: an interrupt while waiting must not leave the pool locked.
    granted(lock(() -> wait_turn!(pool, w), pool.lock), pool, watchdog)
end

function wait_turn!(pool::Pool, w::Waiter)
    pool.refusal === nothing || return Refusal(pool.refusal)
    insert!(pool.queue, searchsortedlast(pool.queue, w; by=x -> x.priority) + 1, w)
    dispatch!(pool, time())
    while w.outcome === nothing
        wait(w.ready)
    end
    w.outcome
end

const TIMEOUT_REFUSAL = StepFailure(:timed_out, "not started: the enclosing Timeout expired")

# A Timeout may expire while its step waits for the lock after being admitted.
granted(r::Refusal, ::Pool, ::Watchdog) = r
granted(g::Grant, pool::Pool, watchdog::Watchdog) =
    expired(watchdog) ? (release!(pool, g); Refusal(TIMEOUT_REFUSAL)) : g

release!(pool::Pool, g::Grant) = (with_lock(give_back!, pool.lock, pool, g); nothing)
function give_back!(pool::Pool, g::Grant)
    i = findfirst(x -> x === g, pool.held)
    i === nothing && return
    deleteat!(pool.held, i)
    pool.used_jobs -= g.threads
    pool.promised_mb -= g.charge_mb
    g.exclusive && (pool.exclusive = false)
    dispatch!(pool, time())
end

"""Refuse every waiter for which `pred(waiter)` holds."""
refuse!(pool::Pool, pred, failure::StepFailure) = (with_lock(refuse_waiters!, pool.lock, pool, pred, failure); nothing)
function refuse_waiters!(pool::Pool, pred, failure::StepFailure)
    for w in pool.queue
        pred(w) || continue
        w.outcome = Refusal(failure)
        notify(w.ready)
    end
    filter!(w -> w.outcome === nothing, pool.queue)
end

"""Refuse every waiter now and every step that asks later."""
close_pool!(pool::Pool, failure::StepFailure) = (with_lock(shut!, pool.lock, pool, failure); nothing)
function shut!(pool::Pool, failure::StepFailure)
    pool.refusal === nothing && (pool.refusal = failure)
    refuse_waiters!(pool, Returns(true), failure)
end

watchdog_expired(w::Waiter) = expired(w.watchdog)

"""
Apply a memory sample from the guard, then admit what now fits. Returns true when the
run should print a "waiting for memory" line (never printed under the pool lock).
"""
update_memory!(pool::Pool, s::GuardSample, now::Float64) =
    with_lock(apply_sample!, pool.lock, pool, s, now)

function apply_sample!(pool::Pool, s::GuardSample, now::Float64)
    pool.available_mb, pool.reserve_mb, pool.paused, pool.sampled_at = s.available_mb, s.reserve_mb, s.paused, now
    for g in pool.held
        g.usage_mb = get(s.usage, g.id, 0)
    end
    stuck = pool.gated && s.paused && !isempty(pool.queue) && !any(g -> haskey(s.usage, g.id), pool.held)
    pool.starved_since = stuck ? (pool.starved_since == 0 ? now : pool.starved_since) : 0.0
    stuck && now - pool.starved_since >= MEMORY_WAIT_LIMIT &&
        refuse_waiters!(pool, Returns(true), memory_refusal(pool))
    dispatch!(pool, now)
    notice = pool.gated && s.paused && !isempty(pool.queue) && now - pool.noticed_at >= NOTICE_INTERVAL
    notice && (pool.noticed_at = now)
    s.paused || (pool.noticed_at = 0.0)
    notice
end

memory_refusal(pool::Pool) = StepFailure(:out_of_memory,
    "not started: less than $(2pool.reserve_mb) MiB of memory was free for $(round(Int, MEMORY_WAIT_LIMIT)) s " *
    "while no other step was running (available $(pool.available_mb) MiB); other programs are using the " *
    "memory. Free memory or lower min_free_mb.")

@specialize
