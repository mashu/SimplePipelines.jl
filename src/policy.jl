# The guard's enforcement policy as a pure function of one tick's measurements: which steps to
# terminate, how hard, and when to give up on the driver. `decide` does no I/O, so every rule is
# unit tested with hand-made views. Memory is in MiB, times are seconds on the guard's monotonic
# clock.

const VICTIM_FLOOR_MB = 64          # smaller steps are never memory victims
const DYING_WINDOW = 2.0            # a victim's memory counts as freed this long after its kill
const GROWTH_WINDOW = 2.0           # usage history used for the growth term
const LEFTOVER_SETTLE = 0.5         # leaders gone: let exiting children finish before acting
const WARN_INTERVAL = 30.0
const DRIVER_SIGINT_AFTER = 2.0     # of critical with nothing of ours left to kill
const DRIVER_SIGKILL_AFTER = 2.0    # after that SIGINT, if still critical
const PSI_CRITICAL = 20.0           # `full avg10`, on two consecutive samples
const MEMORY_GRACE = 1.0
const TIMEOUT_GRACE = 5.0
const LEFTOVER_GRACE = 1.0
const INTERRUPT_GRACE = 1.0

"""One tracked step as the policy sees it."""
struct StepView
    id::Int
    members::Int                  # live processes
    usage::Int                    # confirmed with PSS where §3.5 asks for it
    charge::Int                   # what the driver's pool promised it
    limit::Int                    # 0 = none
    started::Float64
    step_deadline::Float64        # Inf = none
    node_deadline::Float64        # Inf = none
    leaders_dead_since::Float64   # 0 = a leader is alive, or no session yet
    killed_at::Float64            # 0 = not terminating
    grace::Float64
    usage_at_kill::Int
end

"""Everything `decide` needs from one tick. Counters and times are the values from the previous
tick; `decide` returns their new values."""
struct PolicyView
    steps::Vector{StepView}
    memory::Bool                  # false: memory is not measured, only deadlines and leftovers apply
    available::Int                # A
    total::Int                    # T
    min_free::Int                 # R, shown in the warning
    reserve::Int                  # R_eff = R + growth term
    psi::Float64                  # PSI full avg10 this tick; -1 = unknown
    driver_growth::Int            # driver usage above its baseline
    paused::Bool
    low_count::Int                # consecutive samples with A' < R_eff
    psi_count::Int                # consecutive samples with PSI ≥ PSI_CRITICAL
    critical_since::Float64       # 0 = not critical
    driver_sigint_at::Float64     # 0 = not sent
    last_warn::Float64
end

"""Terminate step `id` because of `kind`: SIGTERM, then SIGKILL after `grace` (0 = SIGKILL now).
`alone` tells the driver the step was the only one running."""
struct Term
    id::Int
    kind::Symbol
    grace::Float64
    alone::Bool
end

struct PolicyResult
    terms::Vector{Term}
    kill_now::Vector{Int}         # terminating steps whose grace is over: SIGKILL again
    driver::Symbol                # :none, :sigint or :sigkill
    level::Symbol                 # :ok, :tight, :low, :pressure or :critical
    paused::Bool
    low_count::Int
    psi_count::Int
    critical_since::Float64
    driver_sigint_at::Float64
    last_warn::Float64
    warning::String               # "" = none
end

is_terminating(s::StepView) = s.killed_at > 0
has_members(s::StepView) = s.members > 0
is_finished(s::StepView) = s.leaders_dead_since > 0 && !has_members(s)

auto_min_free(total::Int) = clamp(round(Int, 0.05 * total), 256, 2048)

"""R_eff: the reserve plus the memory the pipeline could add in the next second at its recent
growth rate, capped at 10% of the total."""
effective_reserve(min_free::Int, growth::Float64, total::Int) =
    min_free + round(Int, clamp(growth, 0.0, 0.1 * total))

"""The largest positive growth rate (MiB/s) between consecutive `(time, usage)` samples."""
function growth_rate(history::Vector{Tuple{Float64,Int}})
    rate = 0.0
    for i in 2:length(history)
        dt = history[i][1] - history[i-1][1]
        dt > 0 && (rate = max(rate, (history[i][2] - history[i-1][2]) / dt))
    end
    rate
end

"""Memory of victims killed within DYING_WINDOW that still have members: as good as freed."""
dying_usage(steps::Vector{StepView}, now::Float64) =
    sum((s.usage_at_kill for s in steps
         if is_terminating(s) && has_members(s) && now - s.killed_at < DYING_WINDOW); init=0)

function memory_level(available::Int, reserve::Int, low_count::Int, critical::Bool)
    critical && return :critical
    low_count >= 2 && return :pressure
    available < 2reserve && return :low
    available < 3reserve && return :tight
    :ok
end

"""Admission pauses below 2·R_eff and resumes at 3·R_eff; in between it keeps its state."""
paused_at(level::Symbol, previous::Bool) = level === :tight ? previous : level !== :ok

"""The policy for one tick (design §3.7–3.8)."""
function decide(v::PolicyView, now::Float64)
    a_eff = v.available + dying_usage(v.steps, now)                      # A'
    low_count = v.memory && a_eff < v.reserve ? v.low_count + 1 : 0
    psi_count = v.memory && v.psi >= PSI_CRITICAL ? v.psi_count + 1 : 0
    critical = v.memory && (2v.available < v.reserve || (psi_count >= 2 && v.available < 2v.reserve))
    level = v.memory ? memory_level(v.available, v.reserve, low_count, critical) : :ok
    nlive = count(has_members, v.steps)

    terms = Term[]
    for s in v.steps
        is_terminating(s) || is_finished(s) || step_rule!(terms, s, now, critical)
    end
    termed = Set(t.id for t in terms)
    candidates = [s for s in v.steps
                  if has_members(s) && !is_terminating(s) && s.usage >= VICTIM_FLOOR_MB && !(s.id in termed)]
    # what steps terminated just now use counts as freed, like a dying victim's
    deficit = v.reserve - a_eff - sum((s.usage for s in v.steps if s.id in termed); init=0)

    warning, last_warn = "", v.last_warn
    if critical
        for victim in critical_victims(candidates, deficit)
            push!(terms, Term(victim.id, :critical, 0.0, nlive == 1))
        end
    elseif low_count >= 2 && deficit > 0
        if sum((s.usage for s in candidates); init=0) < deficit
            if now - v.last_warn >= WARN_INTERVAL
                warning, last_warn = outside_pressure_warning(v), now
            end
        elseif nlive > 1
            push!(terms, Term(candidates[victim_index(candidates, deficit)].id, :evicted, MEMORY_GRACE, false))
        end
    end

    kill_now = [s.id for s in v.steps
                if is_terminating(s) && has_members(s) && (critical || now - s.killed_at >= s.grace)]
    critical_since, driver, sigint_at = driver_last_resort(v, now, critical, isempty(candidates))
    PolicyResult(terms, kill_now, driver, level, paused_at(level, v.paused), low_count, psi_count,
                 critical_since, sigint_at, last_warn, warning)
end

# Rules for one live, not yet terminating step: deadlines, its own limit, leftovers.
function step_rule!(terms::Vector{Term}, s::StepView, now::Float64, critical::Bool)
    if now >= s.node_deadline
        push!(terms, Term(s.id, :node_timeout, TIMEOUT_GRACE, false))
    elseif now >= s.step_deadline
        push!(terms, Term(s.id, :step_timeout, TIMEOUT_GRACE, false))
    elseif s.limit > 0 && s.usage > s.limit
        push!(terms, Term(s.id, :memory_limit, critical ? 0.0 : MEMORY_GRACE, false))
    elseif has_members(s) && s.leaders_dead_since > 0 && now - s.leaders_dead_since >= LEFTOVER_SETTLE
        push!(terms, Term(s.id, :leftover, LEFTOVER_GRACE, false))
    end
    terms
end

"""Victims at critical level: picked in victim order until their usage covers the deficit."""
function critical_victims(candidates::Vector{StepView}, deficit::Int)
    pool = copy(candidates)
    victims = StepView[]
    while deficit > 0 && !isempty(pool)
        i = victim_index(pool, deficit)
        push!(victims, pool[i])
        deficit -= pool[i].usage
        deleteat!(pool, i)
    end
    victims
end

"""Victim order: the largest excess over its charge (an undeclared surprise consumer), else the
youngest step that covers the deficit alone, else the largest step."""
function victim_index(pool::Vector{StepView}, deficit::Int)
    best = 0
    for (i, s) in pairs(pool)
        s.usage > s.charge && (best == 0 || s.usage - s.charge > pool[best].usage - pool[best].charge) && (best = i)
    end
    best > 0 && return best
    for (i, s) in pairs(pool)
        s.usage >= deficit && (best == 0 || s.started > pool[best].started) && (best = i)
    end
    best > 0 && return best
    for (i, s) in pairs(pool)
        (best == 0 || s.usage > pool[best].usage) && (best = i)
    end
    best
end

"""Critical with nothing of ours left to kill and a driver that grew by over 25% of the total:
SIGINT the driver after DRIVER_SIGINT_AFTER, SIGKILL it DRIVER_SIGKILL_AFTER later.
Returns `(critical_since, action, driver_sigint_at)`."""
function driver_last_resort(v::PolicyView, now::Float64, critical::Bool, nothing_left::Bool)
    critical || return (0.0, :none, 0.0)
    since = v.critical_since > 0 ? v.critical_since : now
    if v.driver_sigint_at > 0
        action = now - v.driver_sigint_at >= DRIVER_SIGKILL_AFTER ? :sigkill : :none
        return (since, action, v.driver_sigint_at)
    end
    nothing_left && now - since >= DRIVER_SIGINT_AFTER && 4v.driver_growth > v.total && return (since, :sigint, now)
    (since, :none, 0.0)
end

function outside_pressure_warning(v::PolicyView)
    own = sum((s.usage for s in v.steps); init=0) + v.driver_growth
    "memory is low ($(v.available) MiB free, min_free_mb = $(v.min_free)) but this pipeline's commands " *
    "use only $own MiB; other programs are using the memory. New steps wait until memory is free."
end

"""Whether the guard should sample at the fast interval: memory is near the reserve, a step is
dying or has outlived its leaders, or a step is above 75% of its limit."""
fast_tick(v::PolicyView) =
    (v.memory && v.available < 3v.reserve) ||
    any(s -> has_members(s) && (is_terminating(s) || s.leaders_dead_since > 0 || near_limit(s)), v.steps)

near_limit(s::StepView) = s.limit > 0 && 4s.usage > 3s.limit
