# The resource guard: a separate Julia process, one per driver, that tracks every command step by
# session id, measures its memory and terminates steps on limits, deadlines, memory pressure,
# leftover processes, interrupts and driver death. The driver talks to it with one line per
# message over stdin/stdout (protocol below). The package includes this file too, so the driver
# can reuse the readers and the tests can call `decide`.
#
#   driver → guard                                   guard → driver
#   run    <run_id> <min_free_mb|0> <nice>           ready  <pid> <total_mb> <mem:0|1> <psi:0|1>
#   step   <id> <run_id> <limit_mb> <charge_mb>      sample <available_mb> <reserve_mb> <paused:0|1>
#          <step_timeout_ms> <deadline_ms> <label…>         <level> <id>:<usage_mb> …
#   sess   <id> <sid>                                gone   <id> <kind> <peak_mb> <usage_mb>
#   done   <id>                                             <alone:0|1> <detail…>
#   term   <id> <node_timeout|stdout|interrupt>      warn   <text…>
#   endrun <run_id>                                  bye    interrupt
#
# The guard ends on stdin EOF (the driver is gone: SIGKILL everything) or Ctrl-C (`bye interrupt`,
# SIGTERM, then SIGKILL). It ignores SIGTERM, SIGHUP and job control. There is deliberately no
# exception boundary: any exception ends the guard, and the driver then kills everything and stops
# the run (fail closed).

include(joinpath(@__DIR__, "procfs.jl"))
include(joinpath(@__DIR__, "policy.jl"))

const GUARD_TICK = 0.5
const GUARD_FAST_TICK = 0.1
const SIGNAL_POLL = 0.1             # longest wait between two checks for Ctrl-C
const UNKILLABLE_AFTER = 10.0       # seconds after the first SIGKILL
const EXIT_GRACE = 1.0              # Ctrl-C: SIGTERM, wait this long, SIGKILL
const FATAL_SIGNAL_EXIT = 128       # the exit code Julia gives atexit hooks after a fatal signal
const OOM_SCORE_RAISE = 500         # added to Julia's oom_score_adj for steps: the OOM killer prefers them
const MAX_NICE = 19
const PRIO_PGRP = 1
const SIGNAL_HUP = 1
const SIGNAL_TTIN = 21
const SIGNAL_TTOU = 22
const SIGNAL_TSTP = Sys.isbsd() ? 18 : 20
const SIG_UNBLOCK_HOW = Sys.isbsd() ? 2 : 1          # pthread_sigmask's SIG_UNBLOCK
const O_NONBLOCK_FLAG = Sys.isbsd() ? 0x0004 : 0o4000

# ---------------------------------------------------------------------------------------------
# Platforms and memory sources

"""Linux: members are found by scanning /proc (sessions plus setsid'd descendants)."""
struct ProcfsBackend end

"""Elsewhere: a step's members are its session leaders' process groups, probed with kill(-sid, 0)."""
struct GroupBackend end

"""Test hook (`--fake-memory=<path>`): each tick reads `available_mb total_mb psi_full` from a file."""
mutable struct FakeMemory
    const path::String
    const buf::Vector{UInt8}
    last::MemorySample
end

FakeMemory(path::String) = FakeMemory(path, Vector{UInt8}(undef, 256), sample_memory!(NoMemory(), 0.0))

function sample_memory!(m::FakeMemory, ::Float64)
    n = read_file!(m.buf, m.path)
    n > 0 || return m.last
    available, i, ok = parse_int(m.buf, 1, n)
    total, i, ok_total = parse_int(m.buf, i, n)
    ok && ok_total && total > 0 || return m.last
    psi = parse_decimal(m.buf, skip_blanks(m.buf, i, n), n)
    m.last = MemorySample(clamp(available, 0, MAX_MB), min(total, MAX_MB), psi)
end

measures_memory(::FakeMemory) = true
has_psi(::FakeMemory) = true

"""Systems without procfs: memory is not measured; free memory is reported for display only."""
struct NoMemory end

sample_memory!(::NoMemory, ::Float64) =
    MemorySample(Int(Sys.free_memory() ÷ BYTES_PER_MIB), Int(Sys.total_memory() ÷ BYTES_PER_MIB), -1.0)
measures_memory(::NoMemory) = false
has_psi(::NoMemory) = false

memory_source(fake::String) = !isempty(fake) ? FakeMemory(fake) : Sys.islinux() ? HostMemory() : NoMemory()

# ---------------------------------------------------------------------------------------------
# State

"""A command step registered by the driver, with its sessions and what the last scan saw."""
mutable struct GuardStep
    const id::Int
    const run::Int
    const limit_mb::Int                # 0 = none
    const charge_mb::Int
    const started::Float64
    const step_deadline::Float64       # Inf = none
    const node_deadline::Float64       # Inf = none
    const label::String
    const sids::Vector{Int32}
    const members::Vector{Int32}       # live member pids (elsewhere: live session groups)
    const groups::Vector{Int32}        # distinct process groups of the members
    const stuck::Vector{Int32}         # members in uninterruptible sleep (state D)
    const raised::Set{Int32}           # members whose oom_score_adj was raised
    usage_kb::Int                      # summed during a scan
    usage_mb::Int
    last_usage_mb::Int                 # the last usage measured while members were alive
    peak_mb::Int
    leaders_dead_since::Float64        # 0 = a leader is alive, or no session yet
    kind::Symbol                       # :none until terminated
    request::Symbol                    # a termination the driver asked for, applied after a scan
    killed_at::Float64                 # 0 = not terminating
    hard_killed_at::Float64            # first SIGKILL, for the unkillable report
    grace::Float64
    usage_at_kill::Int
    alone::Bool
    done::Bool                         # the driver sent `done`: every leader has exited
    detail::String
end

GuardStep(id::Int, run::Int, limit_mb::Int, charge_mb::Int, now::Float64, step_ms::Int, deadline_ms::Int, label::String) =
    GuardStep(id, run, limit_mb, charge_mb, now, deadline_after(now, step_ms), deadline_after(now, deadline_ms), label,
              Int32[], Int32[], Int32[], Int32[], Set{Int32}(), 0, 0, 0, 0, 0.0, :none, :none, 0.0, 0.0, 0.0, 0, false,
              false, "")

deadline_after(now::Float64, ms::Int) = ms > 0 ? now + ms / 1000 : Inf

is_finished(s::GuardStep) = !isempty(s.sids) && s.leaders_dead_since > 0 && isempty(s.members)

struct GuardRun
    min_free_mb::Int                   # 0 = auto
    nice::Int
    driver_baseline_mb::Int            # driver usage when the run started
end

"""The step a process was attributed to, with the process's starttime (stat field 22) so that a
reused pid is never mistaken for it."""
struct Owner
    step::Int
    starttime::Int
end

const NO_OWNER = Owner(0, 0)

mutable struct GuardState{B,M}
    const backend::B
    const memory::M
    const interval::Float64
    const pid::Int32
    const driver::Int32
    const autogroup::Bool
    const oom_adj::String              # written to the oom_score_adj of every member
    const steps::Dict{Int,GuardStep}
    const by_sid::Dict{Int32,Int}      # sessions of the leaders the driver started
    const adopted_sids::Dict{Int32,Int}   # sessions that members started with setsid
    const runs::Dict{Int,GuardRun}
    const replied_early::Set{Int}      # answered `unkillable` before their `done`
    const procs::Vector{ProcInfo}
    const names::Vector{String}        # scratch space for listing /proc
    const live_sids::Set{Int32}        # scratch space: sessions with a live process
    const buf::Vector{UInt8}
    const history::Vector{Tuple{Float64,Int,Int}}   # (time, pipeline usage U, available A): growth term
    const outbox::Vector{UInt8}        # gone/warn/ready/bye bytes the pipe did not take yet
    owner::Dict{Int32,Owner}           # pid => owner, this scan
    previous_owner::Dict{Int32,Owner}  # the scan before
    sample::MemorySample
    min_free_mb::Int
    reserve_mb::Int
    level::Symbol
    paused::Bool
    fast::Bool
    stale::Bool                        # a command waits for a fresh scan before it is answered
    low_count::Int
    psi_count::Int
    critical_since::Float64
    driver_sigint_at::Float64
    last_warn::Float64
    last_psi_kill::Float64
    driver_usage_mb::Int
    stopping::Bool                     # Ctrl-C: the steps are being terminated
    finished::Bool                     # every process has been killed; atexit has nothing to do
end

function GuardState(backend, memory, interval::Float64)
    driver = Int32(ccall(:getppid, Cint, ()))
    GuardState(backend, memory, interval, Int32(getpid()), driver, autogroup_enabled(), step_oom_adj(driver),
               Dict{Int,GuardStep}(), Dict{Int32,Int}(), Dict{Int32,Int}(), Dict{Int,GuardRun}(), Set{Int}(),
               ProcInfo[], String[], Set{Int32}(), Vector{UInt8}(undef, 8192), Tuple{Float64,Int,Int}[], UInt8[],
               Dict{Int32,Owner}(), Dict{Int32,Owner}(), MemorySample(0, 0, -1.0),
               0, 0, :ok, false, false, false, 0, 0, 0.0, 0.0, -Inf, -Inf, 0, false, false)
end

"""The oom_score_adj of step processes: the driver's raised by OOM_SCORE_RAISE (at most 1000), so the
kernel OOM killer prefers a step to Julia also where Julia's own score is raised (CI runners,
containers, systemd units)."""
function step_oom_adj(driver::Int32)
    buf = Vector{UInt8}(undef, 16)
    n = read_file!(buf, string("/proc/", driver, "/oom_score_adj"))
    string(min(1000, (n > 0 ? parse_int(buf, 1, n)[1] : 0) + OOM_SCORE_RAISE))
end

function autogroup_enabled()
    Sys.islinux() || return false
    buf = Vector{UInt8}(undef, 16)
    read_file!(buf, "/proc/sys/kernel/sched_autogroup_enabled") > 0 && buf[1] == UInt8('1')
end

# ---------------------------------------------------------------------------------------------
# Entry point

"""Entry point of the guard process. Options: `--interval=<seconds>` (normal tick, default 0.5)
and `--fake-memory=<path>` (tests)."""
function guard_main()
    interval, fake = guard_options(ARGS)
    run_guard(GuardState(Sys.islinux() ? ProcfsBackend() : GroupBackend(), memory_source(fake), interval))
end

"""Serve the driver until stdin closes or Ctrl-C arrives (tests also run it with GroupBackend on
Linux). Every task of the guard defers SIGINT, which is taken between ticks only
(`sigint_pending`): it never cuts a tick short, and a second Ctrl-C cannot end the guard before
its cleanup, as it would through Julia's exit-on-SIGINT."""
function run_guard(g::GuardState)
    Base.exit_on_sigint(false)
    disable_sigint(() -> serve!(g))
end

function serve!(g::GuardState)
    start_guard!(g)
    reader = @async disable_sigint(() -> read_commands!(g))
    last_tick = monotonic_seconds()
    while true
        sleep(clamp(last_tick + tick_interval(g) - monotonic_seconds(), 0.0, SIGNAL_POLL))
        istaskdone(reader) && reader_ended!(g, reader)
        sigint_pending() && interrupted!(g)
        now = monotonic_seconds()
        now - last_tick >= tick_interval(g) || continue
        guard_tick!(g, now)
        send_sample!(g)
        last_tick = now
    end
end

"""Whether Ctrl-C arrived. The guard's tasks defer SIGINT, but a new task starts with it enabled,
so a pending SIGINT is thrown at the start of this throwaway task instead."""
function sigint_pending()
    probe = @async nothing
    while !istaskdone(probe)
        yield()
    end
    istaskfailed(probe)
end

"""The reader task ended: Ctrl-C landed in it (at its start, before it deferred SIGINT), or it hit
a bug, which is rethrown here, printed, and ends the guard through `guard_exit!`."""
function reader_ended!(g::GuardState, reader::Task)
    istaskfailed(reader) || stdin_closed!(g)
    is_interrupt(reader.result) ? interrupted!(g) : wait(reader)
end

function guard_options(args::Vector{String})
    interval, fake = GUARD_TICK, ""
    for a in args
        if startswith(a, "--interval=")
            x = something(tryparse(Float64, a[ncodeunits("--interval=")+1:end]), 0.0)
            isfinite(x) && x > 0 && (interval = x)
        elseif startswith(a, "--fake-memory=")
            fake = a[ncodeunits("--fake-memory=")+1:end]
        end
    end
    (interval, fake)
end

function start_guard!(g::GuardState)
    # Terminal hangup, job control and background reads must not stop enforcement.
    for sig in (SIGNAL_HUP, SIGNAL_TSTP, SIGNAL_TTIN, SIGNAL_TTOU)
        ignore_signal(sig)
    end
    # Julia treats SIGTERM as fatal: it prints a backtrace before running atexit hooks, and a second
    # SIGTERM meanwhile ends it with no cleanup at all. SIGTERM to the process group reaches the
    # driver too, which exits: its atexit hook kills its commands, and the guard sees EOF.
    discard_signal(SIGNAL_TERM)
    atexit(code -> guard_exit!(g, code))
    set_nonblocking_stdout()
    compile_guard(g)
    guard_tick!(g, monotonic_seconds())
    emit!(g, "ready $(g.pid) $(g.sample.total) $(Int(measures_memory(g.memory))) $(Int(has_psi(g.memory)))")
    send_sample!(g)
end

"""Compile what commands, ticks and the exit paths run before `ready`. At -O0 the first scan with
a step would otherwise take a third of a second, long enough for a short-lived parent to exit
before its setsid'd child is seen."""
function compile_guard(::G) where {G<:GuardState}
    for (f, types) in ((handle_command!, (G, String, Float64)), (settle_commands!, (G, Float64)),
                       (guard_tick!, (G, Float64)), (send_sample!, (G,)), (stop_steps!, (G,)),
                       (guard_exit!, (G, Int32)), (stdin_closed!, (G,)), (sigint_pending, ()))
        precompile(f, types)
    end
end

ignore_signal(sig::Integer) = ccall(:signal, Ptr{Cvoid}, (Cint, Ptr{Cvoid}), sig, Ptr{Cvoid}(1))   # SIG_IGN

"""Ignore `sig` although Julia blocks it in every thread to collect it with sigwait (a blocked
signal is queued whatever its disposition): unblocked in this main thread with SIG_IGN, it is
dropped on arrival. (macOS reports it to Julia through kqueue all the same.)"""
function discard_signal(sig::Integer)
    ignore_signal(sig)
    set = zeros(UInt8, 128)                     # a sigset_t: 128 bytes on Linux, 4 on macOS
    ccall(:sigemptyset, Cint, (Ptr{UInt8},), set)
    ccall(:sigaddset, Cint, (Ptr{UInt8}, Cint), set, sig)
    ccall(:pthread_sigmask, Cint, (Cint, Ptr{UInt8}, Ptr{UInt8}), SIG_UNBLOCK_HOW, set, C_NULL)
end

function set_nonblocking_stdout()
    flags = ccall(:fcntl, Cint, (Cint, Cint, Cint...), 1, 3)                        # F_GETFL
    flags >= 0 && ccall(:fcntl, Cint, (Cint, Cint, Cint...), 1, 4, flags | O_NONBLOCK_FLAG)  # F_SETFL
end

tick_interval(g::GuardState) = g.fast ? min(g.interval, GUARD_FAST_TICK) : g.interval

"""One tick: membership and usage, the driver's requests, available memory, the policy, then replies."""
function guard_tick!(g::GuardState, now::Float64)
    observe!(g, now)
    apply_requests!(g, now)
    g.sample = sample_memory!(g.memory, now)
    enforce!(g, now)
    g.stale || reply_finished!(g)
    flush_outbox!(g)
end

# ---------------------------------------------------------------------------------------------
# Membership and usage

function observe!(g::GuardState{ProcfsBackend}, now::Float64)
    g.driver_usage_mb = driver_usage_now(g)
    if isempty(g.steps)
        empty!(g.owner)
        empty!(g.previous_owner)
        empty!(g.adopted_sids)
        g.stale = false
        return
    end
    # An unreadable /proc is not an empty one: keep the last view until a scan succeeds.
    scan_processes!(g.procs, g.buf, g.names) || return
    g.owner, g.previous_owner = g.previous_owner, g.owner
    empty!(g.owner)
    attribute!(g)
    collect_members!(g, now)
    g.stale = false
end

function observe!(g::GuardState{GroupBackend}, ::Float64)
    for s in values(g.steps)
        empty!(s.members)
        empty!(s.groups)
        for sid in s.sids
            group_alive(sid) && (push!(s.members, sid); push!(s.groups, sid))
        end
    end
    g.stale = false
end

driver_usage_now(g::GuardState{ProcfsBackend}) = process_usage_kb(g.buf, g.driver) ÷ 1024
driver_usage_now(::GuardState{GroupBackend}) = 0

trackable(g::GuardState, p::ProcInfo) = p.pid > 1 && p.pid != g.pid && p.pid != g.driver

"""Attribute processes to steps: by session, because they were members at the previous scan (a
member stays one until it exits, even after leaving its session), or as descendants of members."""
function attribute!(g::GuardState)
    for p in g.procs
        trackable(g, p) || continue
        id = known_owner(g, p)
        id == 0 || (g.owner[p.pid] = Owner(id, p.starttime))
    end
    adopted = true
    while adopted                       # a chain of setsid'd descendants is found in one scan
        adopted = false
        for p in g.procs
            trackable(g, p) && !haskey(g.owner, p.pid) || continue
            id = get(g.owner, p.ppid, NO_OWNER).step
            id == 0 && continue
            g.owner[p.pid] = Owner(id, p.starttime)
            adopted = true
        end
    end
    adopt_sessions!(g)
end

function known_owner(g::GuardState, p::ProcInfo)
    id = get(g.by_sid, p.sid, 0)
    id == 0 && (id = get(g.adopted_sids, p.sid, 0))
    if id == 0
        o = get(g.previous_owner, p.pid, NO_OWNER)
        o.starttime == p.starttime && (id = o.step)
    end
    haskey(g.steps, id) ? id : 0
end

"""A member that called setsid leads a session of its own: whatever is started in it later
belongs to the same step, even once its leader has exited. A session with no process left is
forgotten, so its id, once reused, cannot attribute a stranger."""
function adopt_sessions!(g::GuardState)
    empty!(g.live_sids)
    for p in g.procs
        push!(g.live_sids, p.sid)
        p.pid == p.sid && !haskey(g.by_sid, p.sid) || continue
        id = get(g.owner, p.pid, NO_OWNER).step
        id == 0 || (g.adopted_sids[p.sid] = id)
    end
    filter!(kv -> kv.first in g.live_sids && haskey(g.steps, kv.second), g.adopted_sids)
end

function collect_members!(g::GuardState, now::Float64)
    for s in values(g.steps)
        empty!(s.members)
        empty!(s.groups)
        empty!(s.stuck)
        s.usage_kb = 0
    end
    for p in g.procs
        id = get(g.owner, p.pid, NO_OWNER).step
        id == 0 || add_member!(g, g.steps[id], p)
    end
    for s in values(g.steps)
        s.usage_mb = s.usage_kb ÷ 1024
        s.peak_mb = max(s.peak_mb, s.usage_mb)
        isempty(s.members) || (s.last_usage_mb = s.usage_mb)
        update_leaders!(s, g.owner, now)
        filter!(in(s.members), s.raised)    # a reused pid gets raised again
    end
end

function add_member!(g::GuardState, s::GuardStep, p::ProcInfo)
    push!(s.members, p.pid)
    p.pgrp in s.groups || push!(s.groups, p.pgrp)
    p.state == UInt8('D') && push!(s.stuck, p.pid)
    s.usage_kb += process_usage_kb(g.buf, p.pid)
    if !(p.pid in s.raised)
        write_file(string("/proc/", p.pid, "/oom_score_adj"), g.oom_adj)   # may fail; harmless
        push!(s.raised, p.pid)
    end
end

# Leaders are the session leaders (pid == sid); a zombie leader counts as gone.
function update_leaders!(s::GuardStep, owner::Dict{Int32,Owner}, now::Float64)
    isempty(s.sids) && return
    if any(sid -> get(owner, sid, NO_OWNER).step == s.id, s.sids)
        s.leaders_dead_since = 0.0
    elseif s.leaders_dead_since == 0
        s.leaders_dead_since = now
    end
end

"""PSS-confirmed usage of a multi-process step (forked copy-on-write pages count once), unless
memory is near critical: smaps_rollup takes the mmap lock, so status sums are used then."""
confirmed_usage(g::GuardState, s::GuardStep) =
    length(s.members) > 1 && !near_critical(g) ? pss_usage(g, s) : s.usage_mb

pss_usage(g::GuardState{ProcfsBackend}, s::GuardStep) =
    sum((process_pss_kb(g.buf, pid) for pid in s.members); init=0) ÷ 1024
pss_usage(::GuardState{GroupBackend}, s::GuardStep) = s.usage_mb

near_critical(g::GuardState) = g.level === :critical || 2g.sample.available < g.reserve_mb

limit_usage(g::GuardState, s::GuardStep) =
    s.limit_mb > 0 && s.usage_mb > s.limit_mb ? confirmed_usage(g, s) : s.usage_mb

# ---------------------------------------------------------------------------------------------
# Enforcement

function enforce!(g::GuardState, now::Float64)
    update_reserve!(g, now)
    view = policy_view(g)
    r = decide(view, now)
    g.level, g.paused, g.low_count, g.psi_count = r.level, r.paused, r.low_count, r.psi_count
    g.critical_since, g.driver_sigint_at, g.last_warn = r.critical_since, r.driver_sigint_at, r.last_warn
    g.last_psi_kill = r.last_psi_kill
    for t in r.terms
        terminate_step!(g, g.steps[t.id], t, now)
    end
    for id in r.kill_now
        escalate_step!(g, g.steps[id], now)
    end
    signal_driver!(g, r.driver)
    isempty(r.warning) || emit!(g, "warn " * r.warning)
    report_unkillable!(g, now)
    g.fast = fast_tick(view) || !isempty(r.terms)
end

function update_reserve!(g::GuardState, now::Float64)
    push!(g.history, (now, pipeline_usage(g), g.sample.available))
    while length(g.history) > 1 && now - first(g.history)[1] > GROWTH_WINDOW
        popfirst!(g.history)
    end
    g.min_free_mb = run_min_free(g)
    g.reserve_mb = effective_reserve(g.min_free_mb, growth_rate(g.history), g.sample.total)
end

"""U: what the pipeline owns, its steps plus the driver's growth since its run started."""
pipeline_usage(g::GuardState) = sum((s.usage_mb for s in values(g.steps)); init=0) + driver_growth(g)

driver_growth(g::GuardState) =
    isempty(g.runs) ? 0 : max(0, g.driver_usage_mb - minimum(r.driver_baseline_mb for r in values(g.runs)))

"""R: the largest min_free_mb over active runs, a run with 0 counting as the automatic value."""
function run_min_free(g::GuardState)
    auto = auto_min_free(g.sample.total)
    isempty(g.runs) ? auto : maximum(r.min_free_mb > 0 ? r.min_free_mb : auto for r in values(g.runs))
end

function policy_view(g::GuardState)
    steps = [StepView(s.id, length(s.members), limit_usage(g, s), s.charge_mb, s.limit_mb, s.started,
                      s.step_deadline, s.node_deadline, s.leaders_dead_since, s.killed_at, s.grace, s.usage_at_kill)
             for s in values(g.steps)]
    sort!(steps; by=s -> s.id)
    PolicyView(steps, measures_memory(g.memory), g.sample.available, g.sample.total, g.min_free_mb,
               g.reserve_mb, g.sample.psi, driver_growth(g), g.paused, g.low_count, g.psi_count,
               g.critical_since, g.driver_sigint_at, g.last_warn, g.last_psi_kill)
end

"""Start terminating a step: SIGTERM to every member, each of their process groups and each
session, or SIGKILL at once when there is no grace."""
function terminate_step!(g::GuardState, s::GuardStep, t::Term, now::Float64)
    s.kind, s.killed_at, s.grace, s.alone = t.kind, now, t.grace, t.alone
    s.usage_at_kill = t.kind === :evicted || t.kind === :memory_limit ? confirmed_usage(g, s) : s.usage_mb
    s.peak_mb = max(s.peak_mb, s.usage_at_kill)     # PSS is read after the scan, and may be higher
    t.kind === :leftover && (s.detail = describe_processes(g, s.members))
    t.grace > 0 ? signal_step!(g, s, SIGNAL_TERM) : escalate_step!(g, s, now)
    nothing
end

function escalate_step!(g::GuardState, s::GuardStep, now::Float64)
    signal_step!(g, s, SIGNAL_KILL)
    s.hard_killed_at == 0 && (s.hard_killed_at = now)
    nothing
end

# Members, then each process group seen among them (children of setpgid tools such as GNU
# `timeout`), then each session's own group.
function signal_step!(::GuardState{ProcfsBackend}, s::GuardStep, sig::Integer)
    for pid in s.members
        send_signal(pid, sig)
    end
    for pgrp in s.groups
        send_signal(-Int(pgrp), sig)
    end
    for sid in s.sids
        send_signal(-Int(sid), sig)
    end
end

function signal_step!(::GuardState{GroupBackend}, s::GuardStep, sig::Integer)
    for sid in s.sids
        send_signal(-Int(sid), sig)
    end
end

signal_all!(g::GuardState, sig::Integer) = foreach(s -> signal_step!(g, s, sig), values(g.steps))

"""The last resort when Julia itself holds the memory (function steps): SIGINT, then SIGKILL."""
function signal_driver!(g::GuardState, action::Symbol)
    if action === :sigint
        emit!(g, "warn memory is critical and Julia itself grew by $(driver_growth(g)) MiB; interrupting it")
        send_signal(g.driver, SIGNAL_INT)
    elseif action === :sigkill
        send_signal(g.driver, SIGNAL_KILL)
    end
    nothing
end

describe_processes(g::GuardState, pids::Vector{Int32}) =
    join((string(pid, ' ', process_comm(g.buf, pid)) for pid in pids), ' ')

"""Members still alive UNKILLABLE_AFTER the first SIGKILL (usually stuck in state D): report the
step as `unkillable` and stop tracking it, even before its `done`."""
function report_unkillable!(g::GuardState, now::Float64)
    for s in collect(values(g.steps))
        s.hard_killed_at > 0 && !isempty(s.members) && now - s.hard_killed_at >= UNKILLABLE_AFTER || continue
        s.kind = :unkillable
        s.detail = describe_processes(g, isempty(s.stuck) ? s.members : s.stuck)
        s.done || push!(g.replied_early, s.id)
        reply_gone!(g, s)
    end
end

"""Steps whose leaders exited (`done`) and that have no live members get their `gone`."""
function reply_finished!(g::GuardState)
    for s in collect(values(g.steps))
        s.done && isempty(s.members) && reply_gone!(g, s)
    end
end

function reply_gone!(g::GuardState, s::GuardStep)
    usage = s.killed_at > 0 ? s.usage_at_kill : s.last_usage_mb
    line = "gone $(s.id) $(s.kind) $(s.peak_mb) $usage $(Int(s.alone))"
    emit!(g, isempty(s.detail) ? line : line * " " * s.detail)
    forget!(g, s)
end

"""Stop tracking a step once its `gone` is sent, so it is never signalled again (the latch)."""
function forget!(g::GuardState, s::GuardStep)
    delete!(g.steps, s.id)
    for sid in s.sids
        get(g.by_sid, sid, 0) == s.id && delete!(g.by_sid, sid)
    end
    filter!(kv -> kv.second != s.id, g.adopted_sids)
end

# ---------------------------------------------------------------------------------------------
# Commands from the driver

function read_commands!(g::GuardState)
    while !eof(stdin)
        handle_command!(g, readline(stdin), monotonic_seconds())
        # Answer when the lines at hand are handled: one scan serves a burst of `done`s. During a
        # long burst the ticks still get their turn.
        bytesavailable(stdin) > 0 ? yield() : settle_commands!(g, monotonic_seconds())
    end
    stdin_closed!(g)
end

function handle_command!(g::GuardState, line::AbstractString, now::Float64)
    f = split(line)
    isempty(f) && return
    word = f[1]
    if word == "run"
        command_run!(g, f)
    elseif word == "step"
        command_step!(g, f, now)
    elseif word == "sess"
        command_sess!(g, f, now)
    elseif word == "done"
        command_done!(g, f, now)
    elseif word == "term"
        command_term!(g, f)
    elseif word == "endrun"
        command_endrun!(g, f)
    else
        emit!(g, "warn the resource guard ignored an unknown message: " * printable(word))
    end
    nothing
end

"""After the commands at hand: one fresh scan for all of them, then the terminations they asked
for and the `gone` replies they made possible."""
function settle_commands!(g::GuardState, now::Float64)
    g.stale && observe!(g, now)
    apply_requests!(g, now)
    g.stale || reply_finished!(g)
    flush_outbox!(g)
end

field_int(f::Vector{<:AbstractString}, i::Int) = i <= length(f) ? parse_int(f[i]) : 0
field_text(f::Vector{<:AbstractString}, i::Int) = i <= length(f) ? String(f[i]) : ""

command_run!(g::GuardState, f) =
    g.runs[field_int(f, 2)] = GuardRun(clamp(field_int(f, 3), 0, MAX_MB), clamp(field_int(f, 4), 0, MAX_NICE),
                                       driver_usage_now(g))

function command_step!(g::GuardState, f, now::Float64)
    id = field_int(f, 2)
    id > 0 && !haskey(g.steps, id) || return
    g.steps[id] = GuardStep(id, field_int(f, 3), clamp(field_int(f, 4), 0, MAX_MB), clamp(field_int(f, 5), 0, MAX_MB),
                            now, field_int(f, 6), field_int(f, 7), join(f[8:end], ' '))
end

# A sid is the pid of a setsid'd leader; ours, the driver's or a shared group/session never is.
valid_session(g::GuardState, sid::Int32) = foreign_session(sid) && sid != g.driver

function command_sess!(g::GuardState, f, now::Float64)
    id, sid = field_int(f, 2), pid32(field_int(f, 3))
    haskey(g.steps, id) && valid_session(g, sid) || return
    s = g.steps[id]
    sid in s.sids || push!(s.sids, sid)
    g.by_sid[sid] = id
    s.leaders_dead_since = 0.0
    set_priority!(g, sid, haskey(g.runs, s.run) ? g.runs[s.run].nice : 0)
    # A step already being terminated takes its new session down with it.
    s.killed_at > 0 && send_signal(-Int(sid), now - s.killed_at >= s.grace ? SIGNAL_KILL : SIGNAL_TERM)
end

"""Lower a new session's priority: autogroup nice (each setsid starts a new autogroup, between
which plain nice has no effect) and setpriority on its process group. Failures are ignored."""
function set_priority!(g::GuardState, sid::Int32, nice::Int)
    nice > 0 || return
    g.autogroup && write_file(string("/proc/", sid, "/autogroup"), string(nice))
    ccall(:setpriority, Cint, (Cint, Cuint, Cint), PRIO_PGRP, sid, nice)
end

function command_done!(g::GuardState, f, now::Float64)
    id = field_int(f, 2)
    if !haskey(g.steps, id)
        id in g.replied_early ? delete!(g.replied_early, id) : emit!(g, "gone $id none 0 0 0")
        return
    end
    s = g.steps[id]
    s.done = true
    leaders_exited!(g, s, now)
    g.stale = true                  # answered after a fresh scan, so anything it started counts
end

# Without procfs the leftover check starts when the driver reports its leaders gone.
leaders_exited!(::GuardState{ProcfsBackend}, ::GuardStep, ::Float64) = nothing
leaders_exited!(::GuardState{GroupBackend}, s::GuardStep, now::Float64) =
    s.leaders_dead_since == 0 && (s.leaders_dead_since = now)

function command_term!(g::GuardState, f)
    id = field_int(f, 2)
    haskey(g.steps, id) && request!(g, g.steps[id], term_cause(field_text(f, 3)))
end

function command_endrun!(g::GuardState, f)
    run = field_int(f, 2)
    delete!(g.runs, run)
    for s in values(g.steps)
        s.run == run && request!(g, s, :interrupt)
    end
end

term_cause(word::String) = word == "node_timeout" ? :node_timeout : word == "stdout" ? :stdout : :interrupt
request_grace(kind::Symbol) = kind === :node_timeout ? TIMEOUT_GRACE : INTERRUPT_GRACE

"""Record a termination the driver asked for, to apply after a fresh scan (the first cause wins)."""
function request!(g::GuardState, s::GuardStep, kind::Symbol)
    s.request === :none && (s.request = kind)
    g.stale = true
end

"""Terminate the steps the driver asked to, with their members as the last scan saw them, unless
they finished or are being terminated for another cause already."""
function apply_requests!(g::GuardState, now::Float64)
    for s in values(g.steps)
        kind = s.request
        kind === :none && continue
        s.request = :none
        s.killed_at > 0 || is_finished(s) || terminate_step!(g, s, Term(s.id, kind, request_grace(kind), false), now)
        g.fast = true
    end
end

# ---------------------------------------------------------------------------------------------
# Driver death, Ctrl-C, bugs

"""The driver is gone (stdin EOF): SIGKILL everything at once and exit."""
function stdin_closed!(g::GuardState)
    kill_steps!(g)
    exit(0)
end

"""Ctrl-C: stop the steps, then exit."""
function interrupted!(g::GuardState)
    stop_steps!(g)
    exit(128 + SIGNAL_INT)
end

"""Tell the driver (`bye interrupt`), give every step EXIT_GRACE to exit after SIGTERM, then
SIGKILL what is left. Entered again (a fatal signal meanwhile), it SIGKILLs at once."""
function stop_steps!(g::GuardState)
    ignore_signal(SIGNAL_INT)                  # a second Ctrl-C has nothing left to ask for
    g.stopping && return kill_steps!(g)
    g.stopping = true
    emit!(g, "bye interrupt")
    signal_all!(g, SIGNAL_TERM)
    deadline = monotonic_seconds() + EXIT_GRACE
    while monotonic_seconds() < deadline && any(s -> !isempty(s.members), values(g.steps))
        Libc.systemsleep(0.05)
        observe!(g, monotonic_seconds())
        flush_outbox!(g)
    end
    kill_steps!(g)
    drain_outbox!(g)
end

"""SIGKILL every step at once; afterwards `guard_exit!` has nothing left to do."""
function kill_steps!(g::GuardState)
    signal_all!(g, SIGNAL_KILL)              # what is already known, before anything can fail
    observe!(g, monotonic_seconds())
    signal_all!(g, SIGNAL_KILL)
    g.finished = true
    nothing
end

"""atexit. The guard exits on its own after EOF and Ctrl-C (`finished`). Getting here otherwise
means a fatal signal (exit code 128: SIGQUIT, or SIGTERM where it cannot be ignored), handled like
Ctrl-C, or a bug (an uncaught exception): then every step is SIGKILLed at once and no `bye` is
sent, so the driver takes its guard-lost path."""
function guard_exit!(g::GuardState, code::Integer)
    g.finished && return
    ignore_signal(SIGNAL_INT)
    code == FATAL_SIGNAL_EXIT ? stop_steps!(g) : (kill_steps!(g); flush_outbox!(g))
    nothing
end

is_interrupt(::InterruptException) = true
is_interrupt(_) = false

# ---------------------------------------------------------------------------------------------
# Output: raw non-blocking writes to fd 1, so a driver that is not reading never blocks the guard

write_stdout(bytes::Vector{UInt8}, from::Int) =
    Int(GC.@preserve bytes ccall(:write, Cssize_t, (Cint, Ptr{UInt8}, Csize_t),
                                 1, pointer(bytes, from), length(bytes) - from + 1))

"""Queue a line that must reach the driver (ready, gone, warn, bye) and write what the pipe takes;
the rest is retried every tick."""
function emit!(g::GuardState, line::AbstractString)
    append!(g.outbox, codeunits(line))
    push!(g.outbox, UInt8('\n'))
    flush_outbox!(g)
end

function flush_outbox!(g::GuardState)
    while !isempty(g.outbox)
        n = write_stdout(g.outbox, 1)
        n > 0 || return false
        deleteat!(g.outbox, 1:n)
    end
    true
end

function drain_outbox!(g::GuardState)
    for _ in 1:50
        flush_outbox!(g) && return
        Libc.systemsleep(0.01)
    end
end

"""At most 64 bytes of `text`, anything but printable ASCII replaced: safe inside one protocol line."""
printable(text::AbstractString) =
    String(map(c -> 0x21 <= c <= 0x7e ? c : UInt8('?'), codeunits(text)[1:min(end, 64)]))

"""Write a sample unless the pipe is full: a dropped sample is superseded by the next one."""
function send_sample!(g::GuardState)
    flush_outbox!(g) || return
    bytes = Vector{UInt8}(sample_line(g) * "\n")
    n = write_stdout(bytes, 1)
    0 < n < length(bytes) && append!(g.outbox, @view bytes[n+1:end])   # a started line must be finished
    nothing
end

function sample_line(g::GuardState)
    io = IOBuffer()
    print(io, "sample ", g.sample.available, ' ', g.reserve_mb, ' ', Int(g.paused), ' ', g.level)
    for id in sort!(collect(keys(g.steps)))
        s = g.steps[id]
        isempty(s.sids) || print(io, ' ', id, ':', s.usage_mb)
    end
    String(take!(io))
end
