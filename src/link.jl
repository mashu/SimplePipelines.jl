# The driver's side of the resource guard. The guard (guard.jl) is a separate Julia
# process, one per Julia process, started by the first `run`. It measures every command
# the pipeline starts, terminates commands that use too much memory or time, and kills
# them all when this process is interrupted or dies. It keeps enforcing while this
# process is busy, and commands never run without it: if it cannot start or exits,
# the run stops (pass `guard=false` to run without it).
#
# Protocol, one line per message. To the guard:
#   run <run> <min_free_mb> <nice> | step <id> <run> <limit_mb> <charge_mb> <timeout_ms> <deadline_ms> <label>
#   sess <id> <sid> | done <id> | term <id> <reason> | endrun <run>
# From the guard:
#   ready <pid> <total_mb> <mem> <psi> | sample <available> <reserve> <paused> <level> <id>:<usage>...
#   gone <id> <kind> <peak_mb> <usage_mb> <alone> <detail> | warn <text> | bye interrupt

@nospecialize   # see runtime.jl

const GUARD_WAIT = 30.0             # seconds to wait for `ready`; a guard silent this long is lost
const GUARD_STDERR_TAIL = 16 * 1024
const GUARD_SUPPORTED = !Sys.iswindows()

"""How a command ended, from the guard's `gone` line (`kind` :none when it ended by itself)."""
struct Gone
    kind::Symbol
    peak_mb::Int
    usage_mb::Int
    alone::Bool                     # it was the only command running when it was stopped
    detail::String
end
Gone(kind::Symbol) = Gone(kind, 0, 0, false, "")

#==============================================================================#
# Sessions: every command leader runs in its own session (setsid). They are known for
# the whole process so an interrupt, a lost guard or exit can kill them directly.
#==============================================================================#

const SESSIONS_LOCK = ReentrantLock()
const SESSIONS = Dict{Int,Vector{Int32}}()      # grant id => session ids
const TERMINATED = Dict{Int,Symbol}()            # without the guard: why a command was killed

remember_sessions!(id::Int, sids::Vector{Int32}) = (with_lock(setindex!, SESSIONS_LOCK, SESSIONS, sids, id); nothing)
forget_sessions!(id::Int) = with_lock(pop!, SESSIONS_LOCK, SESSIONS, id, Int32[])
sessions_of(id::Int) = with_lock(get, SESSIONS_LOCK, SESSIONS, id, Int32[])

"""Every remembered session, forgotten at once: they are about to be killed, and their pids may be reused."""
take_all_sessions!() = with_lock(take_sessions!, SESSIONS_LOCK, SESSIONS)
take_sessions!(d::Dict{Int,Vector{Int32}}) = (sids = reduce(vcat, values(d); init=Int32[]); empty!(d); sids)

"""Kill the sessions and every process in them (and their setsid'd children)."""
kill_all!(sids::Vector{Int32}) = (GUARD_SUPPORTED && !isempty(sids) && kill_sessions!(sids); nothing)

"""atexit hook: kill every command still running (script-mode Ctrl-C, SIGTERM, `exit`)."""
function kill_at_exit()
    trylock(SESSIONS_LOCK) || return    # busy: the guard sees EOF and kills them
    sids = take_sessions!(SESSIONS)
    unlock(SESSIONS_LOCK)
    kill_all!(sids)
end

#==============================================================================#
# Links: GuardLink talks to a guard process; NoGuard is used by `check` and guard=false
#==============================================================================#

mutable struct GuardLink
    const process::Base.Process
    const pid::Int32
    const measures::Bool            # memory is measured on this system
    const errors::Task              # tail of the guard's stderr, shown if it dies
    const lock::ReentrantLock
    const replies::Dict{Int,Channel{Gone}}
    @atomic sample::GuardSample
    @atomic heard::Float64          # when the last line from the guard was read
    @atomic ended::Symbol           # :none while it runs; then :interrupt, :stopped or :closed
end

"""Run without the guard: no measuring and no limits; commands are killed at step end."""
struct NoGuard end
const NO_GUARD = NoGuard()

measures(link::GuardLink) = link.measures
measures(::NoGuard) = false
latest_sample(link::GuardLink) = @atomic link.sample
latest_sample(::NoGuard) = GuardSample()

mutable struct LinkSlot
    @atomic link::Union{Nothing,GuardLink}
end
const CURRENT = LinkSlot(nothing)
const LINK_LOCK = ReentrantLock()      # serializes starting a guard

struct GuardOptions
    interval::Float64
    fake_memory::String             # test hook: read memory from this file
end
const GUARD_OPTIONS = Ref(GuardOptions(0.5, ""))

"""The live guard, started when there is none; NO_GUARD when `guard` is false."""
function connect(guard::Bool)
    guard || return NO_GUARD
    GUARD_SUPPORTED || throw(ArgumentError(
        "the resource guard does not support Windows; pass guard=false to run without memory limits, timeouts or cleanup"))
    lock(live_link, LINK_LOCK)
end

function live_link()
    link = @atomic CURRENT.link
    link !== nothing && (@atomic link.ended) === :none && return link
    started = start_guard(GUARD_OPTIONS[])
    @atomic CURRENT.link = started
    started
end

"""
The guard runs this package's precompiled code: loading it takes a fraction of a second,
where compiling guard.jl from source takes seconds. The child gets this process's
flags, project and load path, so it finds the same precompiled package. While the
package is being precompiled, when its cache is stale (its source was edited), or when
the active project resolves to another copy of it, the guard compiles guard.jl on its
own instead.
"""
function guard_command(o::GuardOptions)
    options = ["--interval=$(o.interval)"]
    isempty(o.fake_memory) || push!(options, "--fake-memory=$(o.fake_memory)")
    precompiled() ? package_guard(options) : script_guard(options)
end

function precompiled()
    id = Base.PkgId(@__MODULE__)
    ccall(:jl_generating_output, Cint, ()) == 0 && Base.locate_package(id) == pathof(@__MODULE__) &&
        Base.isprecompiled(id)
end

function package_guard(options::Vector{String})
    project = Base.active_project()
    flags = project === nothing ? String[] : ["--project=$project"]
    cmd = `$(Base.julia_cmd()) --threads=1 --startup-file=no --history-file=no --color=no $flags
           -e 'using SimplePipelines; SimplePipelines.guard_main()' -- $options`
    addenv(cmd, "JULIA_LOAD_PATH" => join(LOAD_PATH, ':'), "JULIA_DEPOT_PATH" => join(DEPOT_PATH, ':'))
end

function script_guard(options::Vector{String})
    julia = joinpath(Sys.BINDIR, Base.julia_exename())
    script = joinpath(pkgdir(@__MODULE__), "src", "guard.jl")
    `$julia --startup-file=no --history-file=no -O0 --threads=1 --color=no -e 'include(popfirst!(ARGS)); guard_main()' $script $options`
end

function start_guard(o::GuardOptions)
    errors = Pipe()
    process = open(pipeline(guard_command(o); stderr=errors), "r+")
    close(errors.in)
    tail = @async read_tail(errors, GUARD_STDERR_TAIL)
    hello, first_sample = first_lines(process)
    words = split(hello, ' ')
    if words[1] != "ready" || isempty(first_sample)
        kill(process, Base.SIGKILL)
        reason = isempty(hello) && process_running(process) ? "no answer within $(round(Int, GUARD_WAIT)) s" :
                 "it exited: " * strip(fetch(tail))
        error("could not start the resource guard ($reason). SimplePipelines does not run commands without ",
              "memory protection; pass guard=false to run without it (no memory limits, timeouts or cleanup).")
    end
    link = GuardLink(process, Int32(int_field(words, 2)), int_field(words, 4) == 1, tail, ReentrantLock(),
                     Dict{Int,Channel{Gone}}(), parse_sample(split(first_sample, ' ')), time(), :none)
    Threads.@spawn :interactive watch_guard(link)
    link
end

"""The `ready` line and the first `sample`, or empty strings after GUARD_WAIT seconds or EOF."""
function first_lines(process::Base.Process)
    got = Channel{Tuple{String,String}}(2)
    @async put!(got, (readline(process), readline(process)))
    timer = Timer(_ -> put!(got, ("", "")), GUARD_WAIT)
    lines = take!(got)
    close(timer)
    lines
end

"""Stop the guard (it kills what is left). The next `run` starts a new one."""
function stop_guard!()
    link = @atomic CURRENT.link
    @atomic CURRENT.link = nothing
    link === nothing || close_link!(link)
    nothing
end

function close_link!(link::GuardLink)
    (@atomicreplace link.ended :none => :closed).success || return
    run_safely(Call(close, (link.process.in,)))     # EOF: the guard kills its commands and exits
    timedwait(() -> process_exited(link.process), 5.0; pollint=0.01)
    kill(link.process, Base.SIGKILL)
    resolve_replies!(link, Gone(:stopped))
end

"""Test hook: replace the guard with one using `interval` seconds per tick and, optionally, a fake memory file."""
function restart_guard!(; interval::Real=0.5, fake_memory::AbstractString="")
    GUARD_OPTIONS[] = GuardOptions(Float64(interval), String(fake_memory))
    stop_guard!()
    connect(true)
end

#==============================================================================#
# Messages to the guard
#==============================================================================#

"""Write to the guard; a failed write means it is gone, and the run must stop."""
function send!(link::GuardLink, text::String)
    (@atomic link.ended) === :none || return nothing
    sent = run_safely(Call(write, (link.process, text)))
    sent.ok || guard_lost!(link, "writing to it failed: " * sent.value.message)
    nothing
end

begin_run!(link::GuardLink, run::Int, min_free_mb::Int, nice::Int) = send!(link, "run $run $min_free_mb $nice\n")
begin_run!(::NoGuard, ::Int, ::Int, ::Int) = nothing
end_run!(link::GuardLink, run::Int) = send!(link, "endrun $run\n")
end_run!(::NoGuard, ::Int) = nothing

"""
The guard's `step` line for a command: its limits and label. Built before the command
is spawned, so nothing can fail between spawning and registering it.
"""
step_line(id::Int, run::Int, n::Need, deadline_ms::Int, label::String) =
    string("step ", id, ' ', run, ' ', n.limit_mb, ' ', n.charge_mb, ' ', milliseconds(n.timeout), ' ',
           deadline_ms, ' ', replace(label, r"[\r\n]" => ' '), '\n')

milliseconds(seconds::Float64) = round(Int, 1000min(seconds, 2.0^31))

"""Tell the guard about a started command: its `step` line and the sessions of its leaders."""
function register!(link::GuardLink, id::Int, line::String, sids::Vector{Int32})
    reply = Channel{Gone}(Inf)
    with_lock(setindex!, link.lock, link.replies, reply, id)
    ended = @atomic link.ended
    ended === :none || (kill_all!(sids); put!(reply, Gone(ended === :interrupt ? :interrupt : :stopped)); return nothing)
    send!(link, line * join(("sess $id $sid\n" for sid in sids)))
end
register!(::NoGuard, ::Int, ::String, ::Vector{Int32}) = nothing

"""
Wait until the guard reports the command gone: once its leaders have exited (we then
send `done`) and no process of it is left; leftovers are terminated first. A command
stuck in uninterruptible I/O is reported without waiting for its leaders.
"""
function finish_step!(link::GuardLink, id::Int, process::Base.AbstractPipe)
    reply = with_lock(get, link.lock, link.replies, id, Channel{Gone}(1))
    @async (wait(process); send!(link, "done $id\n"))
    gone = await_gone(link, reply)
    gone.kind === :lost && guard_lost!(link, "it did not answer for $(round(Int, GUARD_WAIT)) s")
    with_lock(delete!, link.lock, link.replies, id)
    forget_sessions!(id)
    gone.kind === :lost ? Gone(:stopped) : gone
end

"""
The guard's reply. Its samples, every tick, are its heartbeat: it counts as lost only
after GUARD_WAIT seconds without a line, so time this process spends busy (and not
reading) does not count against it.
"""
function await_gone(link::GuardLink, reply::Channel{Gone})
    while true
        timer = Timer(_ -> put!(reply, Gone(:check)), GUARD_WAIT)
        gone = take!(reply)
        close(timer)
        gone.kind === :check || return gone
        yield()                                     # a starved reader catches up first
        process_exited(link.process) || time() - (@atomic link.heard) >= GUARD_WAIT && return Gone(:lost)
    end
end

function finish_step!(::NoGuard, id::Int, process::Base.AbstractPipe)
    wait(process)
    kill_all!(forget_sessions!(id))                 # nothing the step started outlives it
    Gone(with_lock(pop!, SESSIONS_LOCK, TERMINATED, id, :none))
end

"""Stop a running command: the guard terminates it and reports `reason` as its kind."""
terminate!(link::GuardLink, id::Int, reason::Symbol) = send!(link, "term $id $reason\n")
function terminate!(::NoGuard, id::Int, reason::Symbol)
    with_lock(setindex!, SESSIONS_LOCK, TERMINATED, reason, id)
    kill_all!(sessions_of(id))
end

#==============================================================================#
# Messages from the guard
#==============================================================================#

"""
Reader task: dispatch the guard's lines until it exits. It defers Ctrl-C, so an
interrupt goes to the task that is computing (in the REPL, the user's) instead of to
this one, which is woken by every sample; the guard reports a group-wide Ctrl-C itself.
"""
function watch_guard(link::GuardLink)
    Base.sigatomic_begin()
    read = run_safely(Call(read_guard, (link,)))
    read.ok || guard_lost!(link, "reading from it failed: " * read.value.message)
end

function read_guard(link::GuardLink)
    while true
        line = readline(link.process)
        isempty(line) && eof(link.process) && break
        @atomic link.heard = time()
        words = split(line, ' ')
        message!(Val(Symbol(words[1])), link, words)
    end
    (@atomic link.ended) === :none && guard_lost!(link, "it exited")
end

message!(::Val{:sample}, link::GuardLink, words) = on_sample(link, parse_sample(words))
message!(::Val{:gone}, link::GuardLink, words) =
    deliver!(link, int_field(words, 2), Gone(Symbol(field(words, 3)), int_field(words, 4), int_field(words, 5),
                                             field(words, 6) == "1", join(words[min(7, end + 1):end], ' ')))
message!(::Val{:warn}, ::GuardLink, words) = guard_warning(join(words[2:end], ' '))
# The guard got Ctrl-C (it ignores SIGTERM on Linux; a script's own exit kills the steps).
# A script exits from the same Ctrl-C: give it a moment, so that nothing runs (or
# compiles) here while Julia shuts down.
message!(::Val{:bye}, ::GuardLink, _) = (sleep(0.3); interrupt_all!())
message!(::Val, ::GuardLink, _) = nothing

field(words, i::Int) = i <= length(words) ? words[i] : ""
int_field(words, i::Int) = something(tryparse(Int, field(words, i)), 0)

function parse_sample(words)
    usage = Dict{Int,Int}()
    for pair in words[min(6, end + 1):end]
        colon = findfirst(':', pair)
        colon === nothing && continue
        usage[something(tryparse(Int, pair[1:colon-1]), 0)] = something(tryparse(Int, pair[colon+1:end]), 0)
    end
    GuardSample(int_field(words, 2), int_field(words, 3), field(words, 4) == "1", usage)
end

function deliver!(link::GuardLink, id::Int, gone::Gone)
    reply = with_lock(get, link.lock, link.replies, id, nothing)
    reply === nothing || put!(reply, gone)
    nothing
end

resolve_replies!(link::GuardLink, gone::Gone) =
    foreach(reply -> put!(reply, gone), with_lock(collect ∘ values, link.lock, link.replies))

"""
The guard is gone without being stopped: kill its commands directly and stop every run
using it, because commands must not run without memory protection.
"""
function guard_lost!(link::GuardLink, reason::String)
    (@atomicreplace link.ended :none => :stopped).success || return nothing
    kill(link.process, Base.SIGKILL)
    ids = with_lock(collect ∘ keys, link.lock, link.replies)
    kill_all!(reduce(vcat, map(forget_sessions!, ids); init=Int32[]))
    tail = strip(fetch(link.errors))
    failure = StepFailure(:stopped, "stopped: the resource guard exited unexpectedly")
    for rt in active_runs()
        rt.link === link || continue
        log_guard_lost(rt, link.pid, isempty(tail) ? reason : string(reason, ": ", tail))
        stop_run!(rt, failure)
    end
    resolve_replies!(link, Gone(:stopped))
end

"""
Ctrl-C (here or in the guard): stop every run of this process, kill every command
directly and drop the guard (the next `run` starts a new one). Safe to call repeatedly.
"""
function interrupt_all!()
    link = @atomic CURRENT.link
    ended = link !== nothing && (@atomicreplace link.ended :none => :interrupt).success  # nothing registers now
    failure = StepFailure(:stopped, "stopped: the run was interrupted")
    for rt in active_runs()
        rt.interrupted[] = true
        stop_run!(rt, failure)
        abandon_run!(rt)
    end
    kill_all!(take_all_sessions!())
    ended && (kill(link.process, Base.SIGKILL); resolve_replies!(link, Gone(:interrupt)))
    foreach(rt -> run_safely(Call(save_run_state!, (rt,))), active_runs())
end

@specialize
