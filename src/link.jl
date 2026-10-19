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

const GUARD_WAIT = 30.0             # seconds to wait for `ready`, and for `gone` after `done`
const GUARD_STDERR_TAIL = 16 * 1024

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
all_sessions() = with_lock(collect_sessions, SESSIONS_LOCK, SESSIONS)
collect_sessions(d::Dict{Int,Vector{Int32}}) = reduce(vcat, values(d); init=Int32[])

"""atexit hook: kill every command still running (script-mode Ctrl-C, SIGTERM, `exit`)."""
function kill_at_exit()
    trylock(SESSIONS_LOCK) || return    # busy: the guard sees EOF and kills them
    sids = collect_sessions(SESSIONS)
    unlock(SESSIONS_LOCK)
    kill_sessions!(sids)
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
    Sys.iswindows() && throw(ArgumentError(
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
package is being precompiled, or when its cache is stale (its source was edited), the
guard compiles guard.jl on its own instead.
"""
function guard_command(o::GuardOptions)
    options = ["--interval=$(o.interval)"]
    isempty(o.fake_memory) || push!(options, "--fake-memory=$(o.fake_memory)")
    precompiled() ? package_guard(options) : script_guard(options)
end

precompiled() = ccall(:jl_generating_output, Cint, ()) == 0 && Base.isprecompiled(Base.PkgId(@__MODULE__))

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
                     Dict{Int,Channel{Gone}}(), parse_sample(split(first_sample, ' ')), :none)
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

"""Tell the guard about a started command: its limits and the sessions of its leaders."""
function register!(link::GuardLink, g::Grant, run::Int, n::Need, deadline_ms::Int, label::String, sids::Vector{Int32})
    reply = Channel{Gone}(Inf)
    with_lock(setindex!, link.lock, link.replies, reply, g.id)
    ended = @atomic link.ended
    ended === :none || (kill_sessions!(sids); put!(reply, Gone(ended === :interrupt ? :interrupt : :stopped)); return nothing)
    io = IOBuffer()
    println(io, "step ", g.id, ' ', run, ' ', n.limit_mb, ' ', n.charge_mb, ' ', round(Int, 1000n.timeout), ' ',
            deadline_ms, ' ', replace(label, r"[\r\n]" => ' '))
    foreach(sid -> println(io, "sess ", g.id, ' ', sid), sids)
    send!(link, String(take!(io)))
end
register!(::NoGuard, ::Grant, ::Int, ::Need, ::Int, ::String, ::Vector{Int32}) = nothing

"""
After the command's leaders exited: wait until the guard reports the step gone, which
happens once no process of it is left (leftovers are terminated first).
"""
function finish_step!(link::GuardLink, id::Int)
    reply = with_lock(get, link.lock, link.replies, id, Channel{Gone}(1))
    send!(link, "done $id\n")
    timer = Timer(_ -> put!(reply, Gone(:lost)), GUARD_WAIT)
    gone = take!(reply)
    close(timer)
    gone.kind === :lost && guard_lost!(link, "it did not answer for $(round(Int, GUARD_WAIT)) s")
    with_lock(delete!, link.lock, link.replies, id)
    forget_sessions!(id)
    gone.kind === :lost ? Gone(:stopped) : gone
end

function finish_step!(::NoGuard, id::Int)
    foreach(kill_group, forget_sessions!(id))         # nothing the step started outlives it
    Gone(with_lock(pop!, SESSIONS_LOCK, TERMINATED, id, :none))
end

"""Stop a running command: the guard terminates it and reports `reason` as its kind."""
terminate!(link::GuardLink, id::Int, reason::Symbol) = send!(link, "term $id $reason\n")
function terminate!(::NoGuard, id::Int, reason::Symbol)
    with_lock(setindex!, SESSIONS_LOCK, TERMINATED, reason, id)
    foreach(kill_group, sessions_of(id))
end

kill_group(sid::Int32) = (sid > 1 && ccall(:kill, Cint, (Cint, Cint), -sid, 9); nothing)

#==============================================================================#
# Messages from the guard
#==============================================================================#

"""Reader task: dispatch the guard's lines until it exits."""
function watch_guard(link::GuardLink)
    read = run_safely(Call(read_guard, (link,)); interrupted=interrupt_all!)
    read.ok || guard_lost!(link, "reading from it failed: " * read.value.message)
end

function read_guard(link::GuardLink)
    while true
        line = readline(link.process)
        isempty(line) && eof(link.process) && break
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
message!(::Val{:bye}, ::GuardLink, _) = interrupt_all!()    # the guard got Ctrl-C or SIGTERM
message!(::Val, ::GuardLink, _) = nothing

field(words, i::Int) = i <= length(words) ? words[i] : ""
int_field(words, i::Int) = something(tryparse(Int, field(words, i)), 0)

function parse_sample(words)
    usage = Dict{Int,Int}()
    for pair in words[min(6, end + 1):end]
        id, mb = split(pair, ':'; limit=2)..., ""
        usage[something(tryparse(Int, id), 0)] = something(tryparse(Int, mb), 0)
    end
    GuardSample(int_field(words, 2), int_field(words, 3), field(words, 4) == "1", Symbol(field(words, 5)), usage)
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
    kill_sessions!(reduce(vcat, map(sessions_of, ids); init=Int32[]))
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
    failure = StepFailure(:stopped, "stopped: the run was interrupted")
    for rt in active_runs()
        rt.interrupted[] = true
        stop_run!(rt, failure)
        run_safely(Call(save_run_state!, (rt,)))
    end
    kill_sessions!(all_sessions())
    link = @atomic CURRENT.link
    link === nothing && return nothing
    (@atomicreplace link.ended :none => :interrupt).success || return nothing
    kill(link.process, Base.SIGKILL)
    resolve_replies!(link, Gone(:interrupt))
end

@specialize
