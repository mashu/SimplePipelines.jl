# Leaf execution: running a step's command or function. Templates are rendered when the
# step starts, from what it was granted. Every command leader starts in its own session
# with the thread variables set, and is registered with the resource guard, which
# measures and terminates its whole process tree. Output streams through pipes: stdout
# is captured up to STDOUT_LIMIT (the command is stopped beyond that) and only the tail
# of stderr is kept, so memory stays bounded however much a tool prints.

@nospecialize   # see runtime.jl

const STDOUT_LIMIT = 32 * 1024^2
const STDERR_TAIL = 16 * 1024

"""What starting, registering and reporting one step's command needs."""
struct Launch
    label::String
    need::Need
    grant::Grant
    watchdog::Watchdog
    rt::Runtime
end

"""A command the guard stopped to free memory; the step is queued again (exec_step.jl)."""
struct Evicted
    peak_mb::Int
    usage_mb::Int
    available_mb::Int
    reserve_mb::Int
    alone::Bool                     # nothing else was running: it cannot fit
end

"""Do the work of `step` once its resources are granted. Never called for up-to-date steps."""
function perform(step::Step, l::Launch, input)
    work = launch_work(step.work, l.need)
    log_start(l.rt, l.label, work)
    for o in step.outputs
        dir = dirname(o)
        isempty(dir) || mkpath(dir)
    end
    for i in step.inputs
        ispath(i) || return Outcome(false, StepFailure(:missing_input, "missing input $i"))
    end
    checked_outputs(invoke_work(work, l, input), step)
end

checked_outputs(e::Evicted, ::Step) = e
function checked_outputs(out::Outcome, step::Step)
    out.ok || return out
    for o in step.outputs
        ispath(o) || return Outcome(false, StepFailure(:missing_output,
            "the step finished but did not create its output $o"))
    end
    out
end

#==============================================================================#
# Rendering at launch
#==============================================================================#

"""The work as it runs: templates filled with the granted threads and memory limit."""
launch_work(t::Template, n::Need) = Shell(render(t.script, t.ctx, n.threads, n.limit_mb))
launch_work(j::JuliaScript, n::Need) = Shell(julia_script(render(j.template, j.ctx, n.threads, n.limit_mb), n))
launch_work(w, ::Need) = w

"""
Shell line of a `julia(...)` step: this Julia, the active project, the granted threads
and a heap size hint below the memory limit (Julia exceeds the hint by its startup and
GC slack). The user's arguments come last, so their flags take precedence.
"""
function julia_script(arguments::String, n::Need)
    words = String[shell_quote(x) for x in Base.julia_cmd().exec]
    push!(words, "--startup-file=no")
    project = Base.active_project()
    project === nothing || push!(words, shell_quote("--project=" * dirname(project)))
    push!(words, "--threads=$(n.threads)")
    hint = n.limit_mb - max(300, n.limit_mb ÷ 5)
    n.limit_mb > 0 && hint >= 256 && push!(words, "--heap-size-hint=$(hint)M")
    string("exec ", join(words, ' '), ' ', arguments)
end

invoke_work(w::Union{Shell,Base.AbstractCmd}, l::Launch, _) = run_command(command(w), l)
invoke_work(f::Function, l::Launch, input) = after_call(call_with(f, arguments(input)), l)
invoke_work(c::Call, l::Launch, _) = after_call(call_with(c.f, c.args), l)
invoke_work(::Nothing, ::Launch, _) = succeeded(nothing)

arguments(::Nothing) = ()
arguments(x) = (x,)

"""Call `f` with `args` when it accepts them, otherwise with no arguments."""
call_with(f, args::Tuple) = applicable(f, args...) || !applicable(f) ? f(args...) : f()

# A function that returns a command has that command run as part of the same step.
after_call(cmd::Union{Shell,Base.AbstractCmd}, l::Launch) = run_command(command(cmd), l)
after_call(value, ::Launch) = succeeded(value)

#==============================================================================#
# Starting commands
#==============================================================================#

# Thread counts of common runtimes. They are advisory (a tool may ignore them), but they
# stop OpenMP/BLAS/Rayon programs from starting one thread per core in every step.
const THREAD_VARIABLES = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS", "RAYON_NUM_THREADS", "JULIA_NUM_THREADS",
    "GOMAXPROCS", "POLARS_MAX_THREADS", "BLIS_NUM_THREADS")

thread_variables(threads::Int) = [name => string(threads) for name in THREAD_VARIABLES]

"""Every leaf of a command, in its own session (setsid) and with the thread variables set."""
confine(c::Cmd, vars) = Cmd(addenv(c, vars...); detach=true)
confine(c::Base.OrCmds, vars) = Base.OrCmds(confine(c.a, vars), confine(c.b, vars))
confine(c::Base.ErrOrCmds, vars) = Base.ErrOrCmds(confine(c.a, vars), confine(c.b, vars))
confine(c::Base.AndCmds, vars) = Base.AndCmds(confine(c.a, vars), confine(c.b, vars))
confine(c::Base.CmdRedirect, vars) = Base.CmdRedirect(confine(c.cmd, vars), c.handle, c.stream_no, c.readable)

"""Thread variables a command sets itself (not inherited) to something other than the granted threads."""
overridden(c::Base.AbstractCmd, threads::Int) = filter(e -> conflicting(split(e, '='; limit=2), threads), own_environment(c))
conflicting(entry, threads::Int) = length(entry) == 2 && entry[1] in THREAD_VARIABLES &&
    entry[2] != string(threads) && get(ENV, entry[1], nothing) != entry[2]
own_environment(c::Cmd) = c.env === nothing ? String[] : c.env
own_environment(c::Union{Base.OrCmds,Base.ErrOrCmds,Base.AndCmds}) = [own_environment(c.a); own_environment(c.b)]
own_environment(c::Base.CmdRedirect) = own_environment(c.cmd)

"""
Why a command cannot start (a missing program or directory), checked before anything is
spawned, for a clear message.
"""
unstartable(c::Cmd) = isempty(c.exec) ? "the command is empty" :
    !isempty(c.dir) && !isdir(c.dir) ? "its directory $(c.dir) does not exist" :
    program_missing(first(c.exec), c) ? "`$(first(c.exec))` was not found; check the spelling and that it is installed and on PATH" :
    nothing
unstartable(c::Union{Base.OrCmds,Base.ErrOrCmds,Base.AndCmds}) = something(unstartable(c.a), unstartable(c.b), Some(nothing))
unstartable(c::Base.CmdRedirect) = unstartable(c.cmd)

# A command with its own PATH is not checked: we cannot know where it looks.
program_missing(program::String, c::Cmd) =
    occursin('/', program) ? !Sys.isexecutable(isempty(c.dir) ? program : joinpath(c.dir, program)) :
    !own_path(c) && Sys.which(program) === nothing
own_path(c::Cmd) = c.env !== nothing && any(e -> startswith(e, "PATH="), c.env)

"""A spawn failure, without the command's environment (which may hold secrets) in the message."""
spawn_failure(f::StepFailure, cmd::Base.AbstractCmd) = StepFailure(:process_failed,
    "could not start `$(shorten(command_text(cmd), 200))`: " * something(spawn_reason(f.message),
        "it could not be executed (a script without a #! line, or a program built for another system?)"))
spawn_reason(message::String) = (m = match(r"\): ([^():]+ \([A-Z]+\))$", message); m === nothing ? nothing : String(m[1]))

"""
Start `cmd` and remember its leaders' sessions before anything can interrupt us: the
iolock keeps the exit callback from clearing a fast command's handle before its pid is
read, and deferring Ctrl-C keeps an interrupt from leaving the iolock held. When a later
leaf of a pipeline cannot start, the leaves already started are killed.
"""
function spawn_leaders(cmd::Base.AbstractCmd, out::Pipe, err::Pipe, id::Int)
    started, links = Base.Process[], RawFD[]
    Base.sigatomic_begin()
    Base.iolock_begin()
    spawned = run_safely(Call(GUARD_SUPPORTED ? start_leaves! : start_whole!, (started, links, cmd, devnull, out, err)))
    sids = reduce(vcat, map(leader_pids, started); init=Int32[])
    Base.iolock_end()
    remember_sessions!(id, sids)
    Base.sigatomic_end()
    foreach(close_fd, links)
    spawned.ok || kill_all!(forget_sessions!(id))
    spawned, started, sids
end

"""
Start the leaves of a command one at a time, each in the list before the next starts
(Base starts a whole pipeline at once and loses the started part when a later leaf fails).
Leaves are joined by blocking, close-on-exec pipes, closed here once both ends are given.
"""
start_leaves!(started, links, c::Cmd, in, out, err) =
    (push!(started, run(pipeline(c; stdin=in, stdout=out, stderr=err); wait=false)); nothing)
function start_leaves!(started, links, c::Base.OrCmds, in, out, err)
    r, w = leaf_pipe!(links)
    start_leaves!(started, links, c.a, in, w, err)
    start_leaves!(started, links, c.b, r, out, err)
end
function start_leaves!(started, links, c::Base.ErrOrCmds, in, out, err)
    r, w = leaf_pipe!(links)
    start_leaves!(started, links, c.a, in, out, w)
    start_leaves!(started, links, c.b, r, out, err)
end
start_leaves!(started, links, c::Base.AndCmds, in, out, err) =
    (start_leaves!(started, links, c.a, in, out, err); start_leaves!(started, links, c.b, in, out, err))
start_leaves!(started, links, c::Base.CmdRedirect, in, out, err) =
    redirected!(started, links, c, Val(c.stream_no), in, out, err)
redirected!(started, links, c, ::Val{0}, in, out, err) = start_leaves!(started, links, c.cmd, c.handle, out, err)
redirected!(started, links, c, ::Val{1}, in, out, err) = start_leaves!(started, links, c.cmd, in, c.handle, err)
redirected!(started, links, c, ::Val{2}, in, out, err) = start_leaves!(started, links, c.cmd, in, out, c.handle)
redirected!(_, _, c, ::Val, _, _, _) = throw(ArgumentError("cannot redirect file descriptor $(c.stream_no) of a step's command"))

# Windows (no guard, no sessions): Base starts the whole command.
start_whole!(started, _, cmd, in, out, err) = add_processes!(started, run(pipeline(cmd; stdin=in, stdout=out, stderr=err); wait=false))
add_processes!(started, p::Base.Process) = (push!(started, p); nothing)
add_processes!(started, c::Base.ProcessChain) = (append!(started, c.processes); nothing)

function leaf_pipe!(links::Vector{RawFD})
    fds = Cint[-1, -1]
    ccall(:uv_pipe, Cint, (Ptr{Cint}, Cint, Cint), fds, 0, 0) == 0 || error("could not create a pipe")
    push!(links, RawFD(fds[1]), RawFD(fds[2]))
    RawFD(fds[1]), RawFD(fds[2])
end
close_fd(fd::RawFD) = (ccall(:close, Cint, (RawFD,), fd); nothing)

leader_pids(p::Base.Process) = p.handle == C_NULL ? Int32[] : Int32[ccall(:jl_uv_process_pid, Int32, (Ptr{Cvoid},), p.handle)]

"""Run a command to completion: Outcome(true, stdout), Outcome(false, StepFailure) or Evicted."""
function run_command(cmd::Base.AbstractCmd, l::Launch)
    problem = unstartable(cmd)
    problem === nothing || return Outcome(false, StepFailure(:process_failed, "could not start the command: " * problem))
    rt, id = l.rt, l.grant.id
    conflicts = overridden(cmd, l.need.threads)
    isempty(conflicts) || log_thread_override(rt, l.label, conflicts, l.need.threads)
    line = step_line(id, rt.id, l.need, remaining_ms(l.watchdog), l.label)
    out, err = capture_pipe(), capture_pipe()
    spawned, procs, sids = spawn_leaders(confine(cmd, thread_variables(l.need.threads)), out, err, id)
    spawned.ok || (close(out); close(err); return Outcome(false, spawn_failure(spawned.value, cmd)))
    close(out.in)
    close(err.in)
    register!(rt.link, id, line, sids)
    watch!(l.watchdog, id) && terminate!(rt.link, id, :node_timeout)
    stdout_task = @async read_capped(out, STDOUT_LIMIT, () -> terminate!(rt.link, id, :stdout))
    stderr_task = @async read_tail(err, STDERR_TAIL)
    gone = finish_step!(rt.link, id, procs)             # once no process of the command is left
    unwatch!(l.watchdog, id)
    measures(rt.link) && (l.grant.peak_mb = gone.peak_mb)
    held = !drained(stdout_task, stderr_task, out, err)
    outcome = command_outcome(procs, gone, fetch(stdout_task), fetch(stderr_task), l)
    held ? output_held(outcome) : outcome
end

"""A pipe for a command's output, ready before any leaf starts (a leaf may redirect elsewhere)."""
capture_pipe() = (p = Pipe(); Base.link_pipe!(p; reader_supports_async=true, writer_supports_async=false); p)

"""
Whether the output readers finished on their own. Every process of the step is gone, so
a writer still holding the pipes escaped the step (a daemon); stop reading after a
moment instead of waiting for it.
"""
function drained(stdout_task::Task, stderr_task::Task, out::Pipe, err::Pipe)
    finished() = istaskdone(stdout_task) && istaskdone(stderr_task)
    timedwait(finished, 1.0; pollint=0.01) === :ok && return true
    close(out)
    close(err)
    false
end

output_held(out::Outcome) = out.ok ? Outcome(false, StepFailure(:process_failed,
    "a process that left the step (a daemon) still holds its output open; redirect its output to a file " *
    "(`cmd > log 2>&1 &`), or better, do not leave processes running")) : out
output_held(e::Evicted) = e

function read_capped(io::IO, limit::Int, on_overflow)
    buf = IOBuffer()
    total = 0
    while !eof(io)
        chunk = readavailable(io)
        total += length(chunk)
        if total <= limit
            write(buf, chunk)
        elseif total - length(chunk) <= limit
            on_overflow()          # once; keep draining so the process can exit
        end
    end
    take!(buf)
end

function read_tail(io::IO, n::Int)
    buf = UInt8[]
    dropped = false
    while !eof(io)
        append!(buf, readavailable(io))
        if length(buf) > 2n
            deleteat!(buf, 1:length(buf)-n)
            dropped = true
        end
    end
    length(buf) > n && (deleteat!(buf, 1:length(buf)-n); dropped = true)
    dropped ? "…" * String(buf) : String(buf)
end

#==============================================================================#
# How a command ended
#==============================================================================#

command_outcome(p::Vector{Base.Process}, gone::Gone, out::Vector{UInt8}, err::String, l::Launch) =
    ended(Val(gone.kind), p, gone, out, err, l)

failed(kind::Symbol, message::AbstractString, err::String) = Outcome(false, StepFailure(kind, message; detail=err))

ended(::Val{:none}, p, gone, out, err, l) = success(p) ? succeeded(String(out)) : Outcome(false, exit_failure(p, gone, err, l))
ended(::Val, p, gone, out, err, l) = ended(Val(:none), p, gone, out, err, l)
ended(::Val{:evicted}, p, gone, out, err, l) = evicted(gone, l)
ended(::Val{:critical}, p, gone, out, err, l) = evicted(gone, l)
ended(::Val{:memory_limit}, p, gone, out, err, l) = failed(:memory_limit,
    "used $(gone.usage_mb) MiB of memory, more than its mem_mb = $(l.need.limit_mb) MiB$(limit_source(l.need)); " *
    "raise mem_mb or lower the tool's own memory setting (per-thread flags such as samtools sort -m are " *
    "multiplied by threads)", err)
ended(::Val{:step_timeout}, p, gone, out, err, l) =
    failed(:timed_out, "ran longer than its timeout = $(l.need.timeout) s", err)
ended(::Val{:node_timeout}, p, gone, out, err, l) =
    failed(:timed_out, "terminated: the enclosing Timeout ($(timeout_seconds(tightest(l.watchdog))) s) expired", err)
ended(::Val{:stdout}, p, gone, out, err, l) = failed(:stdout_limit,
    "the command printed more than $(STDOUT_LIMIT ÷ 1024^2) MiB to stdout; redirect large output to a file " *
    "(`cmd > out.txt`) and declare it as an output", err)
ended(::Val{:leftover}, p, gone, out, err, l) = success(p) ? failed(:process_failed,
    "the command finished but left background processes running ($(gone.detail)); they were terminated. " *
    "End the script with `wait`; an output process substitution `>(...)` cannot be waited for, so write to " *
    "a file and process it in a following command", err) :
    Outcome(false, exit_failure(p, gone, err, l))
ended(::Val{:unkillable}, p, gone, out, err, l) =
    failed(:process_failed, "could not be terminated: $(gone.detail) is stuck in uninterruptible I/O", err)
ended(::Val{:interrupt}, p, gone, out, err, l) = failed(:stopped,
    l.rt.interrupted[] ? "stopped: the run was interrupted" : "stopped: the run ended before this step finished", err)
ended(::Val{:stopped}, p, gone, out, err, l) = failed(:stopped, "stopped: the resource guard exited unexpectedly", err)

function evicted(gone::Gone, l::Launch)
    s = latest_sample(l.rt.link)
    Evicted(gone.peak_mb, gone.usage_mb, s.available_mb, s.reserve_mb, gone.alone)
end

limit_source(n::Need) = n.source === :scope ? " (the with_resources default)" :
                        n.source === :clamped ? " (clamped from $(n.declared_mb) MiB to the run's mem_mb budget)" : ""

timeout_seconds(w::Watchdog) = w.seconds
timeout_seconds(::Nothing) = 0.0

function exit_failure(p, gone::Gone, err::String, l::Launch)
    signal, code = exit_status(p)
    signal == 9 || code == 137 ? StepFailure(:out_of_memory,
        "was killed by SIGKILL (exit 137), most likely by the system's out-of-memory killer" *
        (measures(l.rt.link) ? "; peak memory $(gone.peak_mb) MiB" : ""); detail=err) :
    signal == 15 || code == 143 ? StepFailure(:process_failed,
        "was terminated by SIGTERM (exit 143) from outside the pipeline"; detail=err) :
    StepFailure(:process_failed, signal != 0 ? "command was killed by signal $signal" : "command exited with code $code";
                detail=err)
end

exit_status(p::Base.Process) = (Int(p.termsignal), Int(p.exitcode))
function exit_status(procs::Vector{Base.Process})
    i = findfirst(!success, procs)
    i === nothing ? (0, 1) : exit_status(procs[i])
end

"""Remove declared output files written by a failed attempt, so they never look up to date."""
function discard_outputs(step::Step, started::Float64)
    removed = String[]
    for o in step.outputs
        isfile(o) && mtime(o) >= floor(started) || continue
        rm(o; force=true)
        push!(removed, o)
    end
    removed
end

@specialize
