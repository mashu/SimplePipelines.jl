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

"""Thread variables a command sets itself to something other than the granted threads."""
overridden(c::Base.AbstractCmd, threads::Int) =
    filter(e -> first(split(e, '='; limit=2)) in THREAD_VARIABLES && last(split(e, '='; limit=2)) != string(threads),
           own_environment(c))
own_environment(c::Cmd) = c.env === nothing ? String[] : c.env
own_environment(c::Union{Base.OrCmds,Base.ErrOrCmds,Base.AndCmds}) = [own_environment(c.a); own_environment(c.b)]
own_environment(c::Base.CmdRedirect) = own_environment(c.cmd)

"""
Start `cmd` and remember its leaders' sessions before anything can interrupt us: the
iolock keeps the exit callback from clearing a fast command's handle before its pid is
read, and deferring Ctrl-C keeps an interrupt from leaving the iolock held.
"""
function spawn_leaders(cmd::Base.AbstractCmd, out::Pipe, err::Pipe, id::Int)
    Base.sigatomic_begin()
    Base.iolock_begin()
    spawned = run_safely(Call(start_process, (cmd, out, err)))
    sids = leader_pids(spawned.value)
    Base.iolock_end()
    remember_sessions!(id, sids)
    Base.sigatomic_end()
    spawned, sids
end

start_process(cmd::Base.AbstractCmd, out::Pipe, err::Pipe) =
    run(pipeline(cmd; stdin=devnull, stdout=out, stderr=err); wait=false)

leader_pids(p::Base.Process) = p.handle == C_NULL ? Int32[] : Int32[ccall(:jl_uv_process_pid, Int32, (Ptr{Cvoid},), p.handle)]
leader_pids(c::Base.ProcessChain) = reduce(vcat, map(leader_pids, c.processes); init=Int32[])
leader_pids(::StepFailure) = Int32[]

"""Run a command to completion: Outcome(true, stdout), Outcome(false, StepFailure) or Evicted."""
function run_command(cmd::Base.AbstractCmd, l::Launch)
    rt, id = l.rt, l.grant.id
    conflicts = overridden(cmd, l.need.threads)
    isempty(conflicts) || log_thread_override(rt, l.label, conflicts, l.need.threads)
    out, err = Pipe(), Pipe()
    spawned, sids = spawn_leaders(confine(cmd, thread_variables(l.need.threads)), out, err, id)
    spawned.ok || (close(out); close(err); return spawned)
    close(out.in)
    close(err.in)
    register!(rt.link, l.grant, rt.id, l.need, remaining_ms(l.watchdog), l.label, sids)
    watch!(l.watchdog, id) && terminate!(rt.link, id, :node_timeout)
    stdout_task = @async read_capped(out, STDOUT_LIMIT, () -> terminate!(rt.link, id, :stdout))
    stderr_task = @async read_tail(err, STDERR_TAIL)
    wait(spawned.value)
    gone = finish_step!(rt.link, id)        # returns once no process of the command is left
    unwatch!(l.watchdog, id)
    measures(rt.link) && (l.grant.peak_mb = gone.peak_mb)
    command_outcome(spawned.value, gone, fetch(stdout_task), fetch(stderr_task), l)
end

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

command_outcome(p, gone::Gone, out::Vector{UInt8}, err::String, l::Launch) = ended(Val(gone.kind), p, gone, out, err, l)

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
    "End the script with `wait`, or start them with `setsid` if they must outlive the step", err) :
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
function exit_status(c::Base.ProcessChain)
    i = findfirst(!success, c.processes)
    i === nothing ? (0, 1) : exit_status(c.processes[i])
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
