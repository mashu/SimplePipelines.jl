# Leaf execution: running a step's command or function. Commands stream their output
# through pipes: stdout is captured up to STDOUT_LIMIT (the process is stopped beyond
# that) and only the tail of stderr is kept, so memory stays bounded however much a
# tool prints.

@nospecialize   # see runtime.jl

const STDOUT_LIMIT = 32 * 1024^2
const STDERR_TAIL = 16 * 1024

"""Do the work of `step` once its resources are granted. Never called for up-to-date steps."""
function perform(step::Step, watchdog::Watchdog, input)
    for o in step.outputs
        dir = dirname(o)
        isempty(dir) || mkpath(dir)
    end
    for i in step.inputs
        ispath(i) || return Outcome(false, StepFailure(:missing_input, "missing input $i"))
    end
    out = invoke_work(step.work, watchdog, input)
    out.ok || return out
    for o in step.outputs
        ispath(o) || return Outcome(false, StepFailure(:missing_output,
            "the step finished but did not create its output $o"))
    end
    out
end

invoke_work(w::Union{Shell,Base.AbstractCmd}, watchdog, _) = run_command(command(w), watchdog)
invoke_work(f::Function, watchdog, input) = after_call(call_with(f, arguments(input)), watchdog)
invoke_work(c::Call, watchdog, _) = after_call(call_with(c.f, c.args), watchdog)
invoke_work(::Nothing, _, _) = succeeded(nothing)

arguments(::Nothing) = ()
arguments(x) = (x,)

"""Call `f` with `args` when it accepts them, otherwise with no arguments."""
call_with(f, args::Tuple) = applicable(f, args...) || !applicable(f) ? f(args...) : f()

# A function that returns a command has that command run as part of the same step.
after_call(cmd::Union{Shell,Base.AbstractCmd}, watchdog) = run_command(command(cmd), watchdog)
after_call(value, _) = succeeded(value)

"""Run a command to completion: Outcome(true, stdout) or Outcome(false, StepFailure)."""
function run_command(cmd::Base.AbstractCmd, watchdog::Watchdog)
    out, err = Pipe(), Pipe()
    process, handle = launch(cmd, out, err)
    close(out.in)
    close(err.in)
    overflow = Threads.Atomic{Bool}(false)
    stdout_task = @async read_capped(out, STDOUT_LIMIT, () -> (overflow[] = true; terminate(handle)))
    stderr_task = @async read_tail(err, STDERR_TAIL)
    watch!(watchdog, handle)
    wait(process)
    unwatch!(watchdog, handle)
    stdout_bytes, stderr_text = fetch(stdout_task), fetch(stderr_task)
    success(process) && return succeeded(String(stdout_bytes))
    command_failure(process, overflow[], expired(watchdog), stderr_text)
end

# Plain commands run in their own process group so a timeout can stop the shell and
# everything it started; other AbstractCmds (pipelines of Cmds) are killed per process.
function launch(cmd::Cmd, out, err)
    process = run(pipeline(Cmd(cmd; detach=true); stdin=devnull, stdout=out, stderr=err); wait=false)
    process, ProcessGroup(launched_pid(process))
end

# `getpid` throws when a fast command has already exited and been reaped; this
# returns 0 instead, which `terminate` treats as "nothing left to stop".
function launched_pid(p::Base.Process)
    Base.iolock_begin()
    pid = p.handle == C_NULL ? Int32(0) : ccall(:jl_uv_process_pid, Int32, (Ptr{Cvoid},), p.handle)
    Base.iolock_end()
    pid
end
function launch(cmd::Base.AbstractCmd, out, err)
    process = run(pipeline(cmd; stdin=devnull, stdout=out, stderr=err); wait=false)
    process, process
end

function command_failure(process, overflow::Bool, timed_out::Bool, stderr_text::String)
    overflow && return Outcome(false, StepFailure(:stdout_limit,
        "the command printed more than $(STDOUT_LIMIT ÷ 1024^2) MiB to stdout; redirect large output to a file " *
        "(`cmd > out.txt`) and declare it as an output"; detail=stderr_text))
    timed_out && return Outcome(false, StepFailure(:timed_out, "terminated: the enclosing Timeout expired";
                                                   detail=stderr_text))
    Outcome(false, StepFailure(:process_failed, "command " * exit_text(process); detail=stderr_text))
end

exit_text(p::Base.Process) = p.termsignal != 0 ? "was killed by signal $(p.termsignal)" : "exited with code $(p.exitcode)"
function exit_text(c::Base.ProcessChain)
    i = findfirst(!success, c.processes)
    i === nothing ? "failed" : exit_text(c.processes[i])
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
