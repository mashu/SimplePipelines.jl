# Step execution: run_safely (sole try-catch boundary), declared-path readiness checks,
# execute(step, ctx). Declared inputs/outputs are always treated as filesystem paths.

function run_safely(f)::RunOutcome
    try
        RunOutcome(true, f())
    catch e
        RunOutcome(false, sprint(showerror, e))
    end
end

path_ready_error(path::String, is_input::Bool) =
    isfile(path) ? nothing : (is_input ? "Missing input: $path" : "Missing output: $path")

function path_check_inputs(step::Step)
    for inp in step.inputs
        err = path_ready_error(inp, true)
        err !== nothing && return err
    end
    nothing
end
function path_check_outputs(step::Step)
    for out_path in step.outputs
        err = path_ready_error(out_path, false)
        err !== nothing && return err
    end
    nothing
end

step_result(step::Step, success::Bool, elapsed::Float64, result) =
    StepResult(step, success, elapsed, step.inputs, step.outputs, result)

# Cap on stderr captured into RAM. A truncated tail is sufficient for an error
# message; without a cap a chatty failing command can fill memory before the
# step ever returns. Tunable via the `STDERR_CAPTURE_BYTES` const if you need
# fuller stderr for debugging.
const STDERR_CAPTURE_BYTES = 64 * 1024

"""Execute a shell command string under sh -c with stdout/stderr captured separately."""
execute_shell(step::Step, ctx::RunContext, cmdstr::AbstractString) =
    capture_cmd(step, ctx, Cmd(["sh", "-c", cmdstr]), cmdstr)

# Cmd from sh"..." or sh(s) is ["sh", "-c", script]; share the shell path.
function execute(step::Step{Cmd}, ctx::RunContext)
    exec = step.work.exec
    length(exec) >= 3 && exec[1] == "sh" && exec[2] == "-c" &&
        return execute_shell(step, ctx, exec[3])
    capture_cmd(step, ctx, step.work, step.work)
end

# Any other AbstractCmd — including the OrCmds returned by `Base.pipeline(c1, c2, …)`
# (which is what `sh_pipe` builds). Stdout flows through OS pipes between the
# component commands; only the final stage's stdout is captured.
execute(step::Step{T}, ctx::RunContext) where {T<:Base.AbstractCmd} =
    capture_cmd(step, ctx, step.work, step.work)

# Internal: run an AbstractCmd, streaming stdout to a tempfile so peak RAM is
# bounded even when the command produces gigabytes of output. After the process
# exits, if the stdout file is small (below `spill_threshold_bytes`) we read it
# back as a String and delete the file — preserving the legacy in-RAM result
# semantics for typical short outputs. Above the threshold we leave the file on
# disk and return a SpilledStdout wrapper so downstream steps materialise it
# lazily. With auto_spill disabled we fall back to the original IOBuffer path
# so opt-out really means "everything stays in RAM".
function capture_cmd(step::Step, ctx::RunContext, work::Base.AbstractCmd, log_target)
    start = time()
    err = path_check_inputs(step)
    err !== nothing && return step_result(step, false, time() - start, err)
    log_cmd(ctx, log_target)
    ctx.auto_spill || return capture_cmd_inmem(step, ctx, work, start)
    capture_cmd_streaming(step, ctx, work, start)
end

function capture_cmd_inmem(step::Step, ::RunContext, work::Base.AbstractCmd, start::Float64)
    out_buf = IOBuffer()
    err_buf = IOBuffer()
    outcome = run_safely() do
        Base.run(Base.pipeline(work, stdout=out_buf, stderr=err_buf))
    end
    elapsed = time() - start
    if !outcome.ok
        return step_result(step, false, elapsed, "Error: $(outcome.value)\n$(String(take!(err_buf)))")
    end
    out_err = path_check_outputs(step)
    out_err !== nothing && return step_result(step, false, elapsed, out_err)
    step_result(step, true, elapsed, String(take!(out_buf)))
end

function capture_cmd_streaming(step::Step, ctx::RunContext, work::Base.AbstractCmd, start::Float64)
    out_path = reserve_stdout_path(ctx.spill_dir)
    err_buf = IOBuffer()
    outcome = run_safely() do
        Base.run(Base.pipeline(work, stdout=out_path, stderr=err_buf))
    end
    elapsed = time() - start
    if !outcome.ok
        rm(out_path; force=true)
        return step_result(step, false, elapsed,
                           "Error: $(outcome.value)\n$(stderr_tail(err_buf))")
    end
    out_err = path_check_outputs(step)
    if out_err !== nothing
        rm(out_path; force=true)
        return step_result(step, false, elapsed, out_err)
    end
    sz = filesize(out_path)
    if sz <= ctx.spill_threshold_bytes
        s = read(out_path, String)
        rm(out_path; force=true)
        return step_result(step, true, elapsed, s)
    end
    step_result(step, true, elapsed, SpilledStdout(out_path))
end

# Bound the stderr we surface in error messages. The IOBuffer itself is still
# unbounded (Base.run writes whatever the process emits); we just don't keep it
# all in the StepResult.
function stderr_tail(err_buf::IOBuffer)
    bytes = take!(err_buf)
    n = length(bytes)
    n <= STDERR_CAPTURE_BYTES && return String(bytes)
    head = String(bytes[1:STDERR_CAPTURE_BYTES])
    head * "\n[stderr truncated: $(n - STDERR_CAPTURE_BYTES) bytes dropped]"
end

execute(step::Step{<:ShRun}, ctx::RunContext) = execute_shell(step, ctx, step.work())

function execute(step::Step{F}, ctx::RunContext) where {F<:Function}
    start = time()
    err = path_check_inputs(step)
    err !== nothing && return step_result(step, false, time() - start, err)
    outcome = run_safely() do
        isempty(step.inputs) ? step.work() : step.work(step.inputs...)
    end
    elapsed = time() - start
    if !outcome.ok
        return step_result(step, false, elapsed, "Error: $(outcome.value)")
    end
    out_err = path_check_outputs(step)
    out_err !== nothing && return step_result(step, false, elapsed, out_err)
    step_result(step, true, elapsed, outcome.value)
end

"""Run a function step with a single piped input (used by Pipe and Sequence data passing)."""
function execute(step::Step{F}, ctx::RunContext, input) where {F<:Function}
    start = time()
    outcome = run_safely() do
        step.work(input)
    end
    elapsed = time() - start
    if !outcome.ok
        return step_result(step, false, elapsed, "Error: $(outcome.value)")
    end
    out_err = path_check_outputs(step)
    out_err !== nothing && return step_result(step, false, elapsed, out_err)
    step_result(step, true, elapsed, outcome.value)
end

# Fallback for invalid work types: produce a clear failure StepResult, not a MethodError.
function execute(step::Step, ::RunContext)
    msg = "Step work must be an AbstractCmd (Cmd / OrCmds / …), Function, or ShRun. " *
          "Got $(typeof(step.work)). If you used `@step name(input) = process_file(...)`, " *
          "the call runs at build time. Use `@step name(\"path\") = process_file` (function " *
          "without parentheses) so the function receives the input at run time."
    step_result(step, false, 0.0, msg)
end
