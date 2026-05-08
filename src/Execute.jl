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

const STDERR_CAPTURE_BYTES = 64 * 1024

"""Execute a shell command string under sh -c with stdout/stderr captured separately."""
execute_shell(step::Step, ctx::RunContext, cmdstr::AbstractString) =
    capture_cmd(step, ctx, Cmd(["sh", "-c", cmdstr]), cmdstr)

function execute(step::Step{Cmd}, ctx::RunContext)
    exec = step.work.exec
    length(exec) >= 3 && exec[1] == "sh" && exec[2] == "-c" &&
        return execute_shell(step, ctx, exec[3])
    capture_cmd(step, ctx, step.work, step.work)
end

execute(step::Step{T}, ctx::RunContext) where {T<:Base.AbstractCmd} =
    capture_cmd(step, ctx, step.work, step.work)

execute(step::Step{<:ShRun}, ctx::RunContext) = execute_shell(step, ctx, step.work())

function capture_cmd(step::Step, ctx::RunContext, work::Base.AbstractCmd, log_target)
    start = time()
    err = path_check_inputs(step)
    err === nothing || return step_result(step, false, time() - start, err)
    log_cmd(ctx, log_target)
    ctx.auto_spill ? run_streaming(step, ctx, work, start) : run_inmem(step, work, start)
end

function run_streaming(step::Step, ctx::RunContext, work::Base.AbstractCmd, start::Float64)
    out_path = reserve_stdout_path(ctx.spill_dir)
    err_buf = IOBuffer()
    outcome = run_safely(() -> Base.run(Base.pipeline(work, stdout=out_path, stderr=err_buf)))
    elapsed = time() - start
    if !outcome.ok
        rm(out_path; force=true)
        return step_result(step, false, elapsed, "Error: $(outcome.value)\n$(stderr_tail(err_buf))")
    end
    out_err = path_check_outputs(step)
    if out_err !== nothing
        rm(out_path; force=true)
        return step_result(step, false, elapsed, out_err)
    end
    finalize_stdout(step, ctx, out_path, elapsed)
end

function finalize_stdout(step::Step, ctx::RunContext, out_path::String, elapsed::Float64)
    filesize(out_path) > ctx.spill_threshold_bytes &&
        return step_result(step, true, elapsed, SpilledStdout(out_path))
    s = read(out_path, String)
    rm(out_path; force=true)
    step_result(step, true, elapsed, s)
end

function run_inmem(step::Step, work::Base.AbstractCmd, start::Float64)
    out_buf = IOBuffer()
    err_buf = IOBuffer()
    outcome = run_safely(() -> Base.run(Base.pipeline(work, stdout=out_buf, stderr=err_buf)))
    elapsed = time() - start
    outcome.ok || return step_result(step, false, elapsed, "Error: $(outcome.value)\n$(String(take!(err_buf)))")
    out_err = path_check_outputs(step)
    out_err === nothing || return step_result(step, false, elapsed, out_err)
    step_result(step, true, elapsed, String(take!(out_buf)))
end

function stderr_tail(err_buf::IOBuffer)
    bytes = take!(err_buf)
    n = length(bytes)
    n <= STDERR_CAPTURE_BYTES && return String(bytes)
    dropped = n - STDERR_CAPTURE_BYTES
    String(resize!(bytes, STDERR_CAPTURE_BYTES)) * "\n[stderr truncated: $dropped bytes dropped]"
end

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
