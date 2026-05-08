# Step execution: run_safely (sole try-catch boundary), declared-path readiness checks,
# execute(step, ctx). Declared inputs/outputs are always treated as filesystem paths.

function run_safely(f)::RunOutcome
    try
        RunOutcome(true, f())
    catch e
        RunOutcome(false, StepFailure(:exception, sprint(showerror, e)))
    end
end

function check_paths_on_disk(paths::Vector{String}, kind::Symbol, label::AbstractString)
    for path in paths
        isfile(path) || return StepFailure(kind, "$label: $path")
    end
    nothing
end

path_check_inputs(step::Step)  = check_paths_on_disk(step.inputs,  :missing_input,  "Missing input")
path_check_outputs(step::Step) = check_paths_on_disk(step.outputs, :missing_output, "Missing output")

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
    fail = cmd_step_failure(step, outcome, elapsed, stderr_tail(err_buf))
    fail === nothing || (rm(out_path; force=true); return fail)
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
    fail = cmd_step_failure(step, outcome, elapsed, String(take!(err_buf)))
    fail === nothing || return fail
    step_result(step, true, elapsed, String(take!(out_buf)))
end

# Shared post-run check: process failure → :process_failed, missing outputs →
# :missing_output. Returns the failure StepResult or `nothing` on success.
function cmd_step_failure(step::Step, outcome::RunOutcome, elapsed::Float64, stderr_text::AbstractString)
    outcome.ok || return step_result(step, false, elapsed,
                                     StepFailure(:process_failed, "Error while running command";
                                                 detail="$(string(outcome.value))\n$stderr_text"))
    out_err = path_check_outputs(step)
    out_err === nothing && return nothing
    step_result(step, false, elapsed, out_err)
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
    err === nothing || return step_result(step, false, time() - start, err)
    invocation = isempty(step.inputs) ? (() -> step.work()) : (() -> step.work(step.inputs...))
    finish_function_step(step, start, run_safely(invocation))
end

"""Run a function step with a single piped input (used by Pipe and Sequence data passing)."""
execute(step::Step{F}, ::RunContext, input) where {F<:Function} =
    finish_function_step(step, time(), run_safely(() -> step.work(input)))

# Wrap a function step's outcome into a StepResult. Centralises the elapsed-time
# computation, exception-failure construction, and output-path check.
function finish_function_step(step::Step, start::Float64, outcome::RunOutcome)
    elapsed = time() - start
    outcome.ok || return step_result(step, false, elapsed,
                                     StepFailure(:exception, "Error while running function step";
                                                 detail=string(outcome.value)))
    out_err = path_check_outputs(step)
    out_err === nothing || return step_result(step, false, elapsed, out_err)
    step_result(step, true, elapsed, outcome.value)
end

# Fallback for invalid work types: produce a clear failure StepResult, not a MethodError.
function execute(step::Step, ::RunContext)
    msg = """Step work must be an AbstractCmd (Cmd / OrCmds / …), Function, or ShRun. \
             If you used `@step name(input) = process_file(...)`, the call runs at build \
             time. Use `@step name("path") = process_file` (function without parentheses) \
             so the function receives the input at run time."""
    step_result(step, false, 0.0, StepFailure(:invalid_step_work, msg))
end
