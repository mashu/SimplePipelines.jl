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

"""Execute a shell command string under sh -c with stdout/stderr captured separately."""
function execute_shell(step::Step, ctx::RunContext, cmdstr::AbstractString)
    start = time()
    err = path_check_inputs(step)
    err !== nothing && return step_result(step, false, time() - start, err)
    log_cmd(ctx, cmdstr)
    out_buf = IOBuffer()
    err_buf = IOBuffer()
    outcome = run_safely() do
        Base.run(Base.pipeline(Cmd(["sh", "-c", cmdstr]), stdout=out_buf, stderr=err_buf))
    end
    elapsed = time() - start
    if !outcome.ok
        return step_result(step, false, elapsed, "Error: $(outcome.value)\n$(String(take!(err_buf)))")
    end
    out_err = path_check_outputs(step)
    out_err !== nothing && return step_result(step, false, elapsed, out_err)
    step_result(step, true, elapsed, String(take!(out_buf)))
end

# Cmd from sh"..." or sh(s) is ["sh", "-c", script]; share the shell path.
function execute(step::Step{Cmd}, ctx::RunContext)
    exec = step.work.exec
    length(exec) >= 3 && exec[1] == "sh" && exec[2] == "-c" &&
        return execute_shell(step, ctx, exec[3])
    execute_cmd(step, step.work, ctx)
end

# Any other AbstractCmd — including the OrCmds returned by `Base.pipeline(c1, c2, …)`
# (which is what `sh_pipe` builds). Stdout flows through OS pipes between the
# component commands; only the final stage's stdout is captured.
execute(step::Step{T}, ctx::RunContext) where {T<:Base.AbstractCmd} =
    execute_cmd(step, step.work, ctx)

# Internal: run an AbstractCmd, capturing final stdout / stderr.
function execute_cmd(step::Step, work::Base.AbstractCmd, ctx::RunContext)
    start = time()
    err = path_check_inputs(step)
    err !== nothing && return step_result(step, false, time() - start, err)
    log_cmd(ctx, work)
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

execute(step::Step{<:ShRun}, ctx::RunContext) = execute_shell(step, ctx, step.work.f())

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
    msg = "Step work must be a Cmd, Function, or ShRun. Got $(typeof(step.work)). " *
          "If you used `@step name(input) = process_file(...)`, the call runs at build time. " *
          "Use `@step name(\"path\") = process_file` (function without parentheses) so the " *
          "function receives the input at run time."
    step_result(step, false, 0.0, msg)
end
