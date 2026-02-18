# Step execution: run_safely, path/input/output readiness, execute(step). Requires Types.jl, State.jl.

function run_safely(f)::RunOutcome
    try
        RunOutcome(true, f())
    catch e
        RunOutcome(false, sprint(showerror, e))
    end
end

path_like(s::String) = occursin(r"[/\\]", s) || occursin('.', s)
path_ready_error(path::String, is_input::Bool) = path_like(path) && !isfile(path) ? (is_input ? "Missing input: $path" : "Missing output: $path") : nothing
path_ready_error(_, _) = nothing

"""Single execution path for shell steps: input checks, optional log, run sh -c, output checks."""
function execute_shell(step::Step, cmdstr::AbstractString; verbose=nothing)
    start = time()
    for inp in step.inputs
        err = path_ready_error(inp, true)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    verbose isa Verbose && log_cmd(verbose, cmdstr)
    buf = IOBuffer()
    outcome = run_safely() do
        Base.run(Base.pipeline(Cmd(["sh", "-c", cmdstr]), stdout=buf, stderr=buf))
    end
    if !outcome.ok
        return StepResult(step, false, time() - start, step.inputs, step.outputs, "Error: $(outcome.value)\n$(String(take!(buf)))")
    end
    for out_path in step.outputs
        err = path_ready_error(out_path, false)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    StepResult(step, true, time() - start, step.inputs, step.outputs, String(take!(buf)))
end

# Cmd from sh"..." or sh(s) is ["sh", "-c", script]; use shared path. Raw backtick Cmd runs directly.
function execute(step::Step{Cmd}; verbose=nothing)
    exec = step.work.exec
    if length(exec) >= 3 && exec[1] == "sh" && exec[2] == "-c"
        return execute_shell(step, exec[3]; verbose)
    end
    start = time()
    for inp in step.inputs
        err = path_ready_error(inp, true)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    verbose isa Verbose && log_cmd(verbose, step.work)
    buf = IOBuffer()
    outcome = run_safely() do; Base.run(Base.pipeline(step.work, stdout=buf, stderr=buf)); end
    if !outcome.ok
        return StepResult(step, false, time() - start, step.inputs, step.outputs, "Error: $(outcome.value)\n$(String(take!(buf)))")
    end
    for out_path in step.outputs
        err = path_ready_error(out_path, false)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    StepResult(step, true, time() - start, step.inputs, step.outputs, String(take!(buf)))
end
execute(step::Step{<:ShRun}; verbose=nothing) = execute_shell(step, step.work.f(); verbose)

function execute(step::Step{Nothing})
    StepResult(step, false, 0.0, step.inputs, step.outputs, "Step has no work (ForEach block returned nothing). The block must return a Step or node, e.g. @step name = sh\"cmd\".")
end

function execute(step::Step{F}; verbose=nothing) where {F<:Function}
    start = time()
    for inp in step.inputs
        err = path_ready_error(inp, true)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    outcome = run_safely() do
        isempty(step.inputs) ? step.work() : step.work(step.inputs...)
    end
    if !outcome.ok
        return StepResult(step, false, time() - start, step.inputs, step.outputs, "Error: $(outcome.value)")
    end
    for out_path in step.outputs
        err = path_ready_error(out_path, false)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    StepResult(step, true, time() - start, step.inputs, step.outputs, outcome.value)
end

function execute(step::Step{T}) where T
    error(
        "Step work must be a command (Cmd), a function, or nothing. Got $(typeof(step.work)). " *
        "If you used @step name(input) = process_file(...), the call runs at build time. " *
        "Use @step name(\"path\") = process_file (function without parentheses) so the function receives the input at run time."
    )
end

"""Run a function step with a single piped input (used by Pipe). Skips file-input checks."""
function execute(step::Step{F}, input; verbose=nothing) where {F<:Function}
    start = time()
    outcome = run_safely() do; step.work(input); end
    if !outcome.ok
        return StepResult(step, false, time() - start, step.inputs, step.outputs, "Error: $(outcome.value)")
    end
    for out_path in step.outputs
        err = path_ready_error(out_path, false)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    StepResult(step, true, time() - start, step.inputs, step.outputs, outcome.value)
end
