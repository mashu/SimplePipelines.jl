# Step execution: run_safely, path/input/output readiness, execute(step). Requires Types.jl, State.jl.

function run_safely(f)::RunOutcome
    try
        RunOutcome(true, f())
    catch e
        RunOutcome(false, sprint(showerror, e))
    end
end

path_like(s::String) = occursin(r"[/\\]", s) || occursin('.', s)
input_ready_error(inp::String) = path_like(inp) && !isfile(inp) ? "Missing input: $inp" : nothing
input_ready_error(inp) = nothing
output_ready_error(out::String) = path_like(out) && !isfile(out) ? "Missing output: $out" : nothing
output_ready_error(out) = nothing

function execute(step::Step{Cmd}; verbose=nothing)
    start = time()
    for inp in step.inputs
        err = input_ready_error(inp)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    verbose isa Verbose && log_cmd(verbose, step.work)
    buf = IOBuffer()
    outcome = run_safely() do; Base.run(Base.pipeline(step.work, stdout=buf, stderr=buf)); end
    if !outcome.ok
        return StepResult(step, false, time() - start, step.inputs, step.outputs, "Error: $(outcome.value)\n$(String(take!(buf)))")
    end
    for out_path in step.outputs
        err = output_ready_error(out_path)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    StepResult(step, true, time() - start, step.inputs, step.outputs, String(take!(buf)))
end

function execute(step::Step{ShRun}; verbose=nothing)
    start = time()
    for inp in step.inputs
        err = input_ready_error(inp)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    cmd = step.work.f()
    verbose isa Verbose && log_cmd(verbose, cmd)
    buf = IOBuffer()
    outcome = run_safely() do
        Base.run(Base.pipeline(Cmd(["sh", "-c", cmd]), stdout=buf, stderr=buf))
    end
    if !outcome.ok
        return StepResult(step, false, time() - start, step.inputs, step.outputs, "Error: $(outcome.value)\n$(String(take!(buf)))")
    end
    for out_path in step.outputs
        err = output_ready_error(out_path)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    StepResult(step, true, time() - start, step.inputs, step.outputs, String(take!(buf)))
end

function execute(step::Step{Nothing})
    StepResult(step, false, 0.0, step.inputs, step.outputs, "Step has no work (ForEach block returned nothing). The block must return a Step or node, e.g. @step name = sh\"cmd\".")
end

function execute(step::Step{F}; verbose=nothing) where {F<:Function}
    start = time()
    for inp in step.inputs
        err = input_ready_error(inp)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    outcome = run_safely() do
        isempty(step.inputs) ? step.work() : step.work(step.inputs...)
    end
    if !outcome.ok
        return StepResult(step, false, time() - start, step.inputs, step.outputs, "Error: $(outcome.value)")
    end
    for out_path in step.outputs
        err = output_ready_error(out_path)
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
        err = output_ready_error(out_path)
        err !== nothing && return StepResult(step, false, time() - start, step.inputs, step.outputs, err)
    end
    StepResult(step, true, time() - start, step.inputs, step.outputs, outcome.value)
end
