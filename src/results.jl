# What running produces: per-step StepResults collected into a RunResult, and the
# Outcome that flows between nodes during execution.

@nospecialize   # see runtime.jl

"""
    StepFailure(kind, message; detail="")

Why a step failed. `kind` is one of:

| kind | meaning |
|:-----|:--------|
| `:process_failed` | the command exited with a non-zero status, left background processes running, or was killed from outside |
| `:memory_limit` | the command used more memory than its `mem_mb` and was terminated |
| `:out_of_memory` | the computer ran out of memory for this step (or the system's OOM killer stopped it) |
| `:timed_out` | the step's `timeout` or an enclosing [`Timeout`](@ref) expired |
| `:stopped` | the run was interrupted, or the resource guard exited unexpectedly |
| `:exception` | Julia code threw (message and the top of the stack trace) |
| `:template` | a placeholder cannot be filled, e.g. `{mem_mb}` without a declared `mem_mb` |
| `:missing_input` | a declared input does not exist |
| `:missing_output` | the step finished without creating a declared output |
| `:stdout_limit` | the command printed more than 32 MiB; redirect large output to a file |
| `:no_matches` | a [`ForEach`](@ref) pattern matched no files |
| `:unresolved` | a [`Workflow`](@ref) could not work out how to build a target |

`detail` holds extra context such as the end of stderr.
"""
struct StepFailure
    kind::Symbol
    message::String
    detail::String
end
StepFailure(kind::Symbol, message::AbstractString; detail::AbstractString="") =
    StepFailure(kind, String(message), String(detail))

Base.showerror(io::IO, e::StepFailure) =
    isempty(e.detail) ? print(io, e.message) : print(io, e.message, "\n", e.detail)

"""Result of a node during execution: success flag and the value passed downstream."""
struct Outcome
    ok::Bool
    value::Any
end
succeeded(value) = Outcome(true, value)
const FAILED = Outcome(false, nothing)

"""
Run `f()`, converting a thrown exception into a failed Outcome carrying a
StepFailure. This is the package's only exception boundary. An interrupt (Ctrl-C)
runs `interrupted()` and is rethrown so the whole run stops.
"""
function run_safely(f; interrupted=() -> nothing)
    try
        Outcome(true, f())
    catch e
        caught(e, catch_backtrace(), interrupted)
    end
end

caught(e::InterruptException, _, interrupted) = (interrupted(); throw(e))
# A Ctrl-C that lands in a spawned task reaches its parent wrapped by `fetch`.
caught(e::TaskFailedException, backtrace, interrupted) = caught(e.task.result, backtrace, interrupted)
caught(e::TemplateError, _, _) = Outcome(false, StepFailure(:template, e.message))
caught(e, backtrace, _) = Outcome(false, StepFailure(:exception, sprint(showerror, e);
                                                     detail=stack_text(backtrace)))

function stack_text(backtrace; frames::Int=8)
    trace = stacktrace(backtrace)
    lines = ["  [$i] $(frame.func) at $(frame.file):$(frame.line)"
             for (i, frame) in enumerate(Iterators.take(trace, frames))]
    join(lines, "\n")
end

"""
    StepResult

The record of one step in a run.

- `step`: the [`Step`](@ref).
- `label`: display name, including wildcard values (`align[sample=A]`).
- `status`: `:done`, `:skipped` (up to date), `:failed`, or `:planned` (from [`check`](@ref)).
- `success`: `status != :failed`.
- `duration`: seconds.
- `peak_mb`: peak memory of the step's command in MiB, `nothing` when not measured
  (function steps, skipped and planned steps, systems where memory is not measured).
- `inputs`, `outputs`: the declared paths.
- `result`: stdout for a shell step, the return value for a function, a
  [`StepFailure`](@ref) when failed, the reason when planned, `nothing` when skipped.
"""
struct StepResult
    step::Step
    label::String
    status::Symbol
    success::Bool
    duration::Float64
    peak_mb::Union{Nothing,Int}
    inputs::Vector{String}
    outputs::Vector{String}
    result::Any
end
StepResult(step::Step, label::String, status::Symbol, duration::Real, result, peak_mb::Union{Nothing,Int}=nothing) =
    StepResult(step, label, status, status !== :failed, Float64(duration), peak_mb, step.inputs, step.outputs, result)

"""
    RunResult

What [`run`](@ref) and [`check`](@ref) return: a vector of [`StepResult`](@ref)s in
completion order, plus

- `success`: whether the pipeline as a whole succeeded (a `Fallback` that recovered
  counts as success even though its primary failed); also `success(result)`;
- `value`: the value of the pipeline, e.g. the return value of a final function step;
- `duration`: wall time in seconds.
"""
struct RunResult <: AbstractVector{StepResult}
    name::String
    steps::Vector{StepResult}
    success::Bool
    value::Any
    duration::Float64
    planned::Bool
end

Base.size(r::RunResult) = size(r.steps)
Base.getindex(r::RunResult, i::Int) = r.steps[i]
Base.IndexStyle(::Type{RunResult}) = IndexLinear()
Base.success(r::RunResult) = r.success

count_status(r::RunResult, status::Symbol) = count(s -> s.status === status, r.steps)

@specialize
