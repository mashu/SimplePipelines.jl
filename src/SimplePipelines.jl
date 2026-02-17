"""
    SimplePipelines

Minimal, type-stable DAG pipelines for Julia with Make-like incremental builds.
Execution is recursive: `run_node` dispatches on node type and recurses (Sequence in order, Parallel with `@spawn`, ForEach (pattern or collection) expand then run).

# Quick Start
```julia
using SimplePipelines

# Chain steps with >>
pipeline = sh"echo hello" >> sh"echo world"

# Run in parallel with &
pipeline = sh"task_a" & sh"task_b"

# Fallback on failure with |
pipeline = sh"primary" | sh"backup"

# Named steps with file dependencies
download = @step download([] => ["data.csv"]) = sh"curl -o data.csv http://example.com"
process = @step process(["data.csv"] => ["out.csv"]) = sh"sort data.csv > out.csv"

run(download >> process)
```

# Features
- **Recursive execution**: Dispatch on node type; Sequence runs in order, Parallel/ForEach run branches with optional parallelism.
- **Data passing**: When the left has one output, `>>`, `|>`, and `.>>` all pass that value to the next (function) step. When the left has **multiple** outputs (e.g. ForEach, Parallel), they differ:

  | Left side     | `a >> step`        | `a |> step`           | `a .>> step`              |
  | ------------- | ------------------ | --------------------- | ------------------------- |
  | Single output | step(one value)     | step(one value)       | step(one value)           |
  | Multi output  | step(**last** only) | step(**vector** of all) | step **per branch** (one call each) |

  Use `a >>> b` so both run on the **same** input (e.g. branch id). RHS of `|>` must be a function step.
- **Make-like freshness**: Steps skip if outputs are newer than inputs.
- **State persistence**: Tracks completed steps across runs.
- **Colored output**: Visual tree structure with status indicators.
- **Force execution**: `Force(step)` or `run(p, force=true)`.

See also: [`Step`](@ref), [`Pipeline`](@ref), [`run`](@ref), [`is_fresh`](@ref).
"""
module SimplePipelines

export Step, @step, Sequence, Parallel, Pipeline
export StepResult, AbstractStepResult
export Retry, Fallback, Branch, Timeout, Force
export Reduce, ForEach, fe
export SameInputPipe, >>>, BroadcastPipe
export count_steps, steps, print_dag, is_fresh, clear_state!
export @sh_str, sh

import Base: >>, &, |, ^, |>, >>>

using Base.Threads: @spawn, fetch

include("StateFormat.jl")
using .StateFormat: StateFileLayout, STATE_LAYOUT,
    layout_file_size, layout_read_header, layout_write_header, layout_validate_count,
    state_init, state_read, state_write, state_append

#==============================================================================#
# Shell string macro
#==============================================================================#

@doc raw"""
    sh"command"
    sh(command::String)

Shell commands: use `sh"..."` for literals, `sh("...")` when you need interpolation.

# Examples
```julia
sh"sort data.txt > sorted.txt"
sh("process \$(sample)_R1.fq")
```
"""
macro sh_str(s)
    Cmd(["sh", "-c", s])
end

"""Shell command with string interpolation. See also [`@sh_str`](@ref)."""
sh(s::String) = Cmd(["sh", "-c", s])

#==============================================================================#
# Logical includes (order matters: Types → Macro → State → Execute → Logging → ForEach → RunNodes → Run → Display)
#==============================================================================#

include("Types.jl")
include("Macro.jl")
include("State.jl")
include("Execute.jl")
include("Logging.jl")
include("ForEach.jl")
include("RunNodes.jl")
include("Run.jl")
include("Display.jl")

end # module
