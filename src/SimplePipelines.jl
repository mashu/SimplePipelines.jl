raw"""
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
- **Data passing**: All of `>>`, `|>`, and `.>>` use the same notion of *output*: when a step has declared output paths and its result is log text (e.g. shell stdout/stderr), that value is the vector of output paths; otherwise it is the step's result. The operators differ in *which* value(s) the next step receives and *how many* times it runs:

| Operator | Left has one output | Left has multiple outputs (ForEach/Parallel) |
|:---------|:--------------------|:---------------------------------------------|
| ``>>``   | next gets that one value | next gets **last** branch's output only |
| ``\|>``  | next gets that one value | next gets **vector of all** branch outputs (RHS must be function step) |
| ``.>>``  | next gets that one value | next runs **once per branch**, each with that branch's output |
| ``>>>``  | (same-input pipe) second step gets **same input as first**, not first's output | — |

Use `a >>> b` when the second step should run on the same input (e.g. branch id) as the first. RHS of `|>` must be a function step.
- **Make-like freshness**: Steps skip if outputs are newer than inputs.
- **State persistence**: Tracks completed steps across runs.
- **Colored output**: Visual tree structure with status indicators.
- **Force execution**: `Force(step)` or `run(p, force=true)`.

See also: [`Step`](@ref), [`Pipeline`](@ref), [`run`](@ref), [`is_fresh`](@ref).
"""
module SimplePipelines

export Step, @step, @branch, Sequence, Parallel, Pipeline, AbstractNode
export StepResult, AbstractStepResult
export StepFailure
export Retry, Fallback, Branch, Timeout, Force
export Reduce, ForEach, fe
export SameInputPipe, >>>, BroadcastPipe
export count_steps, steps, print_dag, is_fresh, clear_state!
export @sh_str, sh, sh_pipe, ShRun
export @shell_raw_str, shell_raw
export Resources, Resourced, with_resources
export default_jobs, default_memory_budget_mb, default_spill_threshold_bytes
export FilePath, SpilledValue, SpilledStdout, materialize, materialize_table
export Rule, @rule, @targets, @workflow, resolve, NoWork, expand, Workflow, plan
export check, explain, RuleCheck, RuleInstantiationCheck, PlanExplanation

import Base: >>, &, |, ^, |>, >>>

using Base.Threads: @spawn, fetch, Condition

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

"""Shell command with string (build-time). For run-time command use `sh(cmd_func)`. See also [`@sh_str`](@ref)."""
sh(s::String) = Cmd(["sh", "-c", s])

"""
    sh_pipe(cmds::Base.AbstractCmd...) -> Base.AbstractCmd

Compose two or more shell commands into one OS-level pipeline. The resulting
`AbstractCmd` runs all stages as a single `Base.run`, with stdout flowing
through OS pipes between stages — no Julia-side buffering between commands.
Only the final stage's stdout is captured by the runtime.

This is an alias for `Base.pipeline`; the package recognises it inside `@step`
so the call is *not* thunked.

# Example
```julia
@step align(["foo.bam"] => ["filtered.txt"]) =
    sh_pipe(sh"samtools view foo.bam", sh"awk -F'\\t' '\$5>30'", sh"sort > filtered.txt")
```

Compare with the materialising `>>` form, where each step captures its full
stdout into Julia memory before the next step runs.

See also: [`@step`](@ref), [`@sh_str`](@ref).
"""
sh_pipe(cmds::Base.AbstractCmd...) = Base.pipeline(cmds...)

"""
    shell_raw"command"
    shell_raw\"\"\"...\"\"\"

String literal for shell commands where the dollar sign is not interpreted by Julia.
Use for scripts that use shell variables. Triple-quoted form keeps multiline scripts readable;
shell variables (e.g. `\$VAR`, `` `\${var}` ``) stay intact.

# Example (multiline with loop and Julia interpolation)

```julia
donors = ["A", "B"]
cmd = shell_raw\"\"\"
for d in \"\"\" * join(donors, " ") * shell_raw\"\"\"
  do
    echo \"Processing \$d\"
  done
\"\"\"
# Use in a step: sh(() -> cmd)
```
"""
macro shell_raw_str(s)
    # Build string from quoted chars so $ in content is not re-escaped by the compiler
    Expr(:string, (Expr(:quote, c) for c in collect(s))...)
end

#==============================================================================#
# Logical includes (order matters: Types → Macro → State → Logging → Execute → ForEach → RunNodes → Run → Display)
#==============================================================================#

include("Types.jl")
sh(f::Function) = ShRun(f)  # run-time shell command; must be after Types (ShRun)
include("Macro.jl")
include("FilePath.jl")
include("Resources.jl")
include("State.jl")
include("Logging.jl")
include("Execute.jl")
include("ForEach.jl")
include("RunNodes.jl")
include("Rules.jl")
include("Run.jl")
include("Display.jl")

#==============================================================================#
# Precompile workload for common run_node / resolve paths.
#==============================================================================#
using PrecompileTools: @setup_workload, @compile_workload

@setup_workload begin
    precompile_state = tempname()
    prerun(node; kwargs...) = run(Pipeline(node); verbose=false, force=true, state_path=precompile_state, kwargs...)
    @compile_workload begin
        # Single Cmd step
        prerun(@step nop = `true`)
        # Sequence + Parallel + Function step
        f = () -> 1
        prerun((@step a = `true`) >> (@step b = f) & (@step c = `true`))
        # ForEach over a small collection
        prerun(ForEach([1, 2]) do x; Step(Symbol("e", x), `true`); end)
        # Resource budget
        prerun(with_resources(`true`; mem_mb=1); memory_budget_mb=8)
        # OS-level shell pipeline (Step{<:AbstractCmd} path)
        prerun(@step piped = sh_pipe(sh"echo a", sh"cat"))
        # Rule resolution helpers (avoid hitting the filesystem)
        SimplePipelines.match_pattern("data/{x}.fq", "data/A.fq")
        SimplePipelines.substitute("out/{x}.bam", Dict("x" => "A"))
        SimplePipelines.fill_special("cp {input} {output}", ["a"], ["b"])
        # expand: lock in NamedTuple kwargs path
        expand("out/{s}.bam"; s=["A", "B"])
        # Rule onboarding helpers: single-rule check, target check, workflow explain.
        pc_rule = @rule align("raw/{s}.fq" => "out/{s}.bam") = "cmd {input} > {output}"
        check(pc_rule)
        check(pc_rule, "out/A.bam")
        pc_wf = @workflow "pc" begin
            @rule source([] => "raw/{s}.fq") = "touch {output}"
            @rule align("raw/{s}.fq" => "out/{s}.bam") = "cmd {input} > {output}"
            @targets "out/{s}.bam" s=["A", "B"]
        end
        explain(pc_wf; target="out/A.bam")
        plan(pc_wf)
        # Cold paths: NoWork, Branch, FilePath materialize
        prerun(NoWork())
        prerun(@branch true `true` `false`)
        fp = tempname()
        write(fp, "pc")
        materialize(FilePath(fp))
        rm(fp; force=true)
    end
    isfile(precompile_state) && rm(precompile_state)
end

end # module
