"""
    SimplePipelines

Minimal, type-stable DAG pipelines for Julia with Make-like incremental builds.
Execution is recursive: `run_node` dispatches on node type and recurses (Sequence in order, Parallel with `@spawn`, ForEach/Map expand then run).

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
- **Recursive execution**: Dispatch on node type; Sequence runs in order, Parallel/ForEach/Map run branches with optional parallelism.
- **Make-like freshness**: Steps skip if outputs are newer than inputs.
- **State persistence**: Tracks completed steps across runs.
- **Colored output**: Visual tree structure with status indicators.
- **Force execution**: `Force(step)` or `run(p, force=true)`.

See also: [`Step`](@ref), [`Pipeline`](@ref), [`run`](@ref), [`is_fresh`](@ref).
"""
module SimplePipelines

export Step, @step, Sequence, Parallel, Pipeline
export Retry, Fallback, Branch, Timeout, Force
export Map, Reduce, ForEach, fe
export count_steps, steps, print_dag, is_fresh, clear_state!
export @sh_str, sh

import Base: >>, &, |, ^

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
# Core Types
#==============================================================================#

"""
    AbstractNode

Abstract supertype of all pipeline nodes (Step, Sequence, Parallel, Retry, Fallback, Branch, Timeout, Force, Reduce, Map, ForEach).
Constructors only build the struct; execution is via the functor: call `(node)(v, forced)` which dispatches to `run_node(node, v, forced)`.
"""
abstract type AbstractNode end

# Functor: execution is dispatch on the node type, not if-checks
(node::AbstractNode)(v, forced::Bool=false) = run_node(node, v, forced)

"""
    Step{F} <: AbstractNode

A single unit of work in a pipeline. `F` is the work type (`Cmd` or `Function`).

# Fields
- `name::Symbol` — Step identifier (auto-generated if not provided)
- `work::F` — The command or function to execute
- `inputs::Vector{String}` — Input file dependencies
- `outputs::Vector{String}` — Output file paths

# Constructors
```julia
Step(work)                           # Auto-generated name
Step(name::Symbol, work)             # Named step
Step(name, work, inputs, outputs)    # Full specification
@step name = work                    # Macro form
@step name(inputs => outputs) = work # Macro with dependencies
```

# Examples
```julia
# Command step
Step(`sort data.txt`)
Step(:sort, `sort data.txt`)

# Function step  
Step(() -> println("hello"))
Step(:greet, () -> println("hello"))

# With file dependencies (enables Make-like freshness)
Step(:process, `sort in.txt > out.txt`, ["in.txt"], ["out.txt"])
```

See also: [`@step`](@ref), [`is_fresh`](@ref), [`Force`](@ref)
"""
struct Step{F} <: AbstractNode
    name::Symbol
    work::F
    inputs::Vector{String}
    outputs::Vector{String}
end

Step(name::Symbol, work) = Step(name, work, String[], String[])
Step(work) = Step(gensym(:step), work, String[], String[])
Step(name::Symbol, work, inputs, outputs) = Step(name, work, collect(String, inputs), collect(String, outputs))

# Display name: use name if explicit, otherwise show the work
is_gensym(s::Symbol) = startswith(string(s), "##")
step_label(s::Step) = is_gensym(s.name) ? work_label(s.work) : string(s.name)
work_label(c::Cmd) = length(c.exec) ≤ 3 ? join(c.exec, " ") : join(c.exec[1:3], " ") * "…"
work_label(f::Function) = string(nameof(f))
work_label(::Nothing) = "(no work)"
work_label(x) = repr(x)

"""
    Sequence{T} <: AbstractNode

Executes nodes sequentially, stopping on first failure.
Created automatically by the `>>` operator.

```julia
a >> b >> c  # equivalent to Sequence((a, b, c))
```
"""
struct Sequence{T<:Tuple} <: AbstractNode
    nodes::T
end
Sequence(nodes::Vararg{AbstractNode}) = Sequence(nodes)

"""
    Parallel{T} <: AbstractNode

Executes nodes concurrently using threads.
Created automatically by the `&` operator.

```julia
a & b & c  # equivalent to Parallel((a, b, c))
```
"""
struct Parallel{T<:Tuple} <: AbstractNode
    nodes::T
end
Parallel(nodes::Vararg{AbstractNode}) = Parallel(nodes)

"""
    Retry{N} <: AbstractNode

Retries a node up to `max_attempts` times on failure, with optional delay.
Created by the `^` operator or `Retry()` constructor.

```julia
step^3                      # Retry up to 3 times
Retry(step, 5; delay=2.0)   # 5 attempts with 2s delay between
```
"""
struct Retry{N<:AbstractNode} <: AbstractNode
    node::N
    max_attempts::Int
    delay::Float64
end
Retry(node::AbstractNode, max_attempts::Int=3; delay::Real=0.0) = Retry(node, max_attempts, Float64(delay))

"""
    Fallback{A,B} <: AbstractNode

Executes fallback node if primary fails.
Created by the `|` operator.

```julia
primary | backup  # Run backup if primary fails
```
"""
struct Fallback{A<:AbstractNode, B<:AbstractNode} <: AbstractNode
    primary::A
    fallback::B
end

"""
    Branch{C,T,F} <: AbstractNode

Conditional execution based on a predicate function.

```julia
Branch(() -> should_update(), update_step, skip_step)
```
"""
struct Branch{C<:Function, T<:AbstractNode, F<:AbstractNode} <: AbstractNode
    condition::C
    if_true::T
    if_false::F
end

"""
    Timeout{N} <: AbstractNode

Wraps a node with a time limit. Returns failure if time exceeded.

```julia
Timeout(slow_step, 30.0)  # 30 second timeout
```
"""
struct Timeout{N<:AbstractNode} <: AbstractNode
    node::N
    seconds::Float64
end

"""
    Force{N} <: AbstractNode

Forces execution of a node, bypassing freshness checks.
Use when you need to re-run a step even if outputs are up-to-date.

```julia
Force(step)              # Force this specific step
run(pipeline, force=true) # Force all steps in pipeline
```

See also: [`is_fresh`](@ref), [`clear_state!`](@ref)
"""
struct Force{N<:AbstractNode} <: AbstractNode
    node::N
end

"""
    Reduce{F,N} <: AbstractNode

Runs a parallel node and combines successful step outputs with a reducer function.

```julia
Reduce((a & b), join)           # Reduce(join, a & b)
Reduce(a & b)(join)             # curried: reducer later
Reduce(join, a & b; name=:merge)
```
"""
struct Reduce{F<:Function, N<:AbstractNode} <: AbstractNode
    reducer::F
    node::N
    name::Symbol
end
Reduce(f::Function, node::AbstractNode; name::Symbol=:reduce) = Reduce(f, node, name)
Reduce(node::AbstractNode; name::Symbol=:reduce) = f -> Reduce(f, node; name=name)

"""Lazy node: discovers files and runs the block once per match when the pipeline runs (not at construction)."""
struct ForEach{F} <: AbstractNode
    f::F
    pattern::String
end

"""Lazy node: applies `f` to each item when the pipeline runs (not at construction)."""
struct Map{F, T} <: AbstractNode
    f::F
    items::Vector{T}
end

#==============================================================================#
# Composition Operators
#==============================================================================#

# Sequence composition with flattening
>>(a::AbstractNode, b::AbstractNode) = Sequence((a, b))
>>(a::Sequence, b::AbstractNode) = Sequence((a.nodes..., b))
>>(a::AbstractNode, b::Sequence) = Sequence((a, b.nodes...))
>>(a::Sequence, b::Sequence) = Sequence((a.nodes..., b.nodes...))

# Auto-lift Cmd/Function to Step (explicit methods resolve dispatch ambiguity)
>>(a::Cmd, b::Cmd) = Step(a) >> Step(b)
>>(a::Function, b::Function) = Step(a) >> Step(b)
>>(a::Cmd, b::Function) = Step(a) >> Step(b)
>>(a::Function, b::Cmd) = Step(a) >> Step(b)
>>(a::Cmd, b) = Step(a) >> b
>>(a, b::Cmd) = a >> Step(b)
>>(a::Function, b) = Step(a) >> b
>>(a, b::Function) = a >> Step(b)

# Parallel composition with flattening
(&)(a::AbstractNode, b::AbstractNode) = Parallel((a, b))
(&)(a::Parallel, b::AbstractNode) = Parallel((a.nodes..., b))
(&)(a::AbstractNode, b::Parallel) = Parallel((a, b.nodes...))
(&)(a::Parallel, b::Parallel) = Parallel((a.nodes..., b.nodes...))

# Auto-lift Cmd/Function to Step
(&)(a::Cmd, b::Cmd) = Step(a) & Step(b)
(&)(a::Function, b::Function) = Step(a) & Step(b)
(&)(a::Cmd, b::Function) = Step(a) & Step(b)
(&)(a::Function, b::Cmd) = Step(a) & Step(b)
(&)(a::Cmd, b) = Step(a) & b
(&)(a, b::Cmd) = a & Step(b)
(&)(a::Function, b) = Step(a) & b
(&)(a, b::Function) = a & Step(b)

# Fallback composition
(|)(a::AbstractNode, b::AbstractNode) = Fallback(a, b)
(|)(a::Fallback, b::AbstractNode) = Fallback(a.primary, Fallback(a.fallback, b))

# Auto-lift Cmd/Function to Step
(|)(a::Cmd, b::Cmd) = Step(a) | Step(b)
(|)(a::Function, b::Function) = Step(a) | Step(b)
(|)(a::Cmd, b::Function) = Step(a) | Step(b)
(|)(a::Function, b::Cmd) = Step(a) | Step(b)
(|)(a::Cmd, b) = Step(a) | b
(|)(a, b::Cmd) = a | Step(b)
(|)(a::Function, b) = Step(a) | b
(|)(a, b::Function) = a | Step(b)

(^)(a::AbstractNode, n::Int) = Retry(a, n)
(^)(a::Cmd, n::Int) = Retry(Step(a), n)
(^)(a::Function, n::Int) = Retry(Step(a), n)

#==============================================================================#
# Step Macro
#==============================================================================#

"""
    @step name = work
    @step name(inputs => outputs) = work
    @step work

Create a named step with optional file dependencies. Steps are **lazy**: if the right-hand side
is a function call (e.g. `println(x)` or `run_cmd(path)`), it is wrapped in a thunk and runs only
when the pipeline is run via `run(pipeline)`. Building the pipeline or inspecting it (e.g. `print_dag`)
does not execute step work. Shell commands (backtick/`sh"..."`) are also only executed when the step runs.

# Examples
```julia
# Named step
@step download = sh"curl -o data.csv http://example.com"

# With file dependencies (enables Make-like freshness checks)
@step process(["input.csv"] => ["output.csv"]) = sh"sort input.csv > output.csv"

# Anonymous step (auto-named)
@step sh"echo hello"
```
"""
macro step(expr)
    step_expr(expr)
end

# Lazy step work: RHS that is a :call expr is wrapped in a thunk so it runs only when the step runs.
step_work_expr(rhs) = (rhs isa Expr && rhs.head === :call) ? :(() -> $(esc(rhs))) : esc(rhs)

step_expr(expr::Symbol) = :(Step($(esc(expr))))
step_expr(expr) = :(Step($(esc(expr))))

function step_expr(expr::Expr)
    expr.head === :(=) || return :(Step($(esc(expr))))
    lhs, rhs = expr.args[1], expr.args[2]
    step_lhs(lhs, rhs)
end

step_lhs(lhs::Symbol, rhs) = :(Step($(QuoteNode(lhs)), $(step_work_expr(rhs))))

function step_lhs(lhs::Expr, rhs)
    lhs.head === :call || return :(Step($(esc(Expr(:(=), lhs, rhs)))))
    name = QuoteNode(lhs.args[1])
    length(lhs.args) >= 2 || return :(Step($name, $(step_work_expr(rhs))))
    deps = lhs.args[2]
    step_deps(name, deps, rhs)
end

step_deps(name, deps, rhs) = :(Step($name, $(step_work_expr(rhs))))

function step_deps(name, deps::Expr, rhs)
    deps.head === :call && length(deps.args) >= 3 && deps.args[1] === :(=>) || return :(Step($name, $(step_work_expr(rhs))))
    inputs = deps.args[2]
    outputs = deps.args[3]
    :(Step($name, $(step_work_expr(rhs)), $(step_inputs_expr(inputs)), $(step_outputs_expr(outputs))))
end
step_inputs_expr(s::String) = :([$s])
step_inputs_expr(x) = x
step_outputs_expr(s::String) = :([$s])
step_outputs_expr(x) = x

#==============================================================================#
# Result Type
#==============================================================================#

struct StepResult{S<:Step}
    step::S
    success::Bool
    duration::Float64
    output::String
end

#==============================================================================#
# State Persistence (Make-like freshness)
#==============================================================================#
# State file path and in-memory refs live here; all file I/O is in StateFormat
# (memory-mapped format with random access).

const STATE_FILE = Ref(".pipeline_state")

const state_loaded = Ref{Set{UInt64}}(Set{UInt64}())
const pending_completions = Ref{Set{UInt64}}(Set{UInt64}())
const run_depth = Ref{Int}(0)

step_hash(step::Step) = hash((step.name, step.work, step.inputs, step.outputs))

"""Read persisted step hashes from the state file (delegates to StateFormat.state_read)."""
load_state()::Set{UInt64} = state_read(STATE_FILE[], STATE_LAYOUT)

current_state()::Set{UInt64} = run_depth[] >= 1 ? union(state_loaded[], pending_completions[]) : load_state()

function save_state!(completed::Set{UInt64})
    state_write(STATE_FILE[], completed, STATE_LAYOUT)
end

function mark_complete!(step::Step)
    h = step_hash(step)
    if run_depth[] >= 1
        push!(pending_completions[], h)
    else
        state_append(STATE_FILE[], h, STATE_LAYOUT) || save_state!(union(state_read(STATE_FILE[], STATE_LAYOUT), Set{UInt64}([h])))
    end
end

"""
    clear_state!()

Remove the pipeline state file (`.pipeline_state`), forcing all steps
to run on the next execution regardless of freshness. The file uses the
fixed binary layout defined in the `StateFormat` module.

See also: [`is_fresh`](@ref), [`Force`](@ref)
"""
clear_state!() = isfile(STATE_FILE[]) && rm(STATE_FILE[])

"""
    is_fresh(step::Step) -> Bool

Check if a step can be skipped based on Make-like freshness rules.

# Freshness Rules
1. **Has inputs and outputs**: Fresh if all outputs exist and are newer than all inputs
2. **Has only outputs**: Fresh if outputs exist and step was previously completed
3. **No file dependencies**: Fresh if step was previously completed (state-based tracking)

State is persisted in `.pipeline_state` as a fixed-layout, memory-mapped file (see `StateFormat` in `src/StateFormat.jl`: magic, count, then hash array). Completions during a run are batched and written once when `run()` finishes.

# Examples
```julia
step = @step process(["in.txt"] => ["out.txt"]) = sh"sort in.txt > out.txt"
is_fresh(step)  # true if out.txt exists and is newer than in.txt
```

See also: [`Force`](@ref), [`clear_state!`](@ref)
"""
function is_fresh(step::Step)
    has_in, has_out = !isempty(step.inputs), !isempty(step.outputs)
    
    # Check outputs exist
    if has_out
        all(isfile, step.outputs) || return false
    end
    
    # Make-style timestamp check
    if has_in && has_out
        oldest_out = minimum(mtime, step.outputs)
        for inp in step.inputs
            isfile(inp) || return false
            mtime(inp) >= oldest_out && return false
        end
        return true
    end
    
    # Fall back to state-based tracking
    step_hash(step) in current_state()
end

# Bounded concurrency: run(; jobs=n). 0 = unbounded. Set at run time, not on node constructors.
const MAX_PARALLEL = Ref{Int}(8)

# Base.run(::Cmd) and user f() throw on failure; no check-based API. Sole exception boundary (see .cursor/rules: no other try/catch).
function run_safely(f)::Tuple{Bool,String}
    result = Ref{String}("")
    ok = Ref{Bool}(false)
    try
        out = f()
        result[] = out === nothing ? "" : string(out)
        ok[] = true
    catch e
        result[] = sprint(showerror, e)
    end
    (ok[], result[])
end

function execute(step::Step{Cmd})
    start = time()
    for inp in step.inputs
        isfile(inp) || return StepResult(step, false, time() - start, "Missing input file: $inp")
    end
    buf = IOBuffer()
    ok, err = run_safely() do; Base.run(Base.pipeline(step.work, stdout=buf, stderr=buf)); end
    if !ok
        return StepResult(step, false, time() - start, "Error: $err\n$(String(take!(buf)))")
    end
    for out_path in step.outputs
        isfile(out_path) || return StepResult(step, false, time() - start, "Output not created: $out_path")
    end
    StepResult(step, true, time() - start, String(take!(buf)))
end

function execute(step::Step{Nothing})
    StepResult(step, false, 0.0, "Step has no work (ForEach block returned nothing). The block must return a Step or node, e.g. @step name = sh\"cmd\".")
end

function execute(step::Step{F}) where {F<:Function}
    start = time()
    for inp in step.inputs
        isfile(inp) || return StepResult(step, false, time() - start, "Missing input file: $inp")
    end
    ok, output = run_safely() do
        r = step.work()
        r === nothing ? "" : string(r)
    end
    if !ok
        return StepResult(step, false, time() - start, "Error: $output")
    end
    for out_path in step.outputs
        isfile(out_path) || return StepResult(step, false, time() - start, "Output not created: $out_path")
    end
    StepResult(step, true, time() - start, output)
end

#==============================================================================#
# Node Execution (multiple dispatch)
#==============================================================================#

# Verbosity via dispatch
struct Verbose end
struct Silent end

log_start(::Silent, ::Step) = nothing
function log_start(::Verbose, s::Step)
    printstyled("▶ Running: ", color=:cyan)
    println(step_label(s))
end

log_skip(::Silent, ::Step) = nothing
function log_skip(::Verbose, s::Step)
    printstyled("⊳ Up to date: ", color=:light_black)
    printstyled(step_label(s), "\n", color=:light_black)
end

log_result(::Silent, ::StepResult) = nothing
function log_result(::Verbose, r::StepResult)
    if r.success
        printstyled("  ✓ ", color=:green)
        println("Completed in $(round(r.duration, digits=2))s")
    else
        printstyled("  ✗ ", color=:red, bold=true)
        println("Completed in $(round(r.duration, digits=2))s")
        printstyled("  Error: ", color=:red)
        println(r.output)
    end
end

log_parallel(::Silent, ::Int) = nothing
function log_parallel(::Verbose, n::Int)
    printstyled("⊕ ", color=:magenta)
    println("Running $n branches in parallel...")
end

log_retry(::Silent, ::Int, ::Int) = nothing
function log_retry(::Verbose, n::Int, max::Int)
    printstyled("↻ ", color=:yellow)
    println("Attempt $n/$max")
end

log_fallback(::Silent) = nothing
function log_fallback(::Verbose)
    printstyled("↯ ", color=:yellow)
    println("Primary failed, trying fallback...")
end

log_branch(::Silent, ::Bool) = nothing
function log_branch(::Verbose, c::Bool)
    printstyled("? ", color=:blue)
    println("Condition: $(c ? "true → if_true" : "false → if_false")")
end

log_timeout(::Silent, ::Float64) = nothing
function log_timeout(::Verbose, s::Float64)
    printstyled("⏱ ", color=:cyan)
    println("Timeout: $(s)s")
end

log_force(::Silent) = nothing
function log_force(::Verbose)
    printstyled("⚡ ", color=:yellow, bold=true)
    println("Forcing execution...")
end

log_reduce(::Silent, ::Symbol) = nothing
function log_reduce(::Verbose, n::Symbol)
    printstyled("⊛ ", color=:magenta)
    println("Reducing: $n")
end

# Step wrapping another node (e.g. @step name = a >> b): delegate to inner node only when work is AbstractNode
function run_node(step::Step{N}, v, forced::Bool=false) where {N<:AbstractNode}
    log_start(v, step)
    run_node(step.work, v, forced)
end

# Explicit leaf execution for Step{Cmd} (must not dispatch to Step{N} where N<:AbstractNode)
function run_node(step::Step{Cmd}, v, forced::Bool=false)
    if !forced && is_fresh(step)
        log_skip(v, step)
        return [StepResult(step, true, 0.0, "up to date (not re-run)")]
    end
    log_start(v, step)
    result = execute(step)
    log_result(v, result)
    result.success && mark_complete!(step)
    [result]
end

function run_node(step::Step, v, forced::Bool=false)
    if !forced && is_fresh(step)
        log_skip(v, step)
        return [StepResult(step, true, 0.0, "up to date (not re-run)")]
    end
    log_start(v, step)
    result = execute(step)
    log_result(v, result)
    result.success && mark_complete!(step)
    [result]
end

function run_node(seq::Sequence, v, forced::Bool)
    results = StepResult[]
    for node in seq.nodes
        node_results = run_node(node, v, forced)
        append!(results, node_results)
        any(r -> !r.success, node_results) && break
    end
    results
end

function run_node(par::Parallel, v, forced::Bool)
    log_parallel(v, length(par.nodes))
    max_p = MAX_PARALLEL[]
    if max_p > 0
        # Run all branches in rounds of max_p; each round waits before starting the next (no skipping)
        results = StepResult[]
        for i in 1:max_p:length(par.nodes)
            chunk = par.nodes[i:min(i + max_p - 1, end)]
            append!(results, reduce(vcat, fetch.([@spawn run_node(node, v, forced) for node in chunk])))
        end
        results
    else
        reduce(vcat, fetch.([@spawn run_node(node, v, forced) for node in par.nodes]))
    end
end

function run_node(r::Retry, v, forced::Bool)
    local results::Vector{StepResult}
    for attempt in 1:r.max_attempts
        log_retry(v, attempt, r.max_attempts)
        results = run_node(r.node, v, forced)
        all(r -> r.success, results) && return results
        attempt < r.max_attempts && r.delay > 0 && sleep(r.delay)
    end
    results
end

function run_node(f::Fallback, v, forced::Bool)
    results = run_node(f.primary, v, forced)
    all(r -> r.success, results) && return results
    log_fallback(v)
    run_node(f.fallback, v, forced)
end

function run_node(b::Branch, v, forced::Bool)
    cond = b.condition()
    log_branch(v, cond)
    run_node(cond ? b.if_true : b.if_false, v, forced)
end

function run_node(t::Timeout, v, forced::Bool)
    log_timeout(v, t.seconds)
    ch = Channel{Vector{StepResult}}(1)
    @async put!(ch, run_node(t.node, v, forced))
    
    deadline = time() + t.seconds
    while !isready(ch) && time() < deadline
        sleep(0.01)
    end
    
    isready(ch) ? take!(ch) : [StepResult(Step(:timeout, `true`), false, t.seconds, "Timeout after $(t.seconds)s")]
end

function run_node(f::Force, v, ::Bool)
    log_force(v)
    run_node(f.node, v, true)  # Force always runs the inner node
end

function run_node(r::Reduce, v, forced::Bool)
    start = time()
    results = run_node(r.node, v, forced)
    outputs = [res.output for res in results if res.success]
    
    if length(outputs) < length(results)
        return vcat(results, [StepResult(Step(r.name, r.reducer), false, time() - start, "Reduce aborted: upstream failed")])
    end
    
    log_reduce(v, r.name)
    ok, reduced = run_safely() do
        result = r.reducer(outputs)
        result === nothing ? "" : string(result)
    end
    if !ok
        return vcat(results, [StepResult(Step(r.name, r.reducer), false, time() - start, "Reduce error: $reduced")])
    end
    vcat(results, [StepResult(Step(r.name, r.reducer), true, time() - start, reduced)])
end

# Cycle check: does node contain needle? (ForEach/Map expansion only)
contains_node(needle::ForEach, x::ForEach) = x === needle
contains_node(needle::ForEach, s::Step) = step_contains(needle, s)
contains_node(needle::ForEach, n::AbstractNode) = any(c -> contains_node(needle, c), node_children(n))
contains_node(::ForEach, ::Map) = false
contains_node(needle::Map, x::Map) = x === needle
contains_node(needle::Map, s::Step) = step_contains(needle, s)
contains_node(needle::Map, n::AbstractNode) = any(c -> contains_node(needle, c), node_children(n))
step_contains(needle::Union{ForEach,Map}, s::Step{N}) where {N<:AbstractNode} = s.work === needle || contains_node(needle, s.work)
step_contains(::Union{ForEach,Map}, ::Step) = false
node_children(n::Sequence) = collect(n.nodes)
node_children(n::Parallel) = collect(n.nodes)
node_children(n::Retry) = [n.node]
node_children(n::Fallback) = [n.primary, n.fallback]
node_children(n::Branch) = [n.if_true, n.if_false]
node_children(n::Timeout) = [n.node]
node_children(n::Force) = [n.node]
node_children(n::Reduce) = [n.node]
node_children(::Union{Step,ForEach,Map}) = AbstractNode[]

function run_node(fe::ForEach, v, forced::Bool)
    regex = for_each_regex(fe.pattern)
    matches = find_matches(fe.pattern, regex)
    isempty(matches) && error("ForEach: no files match '$(fe.pattern)'")
    nodes = AbstractNode[]
    for c in matches
        r = length(c) == 1 ? fe.f(c[1]) : fe.f(c...)
        r === nothing && error("ForEach block must return a Step or node, not nothing.")
        node = as_node(r)
        contains_node(fe, node) && error("Pipeline cycle: ForEach block returned a node containing this ForEach.")
        push!(nodes, node)
    end
    log_parallel(v, length(nodes))
    max_p = MAX_PARALLEL[]
    if max_p > 0
        # Run all branches in rounds of max_p; each round waits before the next (no skipping)
        results = StepResult[]
        for i in 1:max_p:length(nodes)
            chunk = nodes[i:min(i + max_p - 1, end)]
            append!(results, reduce(vcat, fetch.([@spawn run_node(n, v, forced) for n in chunk])))
        end
        results
    else
        reduce(vcat, fetch.([@spawn run_node(n, v, forced) for n in nodes]))
    end
end

function run_node(m::Map, v, forced::Bool)
    nodes = AbstractNode[]
    for item in m.items
        node = as_node(m.f(item))
        contains_node(m, node) && error("Pipeline cycle: Map block returned a node containing this Map.")
        push!(nodes, node)
    end
    log_parallel(v, length(nodes))
    max_p = MAX_PARALLEL[]
    if max_p > 0
        # Run all branches in rounds of max_p; each round waits before the next (no skipping)
        results = StepResult[]
        for i in 1:max_p:length(nodes)
            chunk = nodes[i:min(i + max_p - 1, end)]
            append!(results, reduce(vcat, fetch.([@spawn run_node(n, v, forced) for n in chunk])))
        end
        results
    else
        length(nodes) == 1 ? run_node(nodes[1], v, forced) : reduce(vcat, fetch.([@spawn run_node(n, v, forced) for n in nodes]))
    end
end

#==============================================================================#
# Pipeline
#==============================================================================#

"""
    Pipeline{N<:AbstractNode}

A named pipeline wrapping a root node for execution.

```julia
p = Pipeline(step1 >> step2, name="ETL")
run(p)
display(p)  # Shows tree structure
```
"""
struct Pipeline{N<:AbstractNode}
    root::N
    name::String
end

Pipeline(node::AbstractNode; name::String="pipeline") = Pipeline(node, name)
Pipeline(nodes::Vararg{AbstractNode}; name::String="pipeline") = Pipeline(Sequence(nodes), name)

"""
    run(p::Pipeline; verbose=true, dry_run=false, force=false, jobs=8) -> Vector{StepResult}
    run(node::AbstractNode; kwargs...) -> Vector{StepResult}

Execute a pipeline or node, returning results for each step.

# Keywords
- `verbose=true`: Show colored progress output
- `dry_run=false`: If true, show DAG structure without executing
- `force=false`: If true, run all steps regardless of freshness
- `jobs=8`: Max concurrent branches for Parallel/ForEach/Map. All branches run; when `jobs > 0`, they run in rounds of `jobs` (each round waits for the previous). `jobs=0` = unbounded (all at once).

# Output
With `verbose=true`, shows tree-structured output: `▶` running, `✓` success, `✗` failure, `⊳` up to date (not re-run).

# Examples
```julia
run(pipeline)                # Default: up to 8 branches at a time
run(pipeline, jobs=0)         # Unbounded (all branches at once)
run(pipeline, jobs=4)         # At most 4 concurrent
run(pipeline, verbose=false)
run(pipeline, dry_run=true)
run(pipeline, force=true)
```

See also: [`is_fresh`](@ref), [`Force`](@ref), [`print_dag`](@ref)
"""
function Base.run(p::Pipeline; verbose::Bool=true, dry_run::Bool=false, force::Bool=false, jobs::Int=8)
    prev = MAX_PARALLEL[]
    MAX_PARALLEL[] = jobs
    results = run_pipeline(p, verbose, dry_run, force)
    MAX_PARALLEL[] = prev
    results
end

function run_pipeline(p::Pipeline, verbose::Bool, dry_run::Bool, force::Bool)
    if verbose
        printstyled("═══ Pipeline: ", color=:blue, bold=true)
        printstyled(p.name, " ═══\n", color=:blue, bold=true)
    end

    if dry_run
        verbose && print_dag(p.root)
        return StepResult[]
    end

    run_depth[] += 1
    if run_depth[] == 1
        state_loaded[] = state_read(STATE_FILE[], STATE_LAYOUT)
        pending_completions[] = Set{UInt64}()
    end

    v = verbose ? Verbose() : Silent()
    start = time()
    results = run_node(p.root, v, force)

    if run_depth[] == 1
        save_state!(union(state_loaded[], pending_completions[]))
    end
    run_depth[] -= 1

    if verbose
        n = count(r -> r.success, results)
        total = length(results)
        success_color = n == total ? :green : :red
        printstyled("═══ Completed: ", color=:blue, bold=true)
        printstyled("$n/$total", color=success_color, bold=true)
        printstyled(" steps in $(round(time() - start, digits=2))s ═══\n", color=:blue, bold=true)
    end
    results
end

Base.run(node::AbstractNode; kwargs...) = run(Pipeline(node); kwargs...)

#==============================================================================#
# DAG Visualization (multiple dispatch per node type)
#==============================================================================#

"""
    print_dag(node [; color=true])
    print_dag(io, node [, indent])

Print a tree visualization of the pipeline DAG. With `color=true` (default when writing to a terminal), uses colors for node types and status. See also [`run`](@ref) and `display(pipeline)`.
"""
print_dag(node::AbstractNode; color::Bool=true) = print_dag(stdout, node, "", "", color)
print_dag(io::IO, node::AbstractNode, ::Int=0) = print_dag(io, node, "", "", false)

# pre = prefix for first line, cont = continuation prefix for subsequent lines
function print_dag(io::IO, s::Step, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "○ ", color=:cyan) : print(io, "○ ")
    println(io, step_label(s))
    if !isempty(s.inputs)
        print(io, cont, "    ")
        color ? printstyled(io, "← ", color=:green) : print(io, "← ")
        println(io, join(s.inputs, ", "))
    end
    if !isempty(s.outputs)
        print(io, cont, "    ")
        color ? printstyled(io, "→ ", color=:yellow) : print(io, "→ ")
        println(io, join(s.outputs, ", "))
    end
end

function print_dag(io::IO, s::Sequence, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "▸ Sequence\n", color=:blue) : println(io, "▸ Sequence")
    print_children(io, s.nodes, cont, color)
end

function print_dag(io::IO, p::Parallel, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⊕ Parallel\n", color=:magenta) : println(io, "⊕ Parallel")
    print_children(io, p.nodes, cont, color)
end

function print_dag(io::IO, r::Retry, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "↻ Retry", color=:yellow) : print(io, "↻ Retry")
    println(io, " ×$(r.max_attempts)", r.delay > 0 ? " ($(r.delay)s delay)" : "")
    print_dag(io, r.node, cont * "    ", cont * "    ", color)
end

function print_dag(io::IO, f::Fallback, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "↯ Fallback\n", color=:yellow) : println(io, "↯ Fallback")
    print_dag(io, f.primary,  cont * "  ├─", cont * "  │ ", color)
    print_dag(io, f.fallback, cont * "  └─", cont * "    ", color)
end

function print_dag(io::IO, b::Branch, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "? Branch\n", color=:blue) : println(io, "? Branch")
    if color
        print_dag(io, b.if_true,  cont * "  ├─", cont * "  │ ", color, :green, "✓ ")
        print_dag(io, b.if_false, cont * "  └─", cont * "    ", color, :red, "✗ ")
    else
        print_dag(io, b.if_true,  cont * "  ├─✓ ", cont * "  │   ", false)
        print_dag(io, b.if_false, cont * "  └─✗ ", cont * "      ", false)
    end
end

function print_dag(io::IO, t::Timeout, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⏱ Timeout", color=:cyan) : print(io, "⏱ Timeout")
    println(io, " $(t.seconds)s")
    print_dag(io, t.node, cont * "    ", cont * "    ", color)
end

function print_dag(io::IO, r::Reduce, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⊛ Reduce", color=:magenta) : print(io, "⊛ Reduce")
    println(io, " :$(r.name)")
    print_dag(io, r.node, cont * "    ", cont * "    ", color)
end

function print_dag(io::IO, f::Force, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⚡ Force\n", color=:yellow, bold=true) : println(io, "⚡ Force")
    print_dag(io, f.node, cont * "    ", cont * "    ", color)
end

function print_dag(io::IO, fe::ForEach, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⊕ ForEach", color=:magenta) : print(io, "⊕ ForEach")
    println(io, " \"", fe.pattern, "\"")
end

function print_dag(io::IO, m::Map, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⊕ Map", color=:magenta) : print(io, "⊕ Map")
    println(io, " ($(length(m.items)) items)")
end

# Branch helper with marker
function print_dag(io::IO, node::AbstractNode, pre::String, cont::String, color::Bool, marker_color::Symbol, marker::String)
    printstyled(io, marker, color=marker_color)
    print_dag(io, node, pre, cont * "  ", color)
end

function print_children(io::IO, nodes, cont::String, color::Bool)
    n = length(nodes)
    for (i, node) in enumerate(nodes)
        last = i == n
        pre  = cont * (last ? "  └─" : "  ├─")
        next = cont * (last ? "    " : "  │ ")
        print_dag(io, node, pre, next, color)
    end
end

#==============================================================================#
# Utilities
#==============================================================================#

"""
    steps(node) -> Vector{Step}

Return all leaf steps in the DAG (flattened).
"""
steps(s::Step) = [s]
steps(s::Sequence) = reduce(vcat, steps.(s.nodes))
steps(p::Parallel) = reduce(vcat, steps.(p.nodes))
steps(r::Retry) = steps(r.node)
steps(f::Fallback) = vcat(steps(f.primary), steps(f.fallback))
steps(b::Branch) = vcat(steps(b.if_true), steps(b.if_false))
steps(t::Timeout) = steps(t.node)
steps(r::Reduce) = steps(r.node)
steps(f::Force) = steps(f.node)
steps(::ForEach) = Step[]  # lazy: not discovered until run
steps(::Map) = Step[]     # lazy: nodes built only when run

"""
    count_steps(node) -> Int

Return the number of steps in the DAG (leaf count for execution).
"""
count_steps(::Step) = 1
count_steps(s::Sequence) = sum(count_steps, s.nodes)
count_steps(p::Parallel) = sum(count_steps, p.nodes)
count_steps(r::Retry) = count_steps(r.node)
count_steps(f::Fallback) = count_steps(f.primary) + count_steps(f.fallback)
count_steps(b::Branch) = max(count_steps(b.if_true), count_steps(b.if_false))
count_steps(t::Timeout) = count_steps(t.node)
count_steps(r::Reduce) = count_steps(r.node) + 1
count_steps(f::Force) = count_steps(f.node)
count_steps(::ForEach) = 0  # lazy: not discovered until run
count_steps(::Map) = 0      # lazy: nodes built only when run

#==============================================================================#
# Display
#==============================================================================#

# Custom show so StepResult doesn't print the ugly closure type (e.g. Step{var"#20#21"{String}})
function Base.show(io::IO, r::StepResult)
    print(io, "StepResult(")
    show(io, r.step)
    print(io, ", ", r.success, ", ", round(r.duration; digits=2), ", ")
    show(io, r.output)
    print(io, ")")
end

Base.show(io::IO, s::Step) = print(io, "Step(:", s.name, ")")
Base.show(io::IO, s::Sequence) = print(io, "Sequence(", join(s.nodes, " >> "), ")")
Base.show(io::IO, p::Parallel) = print(io, "Parallel(", join(p.nodes, " & "), ")")
Base.show(io::IO, r::Retry) = print(io, "Retry(", r.node, ", ", r.max_attempts, ")")
Base.show(io::IO, f::Fallback) = print(io, "(", f.primary, " | ", f.fallback, ")")
Base.show(io::IO, b::Branch) = print(io, "Branch(?, ", b.if_true, ", ", b.if_false, ")")
Base.show(io::IO, t::Timeout) = print(io, "Timeout(", t.node, ", ", t.seconds, "s)")
Base.show(io::IO, r::Reduce) = print(io, "Reduce(:", r.name, ", ", r.node, ")")
Base.show(io::IO, f::Force) = print(io, "Force(", f.node, ")")
Base.show(io::IO, fe::ForEach) = print(io, "ForEach(\"", fe.pattern, "\")")
Base.show(io::IO, m::Map) = print(io, "Map(", length(m.items), " items)")
Base.show(io::IO, p::Pipeline) = print(io, "Pipeline(\"", p.name, "\", ", count_steps(p.root), " steps)")

function Base.show(io::IO, ::MIME"text/plain", p::Pipeline)
    color = get(io, :color, false)::Bool
    if color
        printstyled(io, "Pipeline: ", color=:blue, bold=true)
        printstyled(io, p.name, color=:white, bold=true)
        printstyled(io, " ($(count_steps(p.root)) steps)\n", color=:light_black)
    else
        println(io, "Pipeline: ", p.name, " (", count_steps(p.root), " steps)")
    end
    print_dag(io, p.root, "", "", color)
end

function Base.show(io::IO, ::MIME"text/plain", node::AbstractNode)
    color = get(io, :color, false)::Bool
    print_dag(io, node, "", "", color)
end

#==============================================================================#
# Map & ForEach
#==============================================================================#

"""
    Map(f, items)
    Map(f)

Lazy parallel node: applies `f` to each item **when you call `run(pipeline)`**, not when the pipeline is built.
`Map(f)` returns a function `items -> Map(f, items)`.

# Examples
```julia
Map([1, 2, 3]) do n
    @step step_n = `echo n=\$n`
end
```
"""
function Map(f::Function, items)
    vec = collect(items)
    isempty(vec) && error("Map requires at least one item")
    Map(f, vec)
end
Map(f::F, items::Vector{T}) where {F<:Function, T} = (isempty(items) && error("Map requires at least one item"); Map{F, T}(f, items))
Map(f::Function) = items -> Map(f, items)

as_node(n::AbstractNode) = n
as_node(x) = Step(x)

function for_each_regex(pattern::String)::Regex
    wildcard_rx = r"\{(\w+)\}"
    parts = split(pattern, "/")
    first_wild = findfirst(p -> contains(p, "{"), parts)
    first_wild === nothing && error("ForEach pattern must contain {wildcard}: $pattern")
    pattern_suffix = join(parts[first_wild:end], "/")
    placeholder = "\x00WILD\x00"
    temp = replace(pattern_suffix, wildcard_rx => placeholder)
    for c in ".+^*?\$()[]|"
        temp = replace(temp, string(c) => "\\" * c)
    end
    Regex("^" * replace(temp, placeholder => "([^/]+)") * "\$")
end

"""
    ForEach(pattern) do wildcards...
        # return a Step or node
    end
    fe(pattern) do wildcards... end   # short alias

Discover files matching pattern with `{name}` placeholders; create one parallel branch per match.
The block is run once per match **when you call `run(pipeline)`**, not when the pipeline is built.
It must return a Step or other node (e.g. `@step name = sh\"cmd\"`).
Concurrency is set at run time: `run(pipeline; jobs=8)` (default 8; use `jobs=0` for unbounded).
"""
function ForEach(f::Function, pattern::String)
    wildcard_rx = r"\{(\w+)\}"
    contains(pattern, wildcard_rx) || error("ForEach pattern must contain {wildcard}: $pattern")
    ForEach{typeof(f)}(f, pattern)
end
ForEach(pattern::String) = f -> ForEach(f, pattern)

const fe = ForEach
@doc "Short alias for [`ForEach`](@ref). Use `fe(\"pattern\") do x ... end`." fe

function find_matches(pattern::String, regex::Regex)
    parts = split(pattern, "/")
    first_wild = findfirst(p -> contains(p, "{"), parts)
    first_wild === nothing && error("Pattern must contain {wildcard}")
    
    base = first_wild == 1 ? "." : joinpath(parts[1:first_wild-1]...)
    isdir(base) || return Vector{Vector{String}}()
    
    matches = Vector{Vector{String}}()
    scan_dir!(matches, base, base, parts, first_wild, regex)
    matches
end

function scan_dir!(matches, base, dir, parts, idx, regex)
    idx > length(parts) && return
    is_last = idx == length(parts)
    
    for entry in readdir(dir)
        path = joinpath(dir, entry)
        if is_last && isfile(path)
            rel = replace(relpath(path, base), "\\" => "/")
            m = match(regex, rel)
            m !== nothing && push!(matches, collect(String, m.captures))
        elseif !is_last && isdir(path)
            scan_dir!(matches, base, path, parts, idx + 1, regex)
        end
    end
end

end # module
