# Core node types, composition operators, StepResult, RunOutcome, Verbose/Silent, Pipeline.
# Included first (after StateFormat and shell macro).

"""
    AbstractNode

Abstract supertype of all pipeline nodes (Step, Sequence, Parallel, Retry, Fallback, Branch, Timeout, Force, Reduce, ForEach, Pipe, SameInputPipe, BroadcastPipe).
Constructors only build the struct; execution is via the functor: call `(node)(v, forced)` which dispatches to `run_node(node, v, forced)`.
"""
abstract type AbstractNode end

(node::AbstractNode)(v, forced::Bool=false) = run_node(node, v, forced)

"""
    Step{F} <: AbstractNode

A single unit of work in a pipeline. `F` is the work type: `Cmd` (backtick or `sh"..."`), `Function`, or `ShRun` (from `sh(f)`).

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

"""
    ShRun{F}

Internal: runs a shell command string at execution time. Use `sh(cmd_func)` in steps; the pipeline
prints the command when `verbose=true`.
"""
struct ShRun{F}
    f::F
end

is_gensym(s::Symbol) = startswith(string(s), "##")
step_label(s::Step) = is_gensym(s.name) ? work_label(s.work) : string(s.name)
work_label(c::Cmd) = length(c.exec) ≤ 3 ? join(c.exec, " ") : join(c.exec[1:3], " ") * "…"
work_label(f::Function) = string(nameof(f))
work_label(::Nothing) = "(no work)"
work_label(::ShRun) = "sh(...)"
work_label(x) = repr(x)

"""
    Sequence{T} <: AbstractNode
    a >> b

Executes nodes sequentially, stopping on first failure. **Data passing:** the next node receives one value: the previous step's *output* (declared output paths when applicable, else the step's result). When the previous node produced multiple results (e.g. Parallel/ForEach), the next node receives **only the last** branch's output. Distinct from `|>` (which passes a vector of all) and `.>>` (which runs the next step once per branch).
"""
struct Sequence{T<:Tuple} <: AbstractNode
    nodes::T
end
Sequence(nodes::Vararg{AbstractNode}) = Sequence(nodes)

"""
    Parallel{T} <: AbstractNode

Executes nodes concurrently using threads. Created automatically by the `&` operator.
"""
struct Parallel{T<:Tuple} <: AbstractNode
    nodes::T
end
Parallel(nodes::Vararg{AbstractNode}) = Parallel(nodes)

"""
    Retry{N} <: AbstractNode

Retries a node up to `max_attempts` times on failure, with optional delay. Created by the `^` operator or `Retry()` constructor.
"""
struct Retry{N<:AbstractNode} <: AbstractNode
    node::N
    max_attempts::Int
    delay::Float64
end
Retry(node::AbstractNode, max_attempts::Int=3; delay::Real=0.0) = Retry(node, max_attempts, Float64(delay))

"""
    Fallback{A,B} <: AbstractNode

Executes fallback node if primary fails. Created by the `|` operator.
"""
struct Fallback{A<:AbstractNode, B<:AbstractNode} <: AbstractNode
    primary::A
    fallback::B
end

"""
    Branch{C,T,F} <: AbstractNode

Conditional execution based on a predicate function.
"""
struct Branch{C<:Function, T<:AbstractNode, F<:AbstractNode} <: AbstractNode
    condition::C
    if_true::T
    if_false::F
end

"""
    Timeout{N} <: AbstractNode

Wraps a node with a time limit. Returns failure if time exceeded.
"""
struct Timeout{N<:AbstractNode} <: AbstractNode
    node::N
    seconds::Float64
end

"""
    Force{N} <: AbstractNode

Forces execution of a node, bypassing freshness checks. See also [`is_fresh`](@ref), [`clear_state!`](@ref).
"""
struct Force{N<:AbstractNode} <: AbstractNode
    node::N
end

"""
    Reduce{F,N} <: AbstractNode

Runs a parallel node and combines successful step outputs with a reducer function.
"""
struct Reduce{F<:Function, N<:AbstractNode} <: AbstractNode
    reducer::F
    node::N
    name::Symbol
end
Reduce(f::Function, node::AbstractNode; name::Symbol=:reduce) = Reduce(f, node, name)
Reduce(node::AbstractNode; name::Symbol=:reduce) = f -> Reduce(f, node; name=name)

"""Lazy node: run block over file matches (pattern string) or over a collection (vector). Dispatches on second argument."""
struct ForEach{F, P} <: AbstractNode
    f::F
    source::P  # String (file pattern) or Vector{T} (items)
end

# Operands to >>, &, | may be AbstractNode, Cmd, or Function. Conversion via dispatch only (no isa/Any).
# Explicit (Cmd, Cmd), (Function, Function), etc. avoid ambiguity with Base (e.g. & for process composition)
# and keep one-line bodies via node_operand.
node_operand(x::AbstractNode) = x
node_operand(x::Cmd) = Step(x)
node_operand(x::Function) = Step(x)

# Composition operators
>>(a::AbstractNode, b::AbstractNode) = Sequence((a, b))
>>(a::Sequence, b::AbstractNode) = Sequence((a.nodes..., b))
>>(a::AbstractNode, b::Sequence) = Sequence((a, b.nodes...))
>>(a::Sequence, b::Sequence) = Sequence((a.nodes..., b.nodes...))
>>(a::Cmd, b::Cmd) = node_operand(a) >> node_operand(b)
>>(a::Function, b::Function) = node_operand(a) >> node_operand(b)
>>(a::Cmd, b::Function) = node_operand(a) >> node_operand(b)
>>(a::Function, b::Cmd) = node_operand(a) >> node_operand(b)
>>(a::Cmd, b::AbstractNode) = node_operand(a) >> b
>>(a::AbstractNode, b::Cmd) = a >> node_operand(b)
>>(a::Function, b::AbstractNode) = node_operand(a) >> b
>>(a::AbstractNode, b::Function) = a >> node_operand(b)

(&)(a::AbstractNode, b::AbstractNode) = Parallel((a, b))
(&)(a::Parallel, b::AbstractNode) = Parallel((a.nodes..., b))
(&)(a::AbstractNode, b::Parallel) = Parallel((a, b.nodes...))
(&)(a::Parallel, b::Parallel) = Parallel((a.nodes..., b.nodes...))
(&)(a::Cmd, b::Cmd) = node_operand(a) & node_operand(b)
(&)(a::Function, b::Function) = node_operand(a) & node_operand(b)
(&)(a::Cmd, b::Function) = node_operand(a) & node_operand(b)
(&)(a::Function, b::Cmd) = node_operand(a) & node_operand(b)
(&)(a::Cmd, b::AbstractNode) = node_operand(a) & b
(&)(a::AbstractNode, b::Cmd) = a & node_operand(b)
(&)(a::Function, b::AbstractNode) = node_operand(a) & b
(&)(a::AbstractNode, b::Function) = a & node_operand(b)

(|)(a::AbstractNode, b::AbstractNode) = Fallback(a, b)
(|)(a::Fallback, b::AbstractNode) = Fallback(a.primary, Fallback(a.fallback, b))
(|)(a::Cmd, b::Cmd) = node_operand(a) | node_operand(b)
(|)(a::Function, b::Function) = node_operand(a) | node_operand(b)
(|)(a::Cmd, b::Function) = node_operand(a) | node_operand(b)
(|)(a::Function, b::Cmd) = node_operand(a) | node_operand(b)
(|)(a::Cmd, b::AbstractNode) = node_operand(a) | b
(|)(a::AbstractNode, b::Cmd) = a | node_operand(b)
(|)(a::Function, b::AbstractNode) = node_operand(a) | b
(|)(a::AbstractNode, b::Function) = a | node_operand(b)

(^)(a::AbstractNode, n::Int) = Retry(a, n)
(^)(a::Cmd, n::Int) = node_operand(a) ^ n
(^)(a::Function, n::Int) = node_operand(a) ^ n

"""
    Pipe{A, B} <: AbstractNode
    a |> b

Run `a`, then run `b` with `a`'s output(s) as `b`'s input. RHS must be a **function step**. Same notion of *output* as `>>` (declared output paths when applicable, else result). Single result → one value; multiple results (ForEach/Parallel) → **vector of all** branch outputs. Distinct from `>>` (which passes only the last) and `.>>` (which runs the next step once per branch).
"""
struct Pipe{A<:AbstractNode, B<:AbstractNode} <: AbstractNode
    first::A
    second::B
end
|>(a::AbstractNode, b::AbstractNode) = Pipe(a, b)

"""
    SameInputPipe{A, B} <: AbstractNode
    a >>> b

Run `a` then `b`; both receive the **same** context input. `b` does **not** receive `a`'s output. Distinct from `>>` and `|>` (where `b` receives `a`'s output). Use when the next step should run on the same input (e.g. branch id) as the previous.
"""
struct SameInputPipe{A<:AbstractNode, B<:AbstractNode} <: AbstractNode
    first::A
    second::B
end
>>>(a::AbstractNode, b::AbstractNode) = SameInputPipe(a, b)

"""
    BroadcastPipe{A, B} <: AbstractNode
    a .>> b   (broadcast of >>)

Run the next step **once per branch** of the left node, each with that branch's output. For `ForEach` or `Parallel`, branches run in parallel. For a single-output node, equivalent to `a >> b`. Distinct from `>>` (one run, last branch only) and `|>` (one run with vector of all outputs).
"""
struct BroadcastPipe{A<:AbstractNode, B<:AbstractNode} <: AbstractNode
    first::A
    second::B
end

"""Supertype of all step results; use for `Vector{AbstractStepResult}` (e.g. return of `run`)."""
abstract type AbstractStepResult end

"""
    StepResult(step, success, duration, inputs, outputs, result)

Result of running one step. Type is `StepResult{S, I, O, V}`. Type-stable: no `Any`.

# Fields (all real)
- `inputs` — Input file paths the step declared. Empty for steps that take no input files (e.g. download / start nodes).
- `outputs` — Output file paths the step declared (files the step produces).
- `result` — Execution result: stdout for shell steps, return value for function steps, or `nothing` when `keep_outputs != :all`.
"""
struct StepResult{S<:Step, I, O, V} <: AbstractStepResult
    step::S
    success::Bool
    duration::Float64
    inputs::I
    outputs::O
    result::V
    StepResult{S, I, O, V}(step::S, success::Bool, duration::Float64, inputs::I, outputs::O, result::V) where {S<:Step, I, O, V} = new{S, I, O, V}(step, success, duration, inputs, outputs, result)
end
StepResult(step::S, success::Bool, duration::Float64, inputs::I, outputs::O, result::V) where {S<:Step, I, O, V} = StepResult{S, I, O, V}(step, success, duration, inputs, outputs, result)

"""Type-stable outcome of running a thunk: success and value, or failure and error string. Sole exception boundary."""
struct RunOutcome{T}
    ok::Bool
    value::T
end

struct Verbose end
struct Silent end

"""
    Pipeline{N<:AbstractNode}

A named pipeline wrapping a root node for execution.
"""
struct Pipeline{N<:AbstractNode}
    root::N
    name::String
end

Pipeline(node::AbstractNode; name::String="pipeline") = Pipeline(node, name)
Pipeline(nodes::Vararg{AbstractNode}; name::String="pipeline") = Pipeline(Sequence(nodes), name)
