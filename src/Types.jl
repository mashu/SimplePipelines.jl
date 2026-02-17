# Core node types, composition operators, StepResult, RunOutcome, Verbose/Silent, Pipeline.
# Included first (after StateFormat and shell macro).

"""
    AbstractNode

Abstract supertype of all pipeline nodes (Step, Sequence, Parallel, Retry, Fallback, Branch, Timeout, Force, Reduce, ForEach).
Constructors only build the struct; execution is via the functor: call `(node)(v, forced)` which dispatches to `run_node(node, v, forced)`.
"""
abstract type AbstractNode end

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

is_gensym(s::Symbol) = startswith(string(s), "##")
step_label(s::Step) = is_gensym(s.name) ? work_label(s.work) : string(s.name)
work_label(c::Cmd) = length(c.exec) ≤ 3 ? join(c.exec, " ") : join(c.exec[1:3], " ") * "…"
work_label(f::Function) = string(nameof(f))
work_label(::Nothing) = "(no work)"
work_label(x) = repr(x)

"""
    Sequence{T} <: AbstractNode

Executes nodes sequentially, stopping on first failure. Created automatically by the `>>` operator.
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

# Composition operators
>>(a::AbstractNode, b::AbstractNode) = Sequence((a, b))
>>(a::Sequence, b::AbstractNode) = Sequence((a.nodes..., b))
>>(a::AbstractNode, b::Sequence) = Sequence((a, b.nodes...))
>>(a::Sequence, b::Sequence) = Sequence((a.nodes..., b.nodes...))

>>(a::Cmd, b::Cmd) = Step(a) >> Step(b)
>>(a::Function, b::Function) = Step(a) >> Step(b)
>>(a::Cmd, b::Function) = Step(a) >> Step(b)
>>(a::Function, b::Cmd) = Step(a) >> Step(b)
>>(a::Cmd, b) = Step(a) >> b
>>(a, b::Cmd) = a >> Step(b)
>>(a::Function, b) = Step(a) >> b
>>(a, b::Function) = a >> Step(b)

(&)(a::AbstractNode, b::AbstractNode) = Parallel((a, b))
(&)(a::Parallel, b::AbstractNode) = Parallel((a.nodes..., b))
(&)(a::AbstractNode, b::Parallel) = Parallel((a, b.nodes...))
(&)(a::Parallel, b::Parallel) = Parallel((a.nodes..., b.nodes...))

(&)(a::Cmd, b::Cmd) = Step(a) & Step(b)
(&)(a::Function, b::Function) = Step(a) & Step(b)
(&)(a::Cmd, b::Function) = Step(a) & Step(b)
(&)(a::Function, b::Cmd) = Step(a) & Step(b)
(&)(a::Cmd, b) = Step(a) & b
(&)(a, b::Cmd) = a & Step(b)
(&)(a::Function, b) = Step(a) & b
(&)(a, b::Function) = a & Step(b)

(|)(a::AbstractNode, b::AbstractNode) = Fallback(a, b)
(|)(a::Fallback, b::AbstractNode) = Fallback(a.primary, Fallback(a.fallback, b))

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

"""Supertype of all step results; use for `Vector{AbstractStepResult}` (e.g. return of `run`)."""
abstract type AbstractStepResult end

"""
    StepResult(step, success, duration, inputs, output)

Result of running one step. Type is `StepResult{S, I, V}`. Type-stable: no `Any`.
"""
struct StepResult{S<:Step, I, V} <: AbstractStepResult
    step::S
    success::Bool
    duration::Float64
    inputs::I
    output::V
    StepResult{S, I, V}(step::S, success::Bool, duration::Float64, inputs::I, output::V) where {S<:Step, I, V} = new{S, I, V}(step, success, duration, inputs, output)
end
StepResult(step::S, success::Bool, duration::Float64, inputs::I, output::V) where {S<:Step, I, V} = StepResult{S, I, V}(step, success, duration, inputs, output)

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
