# Core node types, composition operators, StepResult, RunOutcome, Pipeline.
# Included first (after StateFormat and shell macro).

"""
    AbstractNode

Abstract supertype of all pipeline nodes (Step, Sequence, Parallel, Retry, Fallback,
Branch, Timeout, Force, Reduce, ForEach, Pipe, SameInputPipe, BroadcastPipe).
Constructors only build the struct; execution is via [`run`](@ref), which builds a
`RunContext` and dispatches to `run_node(node, ctx, forced, context_input)`.
"""
abstract type AbstractNode end

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

Internal callable wrapper around a thunk that returns a shell command string at
execution time. Use `sh(cmd_func)` in steps; calling `sr()` invokes `sr.f()` and
returns the command string. The pipeline prints the command when `verbose=true`.
"""
struct ShRun{F}
    f::F
end
(s::ShRun)() = s.f()

is_gensym(s::Symbol) = startswith(string(s), "##")
step_label(s::Step) = is_gensym(s.name) ? work_label(s.work) : string(s.name)
work_label(c::Cmd) = length(c.exec) ≤ 3 ? join(c.exec, " ") : join(c.exec[1:3], " ") * "…"
work_label(c::Base.AbstractCmd) = string(c)   # OrCmds/AndCmds/CmdRedirect render as shell pipes
function work_label(f::Function)
    n = nameof(f)
    s = string(n)
    # Anonymous closures from `function(...) ... end` get compiler names like `#24#25`.
    startswith(s, '#') && return "anonymous function"
    s
end
work_label(::Nothing) = "(no work)"
work_label(::ShRun) = "sh(...)"
work_label(x) = repr(x)

"""
    Sequence <: AbstractNode
    a >> b

Executes nodes sequentially, stopping on first failure. **Data passing:** the next node
receives one value: the previous step's *output* (declared output paths when applicable,
else the step's result). When the previous node produced multiple results (e.g.
Parallel/ForEach), the next node receives **only the last** branch's output. Distinct
from `|>` (which passes a vector of all) and `.>>` (which runs the next step once per
branch).

Children are stored in a `Vector{AbstractNode}` so `Sequence` scales to thousands of
children without compile-time tuple-type blow-up.
"""
struct Sequence <: AbstractNode
    nodes::Vector{AbstractNode}
end
Sequence(nodes::Vararg{AbstractNode}) = Sequence(collect(AbstractNode, nodes))

"""
    Parallel <: AbstractNode

Executes nodes concurrently using threads. Created automatically by the `&` operator.
Children are stored in a `Vector{AbstractNode}` so wide fan-outs (hundreds of samples)
do not blow up compile time.
"""
struct Parallel <: AbstractNode
    nodes::Vector{AbstractNode}
end
Parallel(nodes::Vararg{AbstractNode}) = Parallel(collect(AbstractNode, nodes))

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

Conditional execution: at run time, `condition()` must return a `Bool`; the
corresponding branch runs. For a clearer call site than `Branch(() -> cond, a, b)`,
see [`@branch`](@ref).
"""
struct Branch{C<:Function, T<:AbstractNode, F<:AbstractNode} <: AbstractNode
    condition::C
    if_true::T
    if_false::F
end
function Branch(cond::Function, t, f)
    tn = node_operand(t)
    fn = node_operand(f)
    Branch(cond, tn, fn)
end

"""
    Timeout{N} <: AbstractNode

Wraps a node with a time limit. If the deadline passes before the inner node
finishes, the synthetic timeout step fails with [`StepFailure`](@ref) kind
`:timed_out`. If the inner node throws (including from [`Branch`](@ref) before
children run), the result uses kind `:inner_exception`. External processes
started under `sh` / `Cmd` are not reliably terminated on interrupt.
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
    reduce_step::Step{F}
end
function Reduce(f::Function, node::AbstractNode; name::Symbol=:reduce)
    Reduce(f, node, name, Step(name, f))
end
Reduce(node::AbstractNode; name::Symbol=:reduce) = f -> Reduce(f, node; name=name)

"""Lazy node: run block over file matches (pattern string) or over a collection (vector). Dispatches on second argument."""
struct ForEach{F, P} <: AbstractNode
    f::F
    source::P  # String (file pattern) or Vector{T} (items)
end

"""
    NoWork() <: AbstractNode

Sentinel node that performs no work and produces no step results. Returned by
`resolve` when every target is already on disk.
"""
struct NoWork <: AbstractNode end

# Anything we know how to lift into a node — used as the fallback domain for the
# composition operators below. Restricting to a Union keeps Base's own >>/&/|/^
# methods (e.g. Int, Bool) untouched. AbstractCmd covers Cmd plus the OrCmds /
# AndCmds / CmdRedirect produced by `Base.pipeline` (and by `sh_pipe`).
const Liftable = Union{AbstractNode, Base.AbstractCmd, Function}

# Lifters: identity for nodes, wrap raw work into a Step.
node_operand(x::AbstractNode) = x
node_operand(x::Base.AbstractCmd) = Step(x)
node_operand(x::Function) = Step(x)

# Composition operators. The four AbstractNode methods carry the actual semantics;
# a single Liftable fallback covers every Cmd/Function permutation by lifting and
# re-dispatching to the AbstractNode case. (AbstractNode is itself in Liftable, so
# the more-specific AbstractNode methods always win for already-lifted operands.)
>>(a::AbstractNode, b::AbstractNode) = Sequence(AbstractNode[a, b])
>>(a::Sequence, b::AbstractNode) = Sequence(push!(copy(a.nodes), b))
>>(a::AbstractNode, b::Sequence) = Sequence(pushfirst!(copy(b.nodes), a))
>>(a::Sequence, b::Sequence) = Sequence(vcat(a.nodes, b.nodes))
>>(a::Liftable, b::Liftable) = node_operand(a) >> node_operand(b)

(&)(a::AbstractNode, b::AbstractNode) = Parallel(AbstractNode[a, b])
(&)(a::Parallel, b::AbstractNode) = Parallel(push!(copy(a.nodes), b))
(&)(a::AbstractNode, b::Parallel) = Parallel(pushfirst!(copy(b.nodes), a))
(&)(a::Parallel, b::Parallel) = Parallel(vcat(a.nodes, b.nodes))
# Base defines `&(::AbstractCmd, ::AbstractCmd) = AndCmds(...)`; override for Cmd
# specifically so `cmd1 & cmd2` builds a Parallel in our DSL (more-specific method
# wins). For OrCmds/AndCmds operands, Base's behaviour is preserved — use
# `Step(...) & Step(...)` if you want Parallel of pipelines.
(&)(a::Cmd, b::Cmd) = node_operand(a) & node_operand(b)
(&)(a::Liftable, b::Liftable) = node_operand(a) & node_operand(b)

(|)(a::AbstractNode, b::AbstractNode) = Fallback(a, b)
(|)(a::Fallback, b::AbstractNode) = Fallback(a.primary, Fallback(a.fallback, b))
# Same situation as `&`: Base defines `|(::AbstractCmd, ::AbstractCmd) = OrCmds`,
# so we narrow to Cmd to keep `cmd1 | cmd2` as Fallback in our DSL.
(|)(a::Cmd, b::Cmd) = node_operand(a) | node_operand(b)
(|)(a::Liftable, b::Liftable) = node_operand(a) | node_operand(b)

(^)(a::AbstractNode, n::Int) = Retry(a, n)
(^)(a::Liftable, n::Int) = node_operand(a) ^ n

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
    StepFailure(kind, message; detail="")

Structured failure payload for unsuccessful steps and internal execution errors.

- `kind` is a symbol categorising the error (`:missing_input`, `:missing_output`,
  `:process_failed`, `:exception`, `:timed_out`, `:inner_exception`, `:no_matches`, …).
- `message` is a short human-readable summary.
- `detail` holds optional extra context (e.g. stderr tail). It may be empty.

This replaces stringly-typed `"Error: ..."` results; formatting is deferred to
`showerror` / display code.
"""
struct StepFailure
    kind::Symbol
    message::String
    detail::String
end
StepFailure(kind::Symbol, message::AbstractString; detail::AbstractString="") =
    StepFailure(kind, String(message), String(detail))

Base.showerror(io::IO, e::StepFailure) = isempty(e.detail) ?
    print(io, e.message) :
    print(io, e.message, "\n", e.detail)

Base.string(e::StepFailure) = sprint(showerror, e)
function Base.show(io::IO, e::StepFailure)
    print(io, "StepFailure(", repr(e.kind), ", ", repr(e.message), ")")
    isempty(e.detail) && return
    print(io, "\n", e.detail)
end

"""
    StepResult(step, success, duration, inputs, outputs, result)

Result of running one step. Type is `StepResult{S, I, O, V}`. Type-stable: no `Any`.

# Fields (all real)
- `inputs` — Input file paths the step declared. Empty for steps that take no input files (e.g. download / start nodes).
- `outputs` — Output file paths the step declared (files the step produces).
- `result` — Execution result: stdout for shell steps, return value for function steps. Wrap large values in [`FilePath`](@ref) so the in-memory result stays small.
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

"""Type-stable outcome of running a thunk: success + value, or failure + `StepFailure` when a thrown exception was converted by [`run_safely`](@ref)."""
struct RunOutcome{T}
    ok::Bool
    value::T
end

"""
    Pipeline{N<:AbstractNode}

A named pipeline wrapping a root node for execution.
"""
struct Pipeline{N<:AbstractNode}
    root::N
    name::String
end

Pipeline(node::AbstractNode; name::String="pipeline") = Pipeline(node, name)
Pipeline(nodes::Vararg{AbstractNode}; name::String="pipeline") = Pipeline(Sequence(nodes...), name)
