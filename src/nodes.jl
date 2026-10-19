# Pipeline nodes. Constructors only build and validate data; execution lives in
# runtime.jl and execute.jl.

@nospecialize   # see runtime.jl

"""
    AbstractNode

Supertype of everything [`run`](@ref) can execute: [`Step`](@ref), the composite
nodes built by `>>`, `&`, `|` and `^`, [`ForEach`](@ref) and [`Workflow`](@ref).
"""
abstract type AbstractNode end

"""
Resources a step asks the scheduler for: CPU `threads` and peak memory `mem_mb`.
Zero means "not declared": one thread and no memory charge, unless an enclosing
[`with_resources`](@ref) provides a default.
"""
struct Resources
    threads::Int
    mem_mb::Int
end

function Resources(; threads::Integer=0, mem_mb::Integer=0)
    threads >= 0 || throw(ArgumentError("threads must be ≥ 0, got $threads"))
    mem_mb >= 0 || throw(ArgumentError("mem_mb must be ≥ 0, got $mem_mb"))
    Resources(Int(threads), Int(mem_mb))
end

"""Function work bound to its arguments, e.g. the declared paths of a step."""
struct Call
    f::Function
    args::Tuple
end
(c::Call)() = c.f(c.args...)

"""
    Step(work)
    Step(name::Symbol, work; threads=0, mem_mb=0)
    Step(name::Symbol, work, inputs, outputs; threads=0, mem_mb=0)

One unit of work. Usually built with [`@step`](@ref).

`work` is one of:

- a shell command: `sh"..."` or a `String` (run with `bash -e -o pipefail`);
- a `Cmd` (backticks);
- a Julia function.

With declared `inputs`/`outputs` (the path form), a shell template may use
`{input}`, `{output}`, `{input[i]}`, `{output[i]}`, `{threads}` and `{mem_mb}`, and a
function is called with the paths: `f(inputs..., outputs...)`. Without paths, a
function receives the upstream value when it takes one argument, and nothing
otherwise.

Declared paths make the step incremental: it is skipped when its outputs exist, are
newer than its inputs and were made by the same command. Output directories are
created before the step runs, and outputs left behind by a failed run are removed.
"""
struct Step <: AbstractNode
    name::Symbol
    work::Union{Shell,Base.AbstractCmd,Function,Call,Nothing}
    inputs::Vector{String}
    outputs::Vector{String}
    resources::Resources
    wildcards::Vector{Pair{String,String}}
    recipe::UInt64
end

Step(work) = Step(gensym(:step), work)

function Step(name::Symbol, work; threads::Integer=0, mem_mb::Integer=0)
    w, source = prepare_work(work)
    Step(name, w, String[], String[], Resources(; threads, mem_mb), Pair{String,String}[],
         fingerprint((source,)))
end

function Step(name::Symbol, work, inputs, outputs; threads::Integer=0, mem_mb::Integer=0)
    ins, outs = path_list(inputs), path_list(outputs)
    resources = Resources(; threads, mem_mb)
    ctx = TemplateContext([[i] for i in ins], outs, Dict{String,String}(),
                          max(resources.threads, 1), resources.mem_mb)
    w, source = prepare_work(work, ctx)
    Step(name, w, ins, outs, resources, Pair{String,String}[], fingerprint((source, ins...)))
end

"""A step with no work, used to report failures of composite nodes."""
marker_step(name::Symbol) =
    Step(name, nothing, String[], String[], Resources(), Pair{String,String}[], UInt64(0))

path_list(p::AbstractString) = String[p]
path_list(ps::Union{AbstractVector,Tuple}) = String[path_text(p) for p in ps]
path_list(x) = throw(ArgumentError("expected a path or a vector of paths, got $(summary(x))"))
path_text(p::AbstractString) = String(p)
path_text(x) = throw(ArgumentError("paths must be strings, got $(summary(x))"))

# Normalized work plus the text that identifies its recipe (for rerun-on-change).
prepare_work(w::AbstractString) = (Shell(String(w)), String(w))
prepare_work(w::Shell) = (w, w.script)
prepare_work(w::Base.AbstractCmd) = (w, string(w))
prepare_work(f::Function) = (f, "julia")
prepare_work(::AbstractNode) = throw(ArgumentError(
    "a pipeline node cannot be the work of a step; compose nodes directly with >> and &"))
prepare_work(w) = throw(ArgumentError(
    "step work must be a shell command (sh\"...\" or a String), a Cmd or a function; got " *
    "$(summary(w)). To run Julia code when the pipeline runs, write `() -> expr`."))

# Path form: shell templates are rendered, functions are bound to the paths.
prepare_work(w::AbstractString, ctx::TemplateContext) = (Shell(render(w, ctx)), String(w))
prepare_work(w::Shell, ctx::TemplateContext) = (Shell(render(w.script, ctx)), w.script)
prepare_work(f::Function, ctx::TemplateContext) =
    isempty(ctx.inputs) && isempty(ctx.outputs) ? (f, "julia") :
    (Call(f, (Iterators.flatten(ctx.inputs)..., ctx.outputs...)), "julia")
prepare_work(w, ::TemplateContext) = prepare_work(w)

const FNV_OFFSET = 0xcbf29ce484222325
const FNV_PRIME = 0x00000100000001b3

"""Stable 64-bit FNV-1a hash of strings; identical across sessions and Julia versions."""
function fingerprint(parts)
    h = FNV_OFFSET
    for part in parts
        for b in codeunits(part)
            h = (h ⊻ b) * FNV_PRIME
        end
        h = (h ⊻ 0xff) * FNV_PRIME
    end
    h
end

is_gensym(s::Symbol) = startswith(string(s), "##")

"""Display name of a step: its name (or command), plus wildcard values and an optional tag."""
function step_label(s::Step, tag::AbstractString="")
    base = is_gensym(s.name) ? work_label(s.work) : string(s.name)
    parts = [string(k, "=", v) for (k, v) in s.wildcards]
    isempty(tag) || push!(parts, tag)
    isempty(parts) ? base : string(base, "[", join(parts, ","), "]")
end

shorten(s::AbstractString, n::Int=60) = length(s) <= n ? String(s) : first(s, n - 1) * "…"

work_label(s::Shell) = shorten(s.script)
work_label(c::Cmd) = shorten(join(c.exec, " "))
work_label(c::Base.AbstractCmd) = shorten(string(c))
work_label(c::Call) = work_label(c.f)
work_label(::Nothing) = "-"
function work_label(f::Function)
    name = string(nameof(f))
    startswith(name, '#') ? "julia function" : name
end

#==============================================================================#
# Composite nodes
#==============================================================================#

"""
    Sequence(nodes...)
    a >> b

Run nodes one after another, stopping at the first failure. Each node receives the
value of the previous one. The value of a sequence is the value of its last node.
"""
struct Sequence <: AbstractNode
    nodes::Vector{AbstractNode}
    Sequence(nodes::AbstractVector) = new(AbstractNode[as_node(n) for n in nodes])
end
Sequence(nodes...) = Sequence(collect(Any, nodes))

"""
    Parallel(nodes...)
    a & b

Run nodes concurrently; every node receives the same input. The value is the vector
of the node values, in order. A failing branch does not stop the others.
"""
struct Parallel <: AbstractNode
    nodes::Vector{AbstractNode}
    Parallel(nodes::AbstractVector) = new(AbstractNode[as_node(n) for n in nodes])
end
Parallel(nodes...) = Parallel(collect(Any, nodes))

"""
    Retry(node, max_attempts=3; delay=0)
    node^n

Run `node` again after a failure, up to `max_attempts` times, sleeping `delay`
seconds between attempts.
"""
struct Retry <: AbstractNode
    node::AbstractNode
    max_attempts::Int
    delay::Float64
    function Retry(node, max_attempts::Integer=3; delay::Real=0.0)
        max_attempts >= 1 || throw(ArgumentError("Retry needs at least one attempt"))
        delay >= 0 || throw(ArgumentError("Retry delay must be ≥ 0"))
        new(as_node(node), max_attempts, delay)
    end
end

"""
    Fallback(primary, fallback)
    primary | fallback

Run `fallback` only when `primary` fails.
"""
struct Fallback <: AbstractNode
    primary::AbstractNode
    fallback::AbstractNode
    Fallback(primary, fallback) = new(as_node(primary), as_node(fallback))
end

"""
    Branch(condition, if_true, if_false)

Run one of two nodes. `condition` is called when the branch is reached, with the
upstream value if it accepts one argument, and must return a `Bool`. See also
[`@branch`](@ref).
"""
struct Branch <: AbstractNode
    condition::Function
    if_true::AbstractNode
    if_false::AbstractNode
    Branch(condition::Function, if_true, if_false) = new(condition, as_node(if_true), as_node(if_false))
end

"""
    Timeout(node, seconds)

Fail if `node` has not finished within `seconds`. Shell commands still running at
the deadline are terminated, including their child processes; steps that have not
started yet are not started.
"""
struct Timeout <: AbstractNode
    node::AbstractNode
    seconds::Float64
    function Timeout(node, seconds::Real)
        seconds > 0 || throw(ArgumentError("Timeout needs a positive number of seconds"))
        new(as_node(node), seconds)
    end
end

"""
    Force(node)

Run every step inside `node` even when its outputs are up to date.
"""
struct Force <: AbstractNode
    node::AbstractNode
    Force(node) = new(as_node(node))
end

"""A node with default resources for the steps inside it; see [`with_resources`](@ref)."""
struct Resourced <: AbstractNode
    node::AbstractNode
    resources::Resources
end
"""
    with_resources(node; threads=0, mem_mb=0)

Default resources for every step inside `node` that does not declare its own. The
scheduler only starts a step when its threads and memory fit the budgets given to
[`run`](@ref) (`jobs` and `mem_mb`).
"""
with_resources(node; threads::Integer=0, mem_mb::Integer=0) =
    Resourced(as_node(node), Resources(; threads, mem_mb))

"""
    ForEach(items) do item ... end
    ForEach("pattern/{wildcard}.ext") do wildcard... end

Fan out: build one branch per item, or per file matching the pattern, when the
pipeline runs. The block returns a step or node (a shell command or function is
lifted to a step). Branches run in parallel; the value is the vector of branch
values.

With a pattern, the block receives the wildcard values in order of appearance, and
step names in the log are tagged with them (`align[sample=A]`). A pattern that
matches no files is a failure; an empty collection is not.

```julia
ForEach(["A", "B"]) do s
    @step count = sh("wc -l \$(s).txt")
end

ForEach("fastq/{sample}_R1.fq.gz") do sample
    @step trim = sh("fastp -i fastq/\$(sample)_R1.fq.gz -o trim/\$(sample).fq.gz")
end
```
"""
struct ForEach{S} <: AbstractNode
    f::Function
    source::S
    ForEach(f::Function, p::Pattern) = new{Pattern}(f, p)
    ForEach(f::Function, items::Vector{T}) where {T} = new{Vector{T}}(f, items)
end

function ForEach(f::Function, pattern::AbstractString)
    p = Pattern(pattern)
    has_wildcards(p) || throw(ArgumentError("ForEach pattern must contain a {wildcard}: $pattern"))
    ForEach(f, p)
end
ForEach(f::Function, items) = ForEach(f, collect(items))

"""
    Pipeline(node; name="pipeline")

A node with a name, shown in the log of [`run`](@ref).
"""
struct Pipeline
    root::AbstractNode
    name::String
end
Pipeline(node; name::AbstractString="pipeline") = Pipeline(as_node(node), String(name))

"""Lift work into a node: nodes stay as they are, commands and functions become steps."""
as_node(n::AbstractNode) = n
as_node(x::Union{Shell,Base.AbstractCmd,Function,AbstractString}) = Step(x)
as_node(::Nothing) = throw(ArgumentError(
    "expected a step or node, got nothing (did a ForEach block forget to return its step?)"))
as_node(x) = throw(ArgumentError(
    "cannot use $(summary(x)) as a pipeline node; expected a node, a shell command, a Cmd or a function"))

"""Direct children of a node (lazy nodes have none until they are expanded)."""
children(::AbstractNode) = AbstractNode[]
children(s::Sequence) = s.nodes
children(p::Parallel) = p.nodes
children(r::Retry) = AbstractNode[r.node]
children(f::Fallback) = AbstractNode[f.primary, f.fallback]
children(b::Branch) = AbstractNode[b.if_true, b.if_false]
children(t::Timeout) = AbstractNode[t.node]
children(f::Force) = AbstractNode[f.node]
children(r::Resourced) = AbstractNode[r.node]

contains_node(n::AbstractNode, target::AbstractNode) =
    n === target || any(c -> contains_node(c, target), children(n))

"""
    steps(node) -> Vector{Step}

The distinct steps of a node, dependencies first. [`ForEach`](@ref) and
[`Workflow`](@ref) nodes are expanded only when they run, so they contribute no steps
here; use [`plan`](@ref) to resolve a workflow first.
"""
function steps(node)
    found = Step[]
    collect_steps!(found, as_node(node), IdDict{AbstractNode,Nothing}())
end

collect_steps!(found, s::Step, seen) = haskey(seen, s) ? found : (seen[s] = nothing; push!(found, s))
function collect_steps!(found, n::AbstractNode, seen)
    haskey(seen, n) && return found
    seen[n] = nothing
    foreach(c -> collect_steps!(found, c, seen), children(n))
    found
end

"""
    count_steps(node) -> Int

Number of distinct steps in a node; see [`steps`](@ref).
"""
count_steps(node) = length(steps(node))

@specialize
