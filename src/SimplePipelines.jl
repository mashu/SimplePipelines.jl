module SimplePipelines

export Step, @step, Sequence, Parallel, Pipeline
export Retry, Fallback, Branch, Timeout, Force
export Map, Reduce, ForEach
export count_steps, steps, print_dag, is_fresh, clear_state!
export @sh_str, sh

import Base: >>, &, |, ^

using Base.Threads: @spawn, fetch

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

sh(s::String) = Cmd(["sh", "-c", s])

#==============================================================================#
# Core Types
#==============================================================================#

abstract type AbstractNode end

"""
    Step{F}

A single unit of work. `F` is the work type (Cmd, Function, etc.)
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

struct Sequence{T<:Tuple} <: AbstractNode
    nodes::T
end
Sequence(nodes::Vararg{AbstractNode}) = Sequence(nodes)

struct Parallel{T<:Tuple} <: AbstractNode
    nodes::T
end
Parallel(nodes::Vararg{AbstractNode}) = Parallel(nodes)

struct Retry{N<:AbstractNode} <: AbstractNode
    node::N
    max_attempts::Int
    delay::Float64
end
Retry(node::AbstractNode, max_attempts::Int=3; delay::Real=0.0) = Retry(node, max_attempts, Float64(delay))

struct Fallback{A<:AbstractNode, B<:AbstractNode} <: AbstractNode
    primary::A
    fallback::B
end

struct Branch{C<:Function, T<:AbstractNode, F<:AbstractNode} <: AbstractNode
    condition::C
    if_true::T
    if_false::F
end

struct Timeout{N<:AbstractNode} <: AbstractNode
    node::N
    seconds::Float64
end

struct Force{N<:AbstractNode} <: AbstractNode
    node::N
end

struct Reduce{F<:Function, N<:AbstractNode} <: AbstractNode
    reducer::F
    node::N
    name::Symbol
end
Reduce(f::Function, node::AbstractNode; name::Symbol=:reduce) = Reduce(f, node, name)
Reduce(node::AbstractNode; name::Symbol=:reduce) = f -> Reduce(f, node; name=name)

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

macro step(expr)
    if expr isa Expr && expr.head === :(=)
        lhs, rhs = expr.args
        if lhs isa Symbol
            return :(Step($(QuoteNode(lhs)), $(esc(rhs))))
        elseif lhs isa Expr && lhs.head === :call
            name = QuoteNode(lhs.args[1])
            deps = lhs.args[2]
            if deps isa Expr && deps.head === :call && deps.args[1] === :(=>)
                inputs = deps.args[2]
                outputs = deps.args[3]
                inputs_expr = inputs isa String ? :([$(inputs)]) : inputs
                outputs_expr = outputs isa String ? :([$(outputs)]) : outputs
                return :(Step($name, $(esc(rhs)), $inputs_expr, $outputs_expr))
            end
        end
    end
    :(Step($(esc(expr))))
end

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

const STATE_FILE = Ref(".pipeline_state")

step_hash(step::Step) = hash((step.name, step.work, step.inputs, step.outputs))

function load_state()
    isfile(STATE_FILE[]) || return Set{UInt64}()
    try
        Set{UInt64}(parse(UInt64, line) for line in eachline(STATE_FILE[]) if !isempty(strip(line)))
    catch
        Set{UInt64}()
    end
end

function save_state!(completed::Set{UInt64})
    open(STATE_FILE[], "w") do io
        for h in completed
            println(io, h)
        end
    end
end

function mark_complete!(step::Step)
    completed = load_state()
    push!(completed, step_hash(step))
    save_state!(completed)
end

clear_state!() = isfile(STATE_FILE[]) && rm(STATE_FILE[])

"""
    is_fresh(step::Step) -> Bool

Check if step can be skipped. Fresh if:
- Has inputs+outputs: all outputs exist and are newer than all inputs
- Has only outputs: outputs exist and step completed before  
- No files: was completed before (state-based)
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
    step_hash(step) in load_state()
end

#==============================================================================#
# Execution
#==============================================================================#

function execute(step::Step{Cmd})
    start = time()
    for inp in step.inputs
        isfile(inp) || return StepResult(step, false, time() - start, "Missing input file: $inp")
    end
    
    buf = IOBuffer()
    try
        run(pipeline(step.work, stdout=buf, stderr=buf))
    catch e
        return StepResult(step, false, time() - start, "Error: $(sprint(showerror, e))\n$(String(take!(buf)))")
    end
    
    for out in step.outputs
        isfile(out) || return StepResult(step, false, time() - start, "Output not created: $out")
    end
    StepResult(step, true, time() - start, String(take!(buf)))
end

function execute(step::Step{F}) where {F<:Function}
    start = time()
    for inp in step.inputs
        isfile(inp) || return StepResult(step, false, time() - start, "Missing input file: $inp")
    end
    
    local output::String
    try
        result = step.work()
        output = result === nothing ? "" : string(result)
    catch e
        return StepResult(step, false, time() - start, "Error: $(sprint(showerror, e))")
    end
    
    for out in step.outputs
        isfile(out) || return StepResult(step, false, time() - start, "Output not created: $out")
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
log_start(::Verbose, s::Step) = println("▶ Running: $(s.name)")

log_skip(::Silent, ::Step) = nothing
log_skip(::Verbose, s::Step) = println("⊳ Skipping (fresh): $(s.name)")

log_result(::Silent, ::StepResult) = nothing
function log_result(::Verbose, r::StepResult)
    println("  $(r.success ? "✓" : "✗") Completed in $(round(r.duration, digits=2))s")
    r.success || println("  Error: $(r.output)")
end

log_parallel(::Silent, ::Int) = nothing
log_parallel(::Verbose, n::Int) = println("⊕ Running $n branches in parallel...")

log_retry(::Silent, ::Int, ::Int) = nothing
log_retry(::Verbose, n::Int, max::Int) = println("↻ Attempt $n/$max")

log_fallback(::Silent) = nothing
log_fallback(::Verbose) = println("↯ Primary failed, trying fallback...")

log_branch(::Silent, ::Bool) = nothing
log_branch(::Verbose, c::Bool) = println("? Condition: $(c ? "true → if_true" : "false → if_false")")

log_timeout(::Silent, ::Float64) = nothing
log_timeout(::Verbose, s::Float64) = println("⏱ Timeout: $(s)s")

log_force(::Silent) = nothing
log_force(::Verbose) = println("⚡ Forcing execution...")

log_reduce(::Silent, ::Symbol) = nothing
log_reduce(::Verbose, n::Symbol) = println("⊕ Reducing: $n")

# Step wrapping another node (e.g. @step name = a >> b)
function run_node(step::Step{N}, v, forced::Bool=false) where {N<:AbstractNode}
    log_start(v, step)
    run_node(step.work, v, forced)
end

function run_node(step::Step, v, forced::Bool=false)
    if !forced && is_fresh(step)
        log_skip(v, step)
        return [StepResult(step, true, 0.0, "skipped (fresh)")]
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
    tasks = [(@spawn run_node(node, v, forced)) for node in par.nodes]
    reduce(vcat, fetch.(tasks))
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

function run_node(f::Force, v, forced::Bool)
    log_force(v)
    run_node(f.node, v, true)
end

function run_node(r::Reduce, v, forced::Bool)
    start = time()
    results = run_node(r.node, v, forced)
    outputs = [res.output for res in results if res.success]
    
    if length(outputs) < length(results)
        return vcat(results, [StepResult(Step(r.name, r.reducer), false, time() - start, "Reduce aborted: upstream failed")])
    end
    
    log_reduce(v, r.name)
    local reduced::String
    try
        result = r.reducer(outputs)
        reduced = result === nothing ? "" : string(result)
    catch e
        return vcat(results, [StepResult(Step(r.name, r.reducer), false, time() - start, "Reduce error: $(sprint(showerror, e))")])
    end
    
    vcat(results, [StepResult(Step(r.name, r.reducer), true, time() - start, reduced)])
end

#==============================================================================#
# Pipeline
#==============================================================================#

struct Pipeline{N<:AbstractNode}
    root::N
    name::String
end

Pipeline(node::AbstractNode; name::String="pipeline") = Pipeline(node, name)
Pipeline(nodes::Vararg{AbstractNode}; name::String="pipeline") = Pipeline(Sequence(nodes), name)

"""
    run(p::Pipeline; verbose=true, dry_run=false, force=false)

Execute pipeline. Steps are skipped if outputs are fresh (Make-like).
Use `force=true` or `Force(step)` to override.
"""
function Base.run(p::Pipeline; verbose::Bool=true, dry_run::Bool=false, force::Bool=false)
    verbose && println("═══ Pipeline: $(p.name) ═══")
    
    if dry_run
        verbose && print_dag(p.root)
        return StepResult[]
    end
    
    v = verbose ? Verbose() : Silent()
    start = time()
    results = run_node(p.root, v, force)
    
    if verbose
        n = count(r -> r.success, results)
        println("═══ Completed: $n/$(length(results)) steps in $(round(time() - start, digits=2))s ═══")
    end
    results
end

Base.run(node::AbstractNode; kwargs...) = run(Pipeline(node); kwargs...)

#==============================================================================#
# DAG Visualization (multiple dispatch per node type)
#==============================================================================#

print_dag(node::AbstractNode; indent::Int=0) = print_dag(stdout, node, indent)

function print_dag(io::IO, s::Step, indent::Int)
    pre = "  "^indent
    println(io, pre, s.name)
    isempty(s.inputs) || println(io, pre, "  ← ", join(s.inputs, ", "))
    isempty(s.outputs) || println(io, pre, "  → ", join(s.outputs, ", "))
end

function print_dag(io::IO, s::Sequence, indent::Int)
    println(io, "  "^indent, "Sequence:")
    foreach(n -> print_dag(io, n, indent + 1), s.nodes)
end

function print_dag(io::IO, p::Parallel, indent::Int)
    println(io, "  "^indent, "Parallel:")
    foreach(n -> print_dag(io, n, indent + 1), p.nodes)
end

function print_dag(io::IO, r::Retry, indent::Int)
    println(io, "  "^indent, "Retry(max=$(r.max_attempts), delay=$(r.delay)s):")
    print_dag(io, r.node, indent + 1)
end

function print_dag(io::IO, f::Fallback, indent::Int)
    pre = "  "^indent
    println(io, pre, "Fallback:")
    println(io, pre, "  primary:")
    print_dag(io, f.primary, indent + 2)
    println(io, pre, "  fallback:")
    print_dag(io, f.fallback, indent + 2)
end

function print_dag(io::IO, b::Branch, indent::Int)
    pre = "  "^indent
    println(io, pre, "Branch:")
    println(io, pre, "  if_true:")
    print_dag(io, b.if_true, indent + 2)
    println(io, pre, "  if_false:")
    print_dag(io, b.if_false, indent + 2)
end

function print_dag(io::IO, t::Timeout, indent::Int)
    println(io, "  "^indent, "Timeout($(t.seconds)s):")
    print_dag(io, t.node, indent + 1)
end

function print_dag(io::IO, r::Reduce, indent::Int)
    println(io, "  "^indent, "Reduce($(r.name)):")
    print_dag(io, r.node, indent + 1)
end

function print_dag(io::IO, f::Force, indent::Int)
    println(io, "  "^indent, "Force:")
    print_dag(io, f.node, indent + 1)
end

#==============================================================================#
# Utilities
#==============================================================================#

steps(s::Step) = [s]
steps(s::Sequence) = reduce(vcat, steps.(s.nodes))
steps(p::Parallel) = reduce(vcat, steps.(p.nodes))
steps(r::Retry) = steps(r.node)
steps(f::Fallback) = vcat(steps(f.primary), steps(f.fallback))
steps(b::Branch) = vcat(steps(b.if_true), steps(b.if_false))
steps(t::Timeout) = steps(t.node)
steps(r::Reduce) = steps(r.node)
steps(f::Force) = steps(f.node)

count_steps(::Step) = 1
count_steps(s::Sequence) = sum(count_steps, s.nodes)
count_steps(p::Parallel) = sum(count_steps, p.nodes)
count_steps(r::Retry) = count_steps(r.node)
count_steps(f::Fallback) = count_steps(f.primary) + count_steps(f.fallback)
count_steps(b::Branch) = max(count_steps(b.if_true), count_steps(b.if_false))
count_steps(t::Timeout) = count_steps(t.node)
count_steps(r::Reduce) = count_steps(r.node) + 1
count_steps(f::Force) = count_steps(f.node)

#==============================================================================#
# Display
#==============================================================================#

Base.show(io::IO, s::Step) = print(io, "Step(:", s.name, ")")
Base.show(io::IO, s::Sequence) = print(io, "Sequence(", join(s.nodes, " >> "), ")")
Base.show(io::IO, p::Parallel) = print(io, "Parallel(", join(p.nodes, " & "), ")")
Base.show(io::IO, r::Retry) = print(io, "Retry(", r.node, ", ", r.max_attempts, ")")
Base.show(io::IO, f::Fallback) = print(io, "(", f.primary, " | ", f.fallback, ")")
Base.show(io::IO, b::Branch) = print(io, "Branch(?, ", b.if_true, ", ", b.if_false, ")")
Base.show(io::IO, t::Timeout) = print(io, "Timeout(", t.node, ", ", t.seconds, "s)")
Base.show(io::IO, r::Reduce) = print(io, "Reduce(:", r.name, ", ", r.node, ")")
Base.show(io::IO, f::Force) = print(io, "Force(", f.node, ")")
Base.show(io::IO, p::Pipeline) = print(io, "Pipeline(\"", p.name, "\", ", count_steps(p.root), " steps)")

Base.show(io::IO, ::MIME"text/plain", p::Pipeline) = (println(io, "Pipeline: ", p.name, " (", count_steps(p.root), " steps)"); print_dag(io, p.root, 0))
Base.show(io::IO, ::MIME"text/plain", node::AbstractNode) = print_dag(io, node, 0)

#==============================================================================#
# Map & ForEach
#==============================================================================#

function Map(f::Function, items)
    nodes = [f(item) for item in items]
    isempty(nodes) && error("Map requires at least one item")
    length(nodes) == 1 && return first(nodes)
    reduce(&, nodes)
end
Map(f::Function) = items -> Map(f, items)

"""
    ForEach(pattern) do wildcards...
        # build pipeline
    end

Discover files matching pattern with `{name}` placeholders, create parallel branches.
"""
function ForEach(f::Function, pattern::String)
    wildcard_rx = r"\{(\w+)\}"
    contains(pattern, wildcard_rx) || error("ForEach pattern must contain {wildcard}: $pattern")
    
    # Build file-matching regex
    placeholder = "\x00WILD\x00"
    temp = replace(pattern, wildcard_rx => placeholder)
    for c in ".+^*?\$()[]|"
        temp = replace(temp, string(c) => "\\" * c)
    end
    regex = Regex("^" * replace(temp, placeholder => "([^/]+)") * "\$")
    
    # Find matching files
    matches = find_matches(pattern, regex)
    isempty(matches) && error("ForEach: no files match '$pattern'")
    
    nodes = AbstractNode[]
    for captured in matches
        result = length(captured) == 1 ? f(captured[1]) : f(captured...)
        push!(nodes, result isa AbstractNode ? result : Step(result))
    end
    
    length(nodes) == 1 ? first(nodes) : reduce(&, nodes)
end
ForEach(pattern::String) = f -> ForEach(f, pattern)

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
