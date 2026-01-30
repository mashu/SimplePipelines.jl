module SimplePipelines

export Step, @step, Sequence, Parallel, Pipeline
export run_pipeline, count_steps, steps, print_dag

import Base: >>, &

using Base.Threads: @spawn, fetch

#==============================================================================#
# Core Types - Fully parametric for zero-overhead dispatch
#==============================================================================#

"""
    AbstractNode

Base type for all pipeline nodes. Subtypes are:
- [`Step`](@ref): A single unit of work
- [`Sequence`](@ref): Nodes that run in order
- [`Parallel`](@ref): Nodes that run concurrently
"""
abstract type AbstractNode end

"""
    Step{F}

A single unit of work in the pipeline.

# Type Parameters
- `F`: The type of work (e.g., `Cmd` for shell commands, or a `Function` subtype)

# Fields
- `name::Symbol`: Identifier for the step
- `work::F`: The work to execute
- `inputs::Vector{String}`: Input file dependencies (optional)
- `outputs::Vector{String}`: Output files produced (optional)

# Examples
```julia
# Shell command
Step(:align, `bwa mem ref.fa reads.fq`)

# Julia function
Step(:qc, () -> run_quality_control())

# With file dependencies
Step(:variant_call, `bcftools call -m aligned.bam`, ["aligned.bam"], ["variants.vcf"])
```
"""
struct Step{F} <: AbstractNode
    name::Symbol
    work::F
    inputs::Vector{String}
    outputs::Vector{String}
end

# Constructors - no runtime type checks, just dispatch
@inline Step(name::Symbol, work::F) where {F} = Step{F}(name, work, String[], String[])
@inline Step(work::F) where {F} = Step{F}(gensym(:step), work, String[], String[])
@inline Step(name::Symbol, work::F, inputs, outputs) where {F} = 
    Step{F}(name, work, collect(String, inputs), collect(String, outputs))

"""
    Sequence{T<:Tuple}

A sequence of nodes that execute in order. The type parameter `T` captures
the exact tuple type for full type stability.

# Examples
```julia
# Created via >> operator
align >> sort >> index
```
"""
struct Sequence{T<:Tuple} <: AbstractNode
    nodes::T
end

@inline Sequence(nodes::Vararg{AbstractNode}) = Sequence(nodes)

"""
    Parallel{T<:Tuple}

Nodes that execute concurrently. The type parameter `T` captures
the exact tuple type for full type stability.

# Examples
```julia
# Created via & operator
(sample_a & sample_b & sample_c) >> merge
```
"""
struct Parallel{T<:Tuple} <: AbstractNode
    nodes::T
end

@inline Parallel(nodes::Vararg{AbstractNode}) = Parallel(nodes)

#==============================================================================#
# Composition Operators - Pure multiple dispatch, no type checks
#==============================================================================#

"""
    a >> b

Create a [`Sequence`](@ref) where `a` completes before `b` starts.

Supports chaining: `a >> b >> c` creates a single flattened sequence.

# Examples
```julia
# Basic sequence
fastqc >> trim >> align

# Chain shell commands directly
`fastqc raw.fq` >> `trimmomatic ...` >> `bwa mem ...`
```
"""
@inline >>(a::AbstractNode, b::AbstractNode) = Sequence((a, b))
@inline >>(a::Sequence, b::AbstractNode) = Sequence((a.nodes..., b))
@inline >>(a::AbstractNode, b::Sequence) = Sequence((a, b.nodes...))
@inline >>(a::Sequence, b::Sequence) = Sequence((a.nodes..., b.nodes...))

# Lift Cmd to Step automatically
@inline >>(a::Cmd, b::Cmd) = Sequence((Step(a), Step(b)))
@inline >>(a::Cmd, b::AbstractNode) = Sequence((Step(a), b))
@inline >>(a::AbstractNode, b::Cmd) = Sequence((a, Step(b)))
@inline >>(a::Cmd, b::Sequence) = Sequence((Step(a), b.nodes...))
@inline >>(a::Sequence, b::Cmd) = Sequence((a.nodes..., Step(b)))

# Lift Function to Step automatically
@inline >>(a::Function, b::Function) = Sequence((Step(a), Step(b)))
@inline >>(a::Function, b::AbstractNode) = Sequence((Step(a), b))
@inline >>(a::AbstractNode, b::Function) = Sequence((a, Step(b)))
@inline >>(a::Function, b::Sequence) = Sequence((Step(a), b.nodes...))
@inline >>(a::Sequence, b::Function) = Sequence((a.nodes..., Step(b)))
@inline >>(a::Cmd, b::Function) = Sequence((Step(a), Step(b)))
@inline >>(a::Function, b::Cmd) = Sequence((Step(a), Step(b)))

"""
    a & b

Create a [`Parallel`](@ref) where `a` and `b` run concurrently.

Supports chaining: `a & b & c` creates a single parallel group.

# Examples
```julia
# Process multiple samples in parallel
(sample1 & sample2 & sample3) >> merge_results

# Mix with sequences for complex DAGs
(trim_a >> align_a) & (trim_b >> align_b) >> joint_call
```
"""
@inline (&)(a::AbstractNode, b::AbstractNode) = Parallel((a, b))
@inline (&)(a::Parallel, b::AbstractNode) = Parallel((a.nodes..., b))
@inline (&)(a::AbstractNode, b::Parallel) = Parallel((a, b.nodes...))
@inline (&)(a::Parallel, b::Parallel) = Parallel((a.nodes..., b.nodes...))

# Lift Cmd to Step
@inline (&)(a::Cmd, b::Cmd) = Parallel((Step(a), Step(b)))
@inline (&)(a::Cmd, b::AbstractNode) = Parallel((Step(a), b))
@inline (&)(a::AbstractNode, b::Cmd) = Parallel((a, Step(b)))
@inline (&)(a::Cmd, b::Parallel) = Parallel((Step(a), b.nodes...))
@inline (&)(a::Parallel, b::Cmd) = Parallel((a.nodes..., Step(b)))

# Lift Function to Step
@inline (&)(a::Function, b::Function) = Parallel((Step(a), Step(b)))
@inline (&)(a::Function, b::AbstractNode) = Parallel((Step(a), b))
@inline (&)(a::AbstractNode, b::Function) = Parallel((a, Step(b)))
@inline (&)(a::Function, b::Parallel) = Parallel((Step(a), b.nodes...))
@inline (&)(a::Parallel, b::Function) = Parallel((a.nodes..., Step(b)))
@inline (&)(a::Cmd, b::Function) = Parallel((Step(a), Step(b)))
@inline (&)(a::Function, b::Cmd) = Parallel((Step(a), Step(b)))

#==============================================================================#
# Step Macro - Compile-time transformation only
#==============================================================================#

"""
    @step expr
    @step name = expr
    @step name(inputs => outputs) = expr

Create a [`Step`](@ref) with optional name and file dependencies.

# Examples
```julia
# Anonymous step
@step `fastqc sample.fq`

# Named step
@step align = `bwa mem ref.fa reads.fq > aligned.sam`

# With file dependencies (for dependency tracking)
@step sort("aligned.sam" => "sorted.bam") = `samtools sort aligned.sam`

# Julia function
@step qc_report = generate_multiqc_report
```
"""
macro step(expr)
    _step_impl(expr)
end

# Separate function for macro implementation - pure AST transformation
function _step_impl(expr)
    # Named step: @step name = work
    if expr isa Expr && expr.head === :(=)
        lhs, rhs = expr.args
        if lhs isa Symbol
            name = QuoteNode(lhs)
            return :(Step($name, $(esc(rhs))))
        elseif lhs isa Expr && lhs.head === :call
            # @step name(deps) = work
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
    # Anonymous step
    :(Step($(esc(expr))))
end

#==============================================================================#
# Result Types
#==============================================================================#

"""
    StepResult{S<:Step}

Result of executing a single step. Fully parametric for type stability.

# Fields
- `step::S`: The step that was executed
- `success::Bool`: Whether execution succeeded
- `duration::Float64`: Execution time in seconds
- `output::String`: Captured stdout/stderr or return value
"""
struct StepResult{S<:Step}
    step::S
    success::Bool
    duration::Float64
    output::String
end

#==============================================================================#
# Execution - Pure multiple dispatch
#==============================================================================#

# File checking as separate functions for clarity
@inline function check_inputs(inputs::Vector{String})
    for input in inputs
        isfile(input) || return (false, "Missing input file: $input")
    end
    return (true, "")
end

@inline function check_outputs(outputs::Vector{String})
    for output in outputs
        isfile(output) || return (false, "Output file not created: $output")
    end
    return (true, "")
end

"""
    execute(step::Step{Cmd}) -> StepResult

Execute a shell command step. Captures stdout/stderr.
"""
function execute(step::Step{Cmd})
    start = time()
    
    # Check inputs
    ok, msg = check_inputs(step.inputs)
    ok || return StepResult(step, false, time() - start, msg)
    
    # Execute command
    output = IOBuffer()
    try
        run(pipeline(step.work, stdout=output, stderr=output))
    catch e
        return StepResult(step, false, time() - start, 
            string("Error: ", sprint(showerror, e), "\n", String(take!(output))))
    end
    
    # Check outputs
    ok, msg = check_outputs(step.outputs)
    ok || return StepResult(step, false, time() - start, msg)
    
    return StepResult(step, true, time() - start, String(take!(output)))
end

"""
    execute(step::Step{F}) -> StepResult where F<:Function

Execute a Julia function step. Captures the return value as string.
"""
function execute(step::Step{F}) where {F<:Function}
    start = time()
    
    # Check inputs
    ok, msg = check_inputs(step.inputs)
    ok || return StepResult(step, false, time() - start, msg)
    
    # Execute function
    local result_str::String
    try
        result = step.work()
        result_str = result === nothing ? "" : string(result)
    catch e
        return StepResult(step, false, time() - start,
            string("Error: ", sprint(showerror, e)))
    end
    
    # Check outputs
    ok, msg = check_outputs(step.outputs)
    ok || return StepResult(step, false, time() - start, msg)
    
    return StepResult(step, true, time() - start, result_str)
end

#==============================================================================#
# Node Execution - Recursive dispatch on node types
#==============================================================================#

# Verbose printing as separate dispatch
struct Verbose end
struct Silent end

@inline print_start(::Silent, ::Step) = nothing
@inline print_start(::Verbose, step::Step) = println("▶ Running: $(step.name)")

@inline print_result(::Silent, ::StepResult) = nothing
@inline function print_result(::Verbose, result::StepResult)
    status = result.success ? "✓" : "✗"
    println("  $status Completed in $(round(result.duration, digits=2))s")
    result.success || println("  Error: $(result.output)")
    nothing
end

@inline print_parallel(::Silent, ::Int) = nothing
@inline print_parallel(::Verbose, n::Int) = println("⊕ Running $n branches in parallel...")

"""
    run_node(node, verbosity) -> Vector{StepResult}

Execute a pipeline node. Dispatches on node type for type-stable execution.
"""
function run_node(step::Step, verbosity)
    print_start(verbosity, step)
    result = execute(step)
    print_result(verbosity, result)
    return [result]
end

function run_node(seq::Sequence, verbosity)
    results = StepResult[]
    _run_sequence!(results, seq.nodes, verbosity)
    return results
end

# Unroll sequence execution via recursion for type stability
@inline _run_sequence!(results, ::Tuple{}, verbosity) = nothing

function _run_sequence!(results, nodes::Tuple, verbosity)
    node_results = run_node(first(nodes), verbosity)
    append!(results, node_results)
    # Stop on failure
    any_failed = false
    for r in node_results
        r.success || (any_failed = true; break)
    end
    any_failed || _run_sequence!(results, Base.tail(nodes), verbosity)
    nothing
end

function run_node(par::Parallel, verbosity)
    print_parallel(verbosity, length(par.nodes))
    
    # Spawn all branches
    spawned = _spawn_parallel(par.nodes, verbosity)
    
    # Collect results
    results = StepResult[]
    _collect_results!(results, spawned)
    return results
end

# Spawn parallel nodes via tuple recursion
@inline _spawn_parallel(::Tuple{}, verbosity) = ()

@inline function _spawn_parallel(nodes::Tuple, verbosity)
    task = @spawn run_node(first(nodes), verbosity)
    return (task, _spawn_parallel(Base.tail(nodes), verbosity)...)
end

# Collect spawned results
@inline _collect_results!(results, ::Tuple{}) = nothing

@inline function _collect_results!(results, tasks::Tuple)
    append!(results, fetch(first(tasks)))
    _collect_results!(results, Base.tail(tasks))
    nothing
end

#==============================================================================#
# Pipeline - Top-level interface
#==============================================================================#

"""
    Pipeline{N<:AbstractNode}

A complete pipeline ready for execution.

# Fields
- `root::N`: The root node of the pipeline DAG
- `name::String`: Human-readable name for the pipeline

# Examples
```julia
# Create from composed nodes
p = Pipeline(align >> sort >> index, name="alignment")

# Or wrap multiple nodes (creates a Sequence)
p = Pipeline(step1, step2, step3)
```
"""
struct Pipeline{N<:AbstractNode}
    root::N
    name::String
end

@inline Pipeline(node::AbstractNode; name::String="pipeline") = Pipeline(node, name)
@inline Pipeline(nodes::Vararg{AbstractNode}; name::String="pipeline") = 
    Pipeline(Sequence(nodes), name)

"""
    run_pipeline(p::Pipeline; verbose=true, dry_run=false)
    run_pipeline(node::AbstractNode; verbose=true, dry_run=false)

Execute a pipeline or node.

# Arguments
- `verbose::Bool=true`: Print progress information
- `dry_run::Bool=false`: Show DAG structure without executing

# Returns
- `Vector{StepResult}`: Results from all executed steps

# Examples
```julia
# Run with progress output
results = run_pipeline(pipeline)

# Silent execution
results = run_pipeline(pipeline, verbose=false)

# Preview structure
run_pipeline(pipeline, dry_run=true)
```
"""
function run_pipeline(p::Pipeline; verbose::Bool=true, dry_run::Bool=false)
    verbose && println("═══ Pipeline: $(p.name) ═══")
    
    dry_run && return _dry_run(p.root, verbose)
    
    verbosity = verbose ? Verbose() : Silent()
    start = time()
    results = run_node(p.root, verbosity)
    
    _print_summary(verbose, results, time() - start)
    return results
end

@inline run_pipeline(node::AbstractNode; kwargs...) = run_pipeline(Pipeline(node); kwargs...)

# Dry run implementation
function _dry_run(root, verbose)
    verbose && print_dag(root)
    return StepResult[]
end

# Summary printing
@inline _print_summary(verbose::Bool, results, elapsed) = 
    verbose && _print_summary_impl(results, elapsed)

function _print_summary_impl(results, elapsed)
    n_success = count(r -> r.success, results)
    n_total = length(results)
    println("═══ Completed: $n_success/$n_total steps in $(round(elapsed, digits=2))s ═══")
    nothing
end

# Make pipelines runnable with Base.run
Base.run(p::Pipeline; kwargs...) = run_pipeline(p; kwargs...)

#==============================================================================#
# DAG Visualization
#==============================================================================#

"""
    print_dag(node; indent=0)

Print the DAG structure of a pipeline node.

# Examples
```julia
pipeline = (a & b) >> c >> (d & e)
print_dag(pipeline)
# Output:
# Sequence:
#   Parallel:
#     a
#     b
#   c
#   Parallel:
#     d
#     e
```
"""
print_dag(node::AbstractNode) = print_dag(node, 0)

function print_dag(step::Step, indent::Int)
    prefix = "  " ^ indent
    println(prefix, step.name)
    isempty(step.inputs) || println(prefix, "  ← ", join(step.inputs, ", "))
    isempty(step.outputs) || println(prefix, "  → ", join(step.outputs, ", "))
    nothing
end

function print_dag(seq::Sequence, indent::Int)
    prefix = "  " ^ indent
    println(prefix, "Sequence:")
    _print_dag_nodes(seq.nodes, indent + 1)
    nothing
end

function print_dag(par::Parallel, indent::Int)
    prefix = "  " ^ indent
    println(prefix, "Parallel:")
    _print_dag_nodes(par.nodes, indent + 1)
    nothing
end

# Tuple recursion for printing
@inline _print_dag_nodes(::Tuple{}, indent) = nothing
@inline function _print_dag_nodes(nodes::Tuple, indent)
    print_dag(first(nodes), indent)
    _print_dag_nodes(Base.tail(nodes), indent)
end

#==============================================================================#
# Utilities
#==============================================================================#

"""
    steps(node::AbstractNode) -> Vector{Step}

Flatten all steps from a pipeline node into a vector.
"""
steps(step::Step) = [step]
steps(seq::Sequence) = _collect_steps(seq.nodes)
steps(par::Parallel) = _collect_steps(par.nodes)

function _collect_steps(nodes::Tuple)
    result = Step[]
    _collect_steps!(result, nodes)
    return result
end

@inline _collect_steps!(result, ::Tuple{}) = nothing
@inline function _collect_steps!(result, nodes::Tuple)
    append!(result, steps(first(nodes)))
    _collect_steps!(result, Base.tail(nodes))
end

"""
    count_steps(node::AbstractNode) -> Int

Count total steps in a pipeline node.
"""
count_steps(::Step) = 1
count_steps(seq::Sequence) = _count_steps(seq.nodes)
count_steps(par::Parallel) = _count_steps(par.nodes)

@inline _count_steps(::Tuple{}) = 0
@inline _count_steps(nodes::Tuple) = count_steps(first(nodes)) + _count_steps(Base.tail(nodes))

#==============================================================================#
# Display
#==============================================================================#

Base.show(io::IO, s::Step) = print(io, "Step(:", s.name, ")")
Base.show(io::IO, s::Sequence) = print(io, "Sequence(", join(s.nodes, " >> "), ")")
Base.show(io::IO, p::Parallel) = print(io, "Parallel(", join(p.nodes, " & "), ")")
Base.show(io::IO, p::Pipeline) = print(io, "Pipeline(\"", p.name, "\", ", count_steps(p.root), " steps)")

end # module SimplePipelines
