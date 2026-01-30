module SimplePipelines

export Step, @step, Sequence, Parallel, Pipeline
export Retry, Fallback, Branch, Timeout
export Map, Reduce, ForEach
export run_pipeline, count_steps, steps, print_dag

import Base: >>, &, |, ^

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

"""
    Retry{N<:AbstractNode}

Retry a node up to `max_attempts` times on failure, with optional delay between attempts.

# Examples
```julia
# Retry up to 3 times
Retry(flaky_step, 3)

# Retry with delay between attempts
Retry(api_call, 5, delay=2.0)

# Using | operator for simple fallback (try once, then fallback)
risky_step | safe_fallback
```
"""
struct Retry{N<:AbstractNode} <: AbstractNode
    node::N
    max_attempts::Int
    delay::Float64
end

@inline Retry(node::AbstractNode, max_attempts::Int=3; delay::Real=0.0) = 
    Retry(node, max_attempts, Float64(delay))

"""
    Fallback{A<:AbstractNode, B<:AbstractNode}

Try the primary node; if it fails, run the fallback node.
Created with the `|` operator.

# Examples
```julia
# If fast_method fails, use slow_method
fast_method | slow_method

# Chain multiple fallbacks
method_a | method_b | method_c
```
"""
struct Fallback{A<:AbstractNode, B<:AbstractNode} <: AbstractNode
    primary::A
    fallback::B
end

"""
    Branch{C<:Function, T<:AbstractNode, F<:AbstractNode}

Conditional execution: run `if_true` when `condition()` returns true, otherwise `if_false`.

# Examples
```julia
# Branch based on file size
Branch(() -> filesize("data.txt") > 1_000_000, large_file_pipeline, small_file_pipeline)

# Branch based on environment
Branch(() -> haskey(ENV, "DEBUG"), debug_steps, normal_steps)
```
"""
struct Branch{C<:Function, T<:AbstractNode, F<:AbstractNode} <: AbstractNode
    condition::C
    if_true::T
    if_false::F
end

"""
    Timeout{N<:AbstractNode}

Fail if a node doesn't complete within the specified time.

# Examples
```julia
# 30 second timeout
Timeout(long_step, 30.0)

# Combine with retry and fallback
Retry(Timeout(primary, 10.0), 3) | fallback

# Using ^ operator: step^3 = retry 3 times
Timeout(api_call, 5.0)^3 | backup
```
"""
struct Timeout{N<:AbstractNode} <: AbstractNode
    node::N
    seconds::Float64
end

"""
    Reduce{F<:Function, N<:AbstractNode}

Run a node (typically Parallel) and combine outputs with a reducing function.
The function receives a vector of outputs from successful steps.

# Examples
```julia
# Combine parallel outputs
Reduce(a & b & c) do outputs
    join(outputs, "\\n")
end

# With a named function
Reduce(combine_results, process_a & process_b)

# In a pipeline
fetch >> Reduce(merge, analyze_a & analyze_b) >> report
```
"""
struct Reduce{F<:Function, N<:AbstractNode} <: AbstractNode
    reducer::F
    node::N
    name::Symbol
end

@inline Reduce(f::Function, node::AbstractNode; name::Symbol=:reduce) = 
    Reduce{typeof(f), typeof(node)}(f, node, name)

# Curried form for do-block: Reduce(node) do outputs ... end
@inline Reduce(node::AbstractNode; name::Symbol=:reduce) = f -> Reduce(f, node; name=name)

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

"""
    a | b

Create a [`Fallback`](@ref): if `a` fails, run `b`.

# Examples
```julia
# Try primary, fallback to secondary on failure
primary_method | fallback_method

# Chain multiple fallbacks
fast | medium | slow
```
"""
@inline (|)(a::AbstractNode, b::AbstractNode) = Fallback(a, b)
@inline (|)(a::Fallback, b::AbstractNode) = Fallback(a.primary, Fallback(a.fallback, b))

# Lift Cmd to Step
@inline (|)(a::Cmd, b::Cmd) = Fallback(Step(a), Step(b))
@inline (|)(a::Cmd, b::AbstractNode) = Fallback(Step(a), b)
@inline (|)(a::AbstractNode, b::Cmd) = Fallback(a, Step(b))

# Lift Function to Step
@inline (|)(a::Function, b::Function) = Fallback(Step(a), Step(b))
@inline (|)(a::Function, b::AbstractNode) = Fallback(Step(a), b)
@inline (|)(a::AbstractNode, b::Function) = Fallback(a, Step(b))
@inline (|)(a::Cmd, b::Function) = Fallback(Step(a), Step(b))
@inline (|)(a::Function, b::Cmd) = Fallback(Step(a), Step(b))

"""
    a^n

Create a [`Retry`](@ref) that attempts `a` up to `n` times.

# Examples
```julia
# Retry step up to 3 times
flaky_step^3

# Combine with fallback
flaky_step^3 | backup

# With timeout
Timeout(api_call, 5.0)^3
```
"""
@inline (^)(a::AbstractNode, n::Int) = Retry(a, n)
@inline (^)(a::Cmd, n::Int) = Retry(Step(a), n)
@inline (^)(a::Function, n::Int) = Retry(Step(a), n)

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
# Retry, Fallback, Branch Execution
#==============================================================================#

@inline print_retry(::Silent, ::Int, ::Int) = nothing
@inline print_retry(::Verbose, attempt::Int, max::Int) = println("↻ Attempt $attempt/$max")

@inline print_retry_fail(::Silent, ::Float64) = nothing
@inline print_retry_fail(::Verbose, delay::Float64) = 
    delay > 0 ? println("  Failed, retrying in $(delay)s...") : println("  Failed, retrying...")

function run_node(r::Retry, verbosity)
    local results::Vector{StepResult}
    
    for attempt in 1:r.max_attempts
        print_retry(verbosity, attempt, r.max_attempts)
        results = run_node(r.node, verbosity)
        
        # Success - return immediately
        all(res -> res.success, results) && return results
        
        # Failure - maybe retry
        if attempt < r.max_attempts
            print_retry_fail(verbosity, r.delay)
            r.delay > 0 && sleep(r.delay)
        end
    end
    
    return results
end

@inline print_fallback(::Silent) = nothing
@inline print_fallback(::Verbose) = println("↯ Primary failed, trying fallback...")

function run_node(f::Fallback, verbosity)
    results = run_node(f.primary, verbosity)
    
    # If primary succeeded, return
    all(r -> r.success, results) && return results
    
    # Primary failed, try fallback
    print_fallback(verbosity)
    return run_node(f.fallback, verbosity)
end

@inline print_branch(::Silent, ::Bool) = nothing
@inline print_branch(::Verbose, cond::Bool) = 
    println("? Condition: $(cond ? "true → if_true branch" : "false → if_false branch")")

function run_node(b::Branch, verbosity)
    cond_result = b.condition()
    print_branch(verbosity, cond_result)
    
    return cond_result ? run_node(b.if_true, verbosity) : run_node(b.if_false, verbosity)
end

@inline print_timeout(::Silent, ::Float64) = nothing
@inline print_timeout(::Verbose, secs::Float64) = println("⏱ Timeout: $(secs)s")

@inline print_timeout_fail(::Silent) = nothing
@inline print_timeout_fail(::Verbose) = println("  ✗ Timeout exceeded")

function run_node(t::Timeout, verbosity)
    print_timeout(verbosity, t.seconds)
    
    # Run in a task
    result_channel = Channel{Vector{StepResult}}(1)
    task = @async put!(result_channel, run_node(t.node, verbosity))
    
    # Wait with timeout
    deadline = time() + t.seconds
    while !isready(result_channel) && time() < deadline
        sleep(0.01)
    end
    
    if isready(result_channel)
        return take!(result_channel)
    else
        # Timeout - return failure
        print_timeout_fail(verbosity)
        return [StepResult(Step(:timeout, `true`), false, t.seconds, "Timeout after $(t.seconds)s")]
    end
end

@inline print_reduce(::Silent, ::Symbol) = nothing
@inline print_reduce(::Verbose, name::Symbol) = println("⊕ Reducing: $name")

function run_node(r::Reduce, verbosity)
    start = time()
    
    # Run the inner node
    results = run_node(r.node, verbosity)
    
    # Collect outputs from successful steps
    outputs = String[res.output for res in results if res.success]
    
    # If any step failed, propagate failure
    if length(outputs) < length(results)
        failed = first(res for res in results if !res.success)
        return vcat(results, [StepResult(Step(r.name, r.reducer), false, time() - start, 
            "Reduce aborted: upstream step failed")])
    end
    
    print_reduce(verbosity, r.name)
    
    # Apply reducer
    local reduced_output::String
    try
        result = r.reducer(outputs)
        reduced_output = result === nothing ? "" : string(result)
    catch e
        return vcat(results, [StepResult(Step(r.name, r.reducer), false, time() - start,
            "Reduce error: $(sprint(showerror, e))")])
    end
    
    # Return all results plus the reduce result
    reduce_result = StepResult(Step(r.name, r.reducer), true, time() - start, reduced_output)
    return vcat(results, [reduce_result])
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

function print_dag(r::Retry, indent::Int)
    prefix = "  " ^ indent
    println(prefix, "Retry(max=$(r.max_attempts), delay=$(r.delay)s):")
    print_dag(r.node, indent + 1)
    nothing
end

function print_dag(f::Fallback, indent::Int)
    prefix = "  " ^ indent
    println(prefix, "Fallback:")
    println(prefix, "  primary:")
    print_dag(f.primary, indent + 2)
    println(prefix, "  fallback:")
    print_dag(f.fallback, indent + 2)
    nothing
end

function print_dag(b::Branch, indent::Int)
    prefix = "  " ^ indent
    println(prefix, "Branch:")
    println(prefix, "  if_true:")
    print_dag(b.if_true, indent + 2)
    println(prefix, "  if_false:")
    print_dag(b.if_false, indent + 2)
    nothing
end

function print_dag(t::Timeout, indent::Int)
    prefix = "  " ^ indent
    println(prefix, "Timeout($(t.seconds)s):")
    print_dag(t.node, indent + 1)
    nothing
end

function print_dag(r::Reduce, indent::Int)
    prefix = "  " ^ indent
    println(prefix, "Reduce($(r.name)):")
    print_dag(r.node, indent + 1)
    nothing
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
steps(r::Retry) = steps(r.node)
steps(f::Fallback) = vcat(steps(f.primary), steps(f.fallback))
steps(b::Branch) = vcat(steps(b.if_true), steps(b.if_false))
steps(t::Timeout) = steps(t.node)
steps(r::Reduce) = steps(r.node)

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
count_steps(r::Retry) = count_steps(r.node)
count_steps(f::Fallback) = count_steps(f.primary) + count_steps(f.fallback)
count_steps(b::Branch) = max(count_steps(b.if_true), count_steps(b.if_false))
count_steps(t::Timeout) = count_steps(t.node)
count_steps(r::Reduce) = count_steps(r.node) + 1  # +1 for the reduce step itself

@inline _count_steps(::Tuple{}) = 0
@inline _count_steps(nodes::Tuple) = count_steps(first(nodes)) + _count_steps(Base.tail(nodes))

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
Base.show(io::IO, p::Pipeline) = print(io, "Pipeline(\"", p.name, "\", ", count_steps(p.root), " steps)")

#==============================================================================#
# Map Helper - Fan-out over collection
#==============================================================================#

"""
    Map(f, items) -> Parallel

Apply function `f` to each item, creating parallel steps.

# Examples
```julia
# Process files in parallel
Map(["a.txt", "b.txt", "c.txt"]) do file
    @step Symbol(file) = `process \$file`
end

# With named steps
samples = ["sample_A", "sample_B", "sample_C"]
Map(samples) do s
    Step(Symbol("process_", s), `analyze \$s.fastq`)
end >> merge_step
```
"""
function Map(f::Function, items)
    nodes = [f(item) for item in items]
    isempty(nodes) && error("Map requires at least one item")
    length(nodes) == 1 && return first(nodes)
    return reduce(&, nodes)
end

# Curried form: Map(items) do ... end
Map(f::Function) = items -> Map(f, items)

#==============================================================================#
# ForEach - Pattern-based file discovery with wildcard extraction
#==============================================================================#

"""
    ForEach(pattern::String) do wildcard...
        # commands using wildcard
    end

Discover files matching `pattern` with `{name}` placeholders and create a
parallel branch for each match. Extracted values are passed to your function.

# Examples
```julia
# Single file per match - just return a Cmd
ForEach("{sample}.fastq") do sample
    `process \$(sample).fastq`
end

# Multi-step pipeline - chain with >>
ForEach("fastq/{sample}_R1.fq.gz") do sample
    `pear -f \$(sample)_R1 -r \$(sample)_R2` >> `analyze \$(sample)`
end

# Multiple wildcards
ForEach("data/{project}/{sample}.csv") do project, sample
    `process \$(project)/\$(sample).csv`
end

# Chain ForEach with downstream steps
ForEach("{id}.fastq") do id
    `align \$(id).fastq`
end >> @step merge = `merge *.bam`
```
"""
function ForEach(f::Function, pattern::String)
    # Extract {name} wildcards from pattern
    wildcard_regex = r"\{(\w+)\}"
    wildcard_names = [m.captures[1] for m in eachmatch(wildcard_regex, pattern)]
    isempty(wildcard_names) && error("ForEach pattern must contain at least one {wildcard}: $pattern")
    
    # Build regex to match files and extract wildcard values
    # First replace {name} with a placeholder, escape regex special chars, then restore capture group
    placeholder = "\x00WILDCARD\x00"
    temp = replace(pattern, wildcard_regex => placeholder)
    # Escape regex special chars (not backslash since pattern shouldn't contain it)
    for c in ".+^*?\$()[]|"
        temp = replace(temp, string(c) => "\\" * c)
    end
    regex_pattern = replace(temp, placeholder => "([^/]+)")
    regex = Regex("^" * regex_pattern * "\$")
    
    # Find matching files using simple glob
    matches = _foreach_glob(pattern, regex)
    isempty(matches) && error("ForEach: no files match pattern '$pattern'")
    
    # Create a pipeline branch for each match
    nodes = AbstractNode[]
    for captured in matches
        result = length(captured) == 1 ? f(captured[1]) : f(captured...)
        # Auto-lift Cmd or Function to Step for convenience
        node = result isa AbstractNode ? result : Step(result)
        push!(nodes, node)
    end
    
    length(nodes) == 1 && return first(nodes)
    return reduce(&, nodes)
end

# do-block syntax: ForEach("pattern") do x ... end
ForEach(pattern::String) = f -> ForEach(f, pattern)

# Simple recursive glob that finds files matching pattern with {wildcards}
function _foreach_glob(pattern::String, regex::Regex)
    # Split pattern into parts
    parts = split(pattern, "/")
    
    # Find first part with wildcard
    first_wild = findfirst(p -> contains(p, "{"), parts)
    first_wild === nothing && error("Pattern must contain {wildcard}")
    
    # Base directory (parts before first wildcard)
    base_parts = parts[1:first_wild-1]
    base_dir = isempty(base_parts) ? "." : joinpath(base_parts...)
    
    isdir(base_dir) || return Vector{Vector{String}}()
    
    # Collect all matching files recursively
    matches = Vector{Vector{String}}()
    _foreach_scan!(matches, base_dir, parts, first_wild, regex, pattern)
    return matches
end

function _foreach_scan!(matches::Vector{Vector{String}}, dir::String, parts::Vector{<:AbstractString}, idx::Int, regex::Regex, pattern::String)
    idx > length(parts) && return
    
    is_last = idx == length(parts)
    
    for entry in readdir(dir)
        full_path = joinpath(dir, entry)
        
        if is_last
            # Final part - must be a file and match regex
            if isfile(full_path)
                # Build the path as it appears in the pattern (without ./ prefix if pattern doesn't have it)
                relative_parts = parts[1:idx-1]
                match_path = isempty(relative_parts) ? entry : joinpath(relative_parts..., entry)
                m = match(regex, match_path)
                if m !== nothing
                    push!(matches, collect(String, m.captures))
                end
            end
        else
            # Intermediate part - must be a directory
            if isdir(full_path)
                _foreach_scan!(matches, full_path, parts, idx + 1, regex, pattern)
            end
        end
    end
end

end # module SimplePipelines
