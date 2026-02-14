# Extending SimplePipelines

SimplePipelines is designed for easy extension via Julia's multiple dispatch.

## Testing and coverage

Run the test suite:

```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

## Building the docs

From the repo root, install Documenter (if needed) and run the doc build. **Note:** `Pkg.activate` does not accept an `extras` argument; use `Pkg.add("Documenter")` to pull in the docs dependency.

```bash
julia --project=. -e 'using Pkg; Pkg.add("Documenter"); using Documenter; using SimplePipelines; cd("docs"); include("make.jl")'
```

Or in the Julia REPL (start with `julia --project=.`): run `using Pkg; Pkg.add("Documenter"); cd("docs"); include("make.jl")`.

Run tests with code coverage (for local profiling or CI):

```bash
julia --project=. -e 'using Pkg; Pkg.test(coverage=true)'
```

Coverage is reported in `lcov.info`; CI uploads it to Codecov.

## Architecture

All nodes inherit from `AbstractNode`. To add custom behavior, define a new subtype and implement dispatch methods:

```
AbstractNode
    ├── Step{F}           # Leaf node
    ├── Sequence{T}       # Sequential
    ├── Parallel{T}       # Concurrent
    ├── Retry{N}          # Retry on failure
    ├── Fallback{A,B}     # Try A, else B
    └── Branch{C,T,F}     # Conditional
```

## Custom Node in 4 Steps

```julia
using SimplePipelines
import SimplePipelines: AbstractNode, run_node, print_dag, count_steps, steps

# 1. Define type (parametric for type stability)
struct Timeout{N<:AbstractNode} <: AbstractNode
    node::N
    seconds::Float64
end

# 2. Implement execution
function run_node(t::Timeout, verbosity)
    # ... timeout logic ...
    return run_node(t.node, verbosity)
end

# 3. Implement visualization
print_dag(t::Timeout, indent::Int) = begin
    println("  "^indent, "Timeout($(t.seconds)s):")
    print_dag(t.node, indent + 1)
end

# 4. Implement utilities
count_steps(t::Timeout) = count_steps(t.node)
steps(t::Timeout) = steps(t.node)
```

## Custom Operator

```julia
import Base: ^

# Node^3 means repeat 3 times
^(node::AbstractNode, n::Int) = Repeat(node, n)
```

## Custom Step Executor

Extend `execute` for new work types:

```julia
struct HTTPGet
    url::String
end

function SimplePipelines.execute(step::Step{HTTPGet})
    start = time()
    resp = HTTP.get(step.work.url)
    return StepResult(step, resp.status == 200, time() - start, String(resp.body))
end

# Usage: Step(:fetch, HTTPGet("https://api.example.com"))
```

## Type Stability Rules

1. Use parametric types: `struct MyNode{T<:AbstractNode}` not `children::Vector{AbstractNode}`
2. Use tuple recursion for heterogeneous collections
3. Dispatch on types, don't check with `isa`
