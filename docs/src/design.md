# Design

## Interface Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    SimplePipelines.jl                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  @step name = sh"cmd"       Shell step                      │
│  @step name = sh"cmd > f"   Shell with redirection/pipes    │
│  @step name = () -> ...     Julia step                      │
│                                                             │
│  a >> b                     Sequential; pass output to next (function step) │
│  a & b                      Parallel: a and b together      │
│  a |> b                     Pipe: run b with a's output(s)  │
│  a >>> b                    Same input: both get same context │
│  a .>> b                    Broadcast: attach b to each branch of a │
│                                                             │
│  run(p)                     Execute the pipeline            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Type Hierarchy

```
AbstractNode
    │
    ├── Step{F}           Single unit of work
    │                     F = Cmd | Function
    │
    ├── Sequence{T}       Sequential execution
    │                     T = Tuple of nodes
    │
    └── Parallel{T}       Concurrent execution
                          T = Tuple of nodes
```

All types are **fully parametric**—the compiler knows exact types at every level.

## Composition Model

```
# User writes:
sh"echo a" >> (sh"echo b" & sh"echo c") >> sh"echo d"

# Becomes:
Sequence{Tuple{
    Step{Cmd},
    Parallel{Tuple{Step{Cmd}, Step{Cmd}}},
    Step{Cmd}
}}
```

The complete structure is encoded in the type, enabling full compile-time specialization.

## Execution Flow

Execution is recursive: dispatch on node type and recurse.

```
run(Pipeline)
       │
       ▼
run_node(root, v, force)  ─── dispatch on node type
       │
       ├─► Step:     execute(step) → StepResult
       ├─► Sequence: run_node each in order; break on first failure
       ├─► Parallel: @spawn run_node each; fetch and concat
       ├─► ForEach:  (String) find file matches, or (Vector) iterate items; get nodes from block (cycle check), then run like Parallel
       └─► Retry/Fallback/Branch/Timeout/Force/Reduce: recurse on inner node(s)
       │
       ▼
Vector{AbstractStepResult}
```

## Key Design Decisions

### 1. Tuples, Not Vectors

```julia
# ✗ Vector: type information lost
Sequence(nodes::Vector{AbstractNode})

# ✓ Tuple: exact types preserved
Sequence{Tuple{Step{Cmd}, Step{Function}}}
```

Tuples enable the compiler to generate specialized code for each node.

### 2. Multiple Dispatch, Not Type Checks

```julia
# ✗ Runtime type checking (type unstable)
function run_node(node)
    if node isa Step
        # ...
    elseif node isa Sequence
        # ...
    end
end

# ✓ Multiple dispatch (type stable)
run_node(step::Step, v) = execute(step)
run_node(seq::Sequence, v) = _run_sequence!([], seq.nodes, v)
run_node(par::Parallel, v) = _spawn_parallel(par.nodes, v)
```

### 3. Tuple Recursion

Iterate tuples in a type-stable way:

```julia
# Base case
_run_sequence!(results, ::Tuple{}, v) = nothing

# Recursive case
function _run_sequence!(results, nodes::Tuple, v)
    append!(results, run_node(first(nodes), v))
    _run_sequence!(results, Base.tail(nodes), v)
end
```

The compiler unrolls this into efficient, specialized code.

### 4. Verbosity as Types

```julia
struct Verbose end
struct Silent end

log_start(::Silent, ::Step) = nothing
log_start(::Verbose, s::Step) = println("▶ ", step_label(s))
```

Dead code elimination removes printing when `verbose=false`.

## Performance Characteristics

| Aspect | Design Choice | Benefit |
|--------|--------------|---------|
| Node storage | Tuples | Full type info, inline storage |
| Dispatch | Multiple dispatch | Zero runtime type checks |
| Iteration | Recursion | Compiler unrolling |
| Operators | `@inline` | Zero call overhead |
| Verbosity | Singleton types | Dead code elimination |
