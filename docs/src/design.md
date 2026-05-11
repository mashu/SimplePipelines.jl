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
    ├── Sequence          Sequential execution
    │                     children::Vector{AbstractNode}
    │
    └── Parallel          Concurrent execution
                          children::Vector{AbstractNode}
```

Leaf and wrapper nodes are parametric where it matters (`Step{F}`,
`Retry{N}`, `StepResult{S,I,O,V}`), while wide child lists use
`Vector{AbstractNode}` to keep large DAGs practical to compile.

## Composition Model

```
# User writes:
sh"echo a" >> (sh"echo b" & sh"echo c") >> sh"echo d"

# Becomes:
Sequence(AbstractNode[
    Step{Cmd}(...),
    Parallel(AbstractNode[Step{Cmd}(...), Step{Cmd}(...)]),
    Step{Cmd}(...),
])
```

The node kind still drives execution through dispatch. Child lists are stored as
vectors so pipelines with hundreds or thousands of nodes do not create enormous
tuple types.

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

### 1. Vector-Backed Child Lists

```julia
# Used intentionally for scalable graph shape
Sequence(nodes::Vector{AbstractNode})

# Leaf/runtime payloads remain parametric
Step{Cmd}
StepResult{S,I,O,V}
```

This trades a small amount of dynamic dispatch at graph traversal points for
much lower compile-time and memory pressure on wide DAGs.

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

### 3. Bounded Parallel Traversal

```julia
tasks = [@spawn run_node(nodes[k], ctx, forced, contexts[k]) for k in rng]
append!(results, fetch(t))
```

Parallel and `ForEach` traversal runs in batches controlled by `jobs`, with
optional resource hints from `with_resources`.

### 4. Per-Run Context

```julia
ctx = RunContext(verbose=verbose, jobs=jobs, state_path=state_path)
run_node(root, ctx, force, nothing)
```

Mutable run state, logging locks, resource budgets, spill policy, and memoization
live in one `RunContext`, so concurrent `run(...)` calls do not share scheduler
state.

## Performance Characteristics

| Aspect | Design Choice | Benefit |
|--------|--------------|---------|
| Node storage | `Vector{AbstractNode}` for children | Scales to wide DAGs |
| Dispatch | Multiple dispatch | Avoids runtime type-check trees |
| Iteration | Bounded loops + `@spawn` | Predictable concurrency |
| Results | Parametric `StepResult` values in `Vector{AbstractStepResult}` | Heterogeneous traces without `Any` fields |
| State | Per-run `RunContext` | Isolates memoization and resource budgets |
