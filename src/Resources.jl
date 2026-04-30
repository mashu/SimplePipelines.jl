# Per-step resource declarations and per-run memory budgets. Lets a parallel
# scheduler hold a soft cap on concurrent memory usage so that, e.g., 100 parallel
# branches each loading a 4 GB DataFrame do not OOM the machine.
#
# Usage:
#     heavy = with_resources(my_step; mem_mb=4_000, threads=4)
#     run(Pipeline(heavy & heavy_b); memory_budget_mb=8_000)
#
# Branches with `mem_mb > 0` block (cooperatively, on a Condition) until the
# RunContext has enough free budget to admit them. `mem_mb=0` is unconstrained
# (default) so existing pipelines keep their semantics.

"""
    Resources(; mem_mb=0, threads=1)

Resource hint for a node. `mem_mb` is the peak in-memory footprint expected while
the node runs (megabytes); `threads` is informational. A `mem_mb` of `0` means
"no declared budget" — the node is treated as cheap.
"""
struct Resources
    mem_mb::Int
    threads::Int
end
Resources(; mem_mb::Int=0, threads::Int=1) = Resources(mem_mb, threads)

const NO_RESOURCES = Resources(0, 1)

"""
    Resourced{N} <: AbstractNode

Wraps a node with `Resources`. The parallel scheduler reads `node_resources` and
enforces the memory budget before starting the wrapped node.
"""
struct Resourced{N<:AbstractNode} <: AbstractNode
    node::N
    resources::Resources
end

"""
    with_resources(node; mem_mb=0, threads=1) -> Resourced
    with_resources(cmd; ...) -> Resourced  (lifts Cmd via Step)
    with_resources(f::Function; ...) -> Resourced  (lifts Function via Step)

Annotate a node with resource hints used by the parallel scheduler.
"""
with_resources(node::AbstractNode; mem_mb::Int=0, threads::Int=1) =
    Resourced(node, Resources(mem_mb, threads))
with_resources(x; kwargs...) = with_resources(node_operand(x); kwargs...)

"""
    node_resources(node) -> Resources

Total declared memory budget for executing `node` (sum across nested `Resourced`
wrappers; the scheduler holds the highest peak across one branch).
"""
node_resources(::AbstractNode) = NO_RESOURCES
node_resources(r::Resourced) = r.resources

"""Budget primitive owned by RunContext. Threads block on `cond` until enough mem_mb is free."""
mutable struct MemoryBudget
    capacity_mb::Int       # 0 = unlimited
    used_mb::Int
    cond::Threads.Condition
end
MemoryBudget(capacity_mb::Int) = MemoryBudget(capacity_mb, 0, Threads.Condition())

"""Acquire `mem_mb` from the budget; blocks until available. No-op when capacity_mb == 0 or mem_mb == 0."""
function acquire_memory!(b::MemoryBudget, mem_mb::Int)
    (b.capacity_mb == 0 || mem_mb <= 0) && return
    lock(b.cond)
    try
        # An over-large request is admitted alone (rather than deadlocking) when
        # nothing else is holding the budget.
        admit_mb = min(mem_mb, b.capacity_mb)
        while b.used_mb + admit_mb > b.capacity_mb
            wait(b.cond)
        end
        b.used_mb += admit_mb
    finally
        unlock(b.cond)
    end
    nothing
end

function release_memory!(b::MemoryBudget, mem_mb::Int)
    (b.capacity_mb == 0 || mem_mb <= 0) && return
    lock(b.cond)
    try
        admit_mb = min(mem_mb, b.capacity_mb)
        b.used_mb = max(0, b.used_mb - admit_mb)
        notify(b.cond; all=true)
    finally
        unlock(b.cond)
    end
    nothing
end
