# Per-step resource declarations and per-run resource budgets. Lets a parallel
# scheduler hold soft caps on concurrent memory and threads so that, e.g.,
# 100 parallel branches each loading a 4 GB DataFrame do not OOM the machine.
#
# Usage:
#     heavy = with_resources(my_step; mem_mb=4_000, threads=4)
#     run(Pipeline(heavy & heavy_b); memory_budget_mb=8_000, thread_budget=8)
#
# Branches with declared resources block (cooperatively, on a Condition) until
# the RunContext has enough free budget to admit them. Zero means "no cap" so
# existing pipelines keep their semantics.

"""
    Resources(; mem_mb=0, threads=1)

Resource hint for a node. `mem_mb` is the peak in-memory footprint (megabytes)
expected while the node runs; `threads` is the number of CPU threads it intends
to use. A `0` field means "do not charge against the corresponding budget".
"""
struct Resources
    mem_mb::Int
    threads::Int
end
Resources(; mem_mb::Int=0, threads::Int=1) = Resources(mem_mb, threads)

"""
    Resourced{N} <: AbstractNode

Wraps a node with `Resources`. The runtime acquires the matching budget slots on
the `RunContext` before executing the wrapped node, releasing them on
completion.
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

"""Generic counting semaphore guarded by a `Condition`. `capacity == 0` disables the cap."""
mutable struct ResourceBudget
    capacity::Int
    used::Int
    cond::Condition
end
ResourceBudget(capacity::Int) = ResourceBudget(capacity, 0, Condition())

"""
    acquire!(b::ResourceBudget, n::Int)

Acquire `n` units; blocks until available. No-op when the budget is uncapped or
`n <= 0`. An oversized request (`n > capacity`) is admitted alone rather than
deadlocking. Returns the actual amount admitted (so the caller releases the
same number).
"""
function acquire!(b::ResourceBudget, n::Int)
    (b.capacity == 0 || n <= 0) && return 0
    admit = min(n, b.capacity)
    lock(b.cond)
    try
        while b.used + admit > b.capacity
            wait(b.cond)
        end
        b.used += admit
    finally
        unlock(b.cond)
    end
    admit
end

"""
    release!(b::ResourceBudget, n::Int)

Release `n` previously-acquired units and wake any waiters. No-op when uncapped
or `n <= 0`. `n` should be the value returned by the matching `acquire!`.
"""
function release!(b::ResourceBudget, n::Int)
    (b.capacity == 0 || n <= 0) && return
    lock(b.cond)
    try
        b.used = max(0, b.used - n)
        notify(b.cond; all=true)
    finally
        unlock(b.cond)
    end
    nothing
end
