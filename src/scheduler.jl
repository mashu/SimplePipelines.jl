# Admission control. Every running step holds `threads` of the run's `jobs` CPU
# slots and its declared memory, so the limits hold however deeply parallel nodes
# are nested. Waiting steps are admitted in priority order: the priority of a step is
# its position in the DAG, which makes the scheduler finish one branch (sample)
# before starting the next instead of running every first step first. A step that
# does not fit blocks the ones behind it, so large steps are never starved.

mutable struct Waiter
    const priority::Vector{Int}
    const threads::Int
    const mem_mb::Int
    const ready::Threads.Condition
    admitted::Bool
end

mutable struct Pool
    const lock::ReentrantLock
    const jobs::Int
    const mem_mb::Int                   # 0 = no memory budget
    used_jobs::Int
    used_mem::Int
    const queue::Vector{Waiter}         # sorted by priority, FIFO among equals
end

function Pool(jobs::Integer, mem_mb::Integer)
    jobs >= 1 || throw(ArgumentError("jobs must be at least 1, got $jobs"))
    mem_mb >= 0 || throw(ArgumentError("mem_mb must be ≥ 0, got $mem_mb"))
    Pool(ReentrantLock(), Int(jobs), Int(mem_mb), 0, 0, Waiter[])
end

"""Resources actually granted: requests larger than the budget are clamped so they can run alone."""
struct Grant
    threads::Int
    mem_mb::Int
end

fits(pool::Pool, g::Grant) =
    pool.used_jobs + g.threads <= pool.jobs && (pool.mem_mb == 0 || pool.used_mem + g.mem_mb <= pool.mem_mb)

function occupy!(pool::Pool, g::Grant)
    pool.used_jobs += g.threads
    pool.used_mem += g.mem_mb
    nothing
end

function acquire!(pool::Pool, priority::Vector{Int}, threads::Int, mem_mb::Int)
    g = Grant(clamp(threads, 1, pool.jobs), pool.mem_mb == 0 ? 0 : clamp(mem_mb, 0, pool.mem_mb))
    lock(pool.lock) do
        if isempty(pool.queue) && fits(pool, g)
            occupy!(pool, g)
            return
        end
        w = Waiter(priority, g.threads, g.mem_mb, Threads.Condition(pool.lock), false)
        insert!(pool.queue, searchsortedlast(pool.queue, w; by=x -> x.priority) + 1, w)
        dispatch!(pool)
        while !w.admitted
            wait(w.ready)
        end
    end
    g
end

function release!(pool::Pool, g::Grant)
    lock(pool.lock) do
        pool.used_jobs -= g.threads
        pool.used_mem -= g.mem_mb
        dispatch!(pool)
    end
    nothing
end

# Admit waiters from the head of the queue while they fit. Caller holds the lock.
function dispatch!(pool::Pool)
    while !isempty(pool.queue)
        w = first(pool.queue)
        g = Grant(w.threads, w.mem_mb)
        fits(pool, g) || break
        popfirst!(pool.queue)
        occupy!(pool, g)
        w.admitted = true
        notify(w.ready)
    end
end

#==============================================================================#
# Watchdog: terminate the processes of a node when its Timeout expires or the run is
# interrupted. Watchdogs nest; a process is registered with every enclosing one.
#==============================================================================#

"""A detached process group, terminated as a whole (the shell and its children)."""
struct ProcessGroup
    pid::Int32
end
# pid 0 means the process had already exited; kill(0) would signal our own group.
terminate(g::ProcessGroup) = (g.pid > 0 && ccall(:kill, Cint, (Cint, Cint), -g.pid, 15); nothing)
terminate(p::Union{Base.Process,Base.ProcessChain}) = (kill(p); nothing)

const Running = Union{ProcessGroup,Base.Process,Base.ProcessChain}

mutable struct Watchdog
    const lock::ReentrantLock
    @atomic expired::Bool
    const running::Vector{Running}
    const parent::Union{Nothing,Watchdog}
end
Watchdog(parent::Union{Nothing,Watchdog}=nothing) = Watchdog(ReentrantLock(), false, Running[], parent)

expired(::Nothing) = false
expired(w::Watchdog) = (@atomic w.expired) || expired(w.parent)

watch!(::Nothing, _) = nothing
function watch!(w::Watchdog, r::Running)
    lock(w.lock) do
        push!(w.running, r)
        (@atomic w.expired) && terminate(r)
    end
    watch!(w.parent, r)
end

unwatch!(::Nothing, _) = nothing
function unwatch!(w::Watchdog, r::Running)
    lock(() -> filter!(x -> x !== r, w.running), w.lock)
    unwatch!(w.parent, r)
end

function expire!(w::Watchdog)
    lock(w.lock) do
        @atomic w.expired = true
        foreach(terminate, w.running)
    end
    nothing
end
