# Per-run state and freshness. RunContext replaces module-level globals so concurrent
# runs do not interfere, and shared sets are mutated under a lock.

const STATE_FILE = Ref(".pipeline_state")

"""
    step_id(step::Step) -> Tuple

Identifier used for state-based freshness. Stable across sessions for named steps and
shell `Cmd` work; for anonymous functions it falls back to the step name.
"""
step_id(step::Step) = (string(step.name), step_work_id(step.work), step.inputs, step.outputs)

step_work_id(c::Cmd) = ("Cmd", c.exec)
step_work_id(c::Base.AbstractCmd) = ("AbstractCmd", string(c))   # OrCmds / AndCmds / CmdRedirect
step_work_id(s::ShRun) = ("ShRun", string(nameof(s.f)))
step_work_id(f::Function) = ("Function", string(nameof(f)))
step_work_id(n::AbstractNode) = ("Node", objectid(n))
step_work_id(x) = ("Other", repr(x))

step_hash(step::Step) = hash(step_id(step))

"""
    RunContext

Mutable per-run context: verbose flag, state file path, completed-hash set with lock,
log lock for concurrent printing, max parallel jobs, and node-result memoization for
DAG sharing. Constructed once per `run(pipeline)` call.
"""
mutable struct RunContext
    verbose::Bool
    state_path::String
    state::Set{UInt64}
    persisted::Set{UInt64}
    state_lock::ReentrantLock
    log_lock::ReentrantLock
    jobs::Int
    memo::IdDict{Step, Vector{AbstractStepResult}}
    in_flight::IdDict{Step, Channel{Vector{AbstractStepResult}}}
    memory_budget::ResourceBudget   # capacity in MB
    thread_budget::ResourceBudget   # capacity in threads
    auto_spill::Bool                # serialise large step results to disk
    spill_threshold_bytes::Int      # values larger than this are spilled
    spill_dir::String               # where SpilledValue tempfiles live
end

"""
    default_jobs() -> Int

Safe default for the number of concurrent parallel/foreach branches: at most as
many as `Threads.nthreads()`, capped at 8. Spawning more branches than threads
just queues work without speeding it up, but it *does* multiply memory pressure.
"""
default_jobs() = min(Threads.nthreads(), 8)

"""
    default_memory_budget_mb() -> Int

Safe default soft cap for the per-run memory budget: 50% of total system RAM,
in MB. Steps wrapped with [`with_resources`](@ref) charge the budget; un-annotated
steps don't, so this is only an upper bound on *declared* concurrent memory. To
disable the cap entirely, pass `memory_budget_mb=0` to `run`.

Override at the user level by passing `memory_budget_mb=N` to `run(...)`.
"""
default_memory_budget_mb() = max(0, floor(Int, Sys.total_memory() / 1_000_000 / 2))

"""
    default_spill_threshold_bytes() -> Int

Default size above which a step's in-memory result is auto-spilled to disk.
10 MB — small enough to spill anything DataFrame-sized, large enough to keep
small dictionaries / counters / arrays in RAM where the I/O cost would dominate.
"""
default_spill_threshold_bytes() = 10_000_000

RunContext(; verbose::Bool=false, jobs::Int=default_jobs(), state_path::String=STATE_FILE[],
           memory_budget_mb::Int=default_memory_budget_mb(), thread_budget::Int=0,
           auto_spill::Bool=true,
           spill_threshold_bytes::Int=default_spill_threshold_bytes(),
           spill_dir::String=tempdir()) =
    RunContext(verbose, state_path, Set{UInt64}(), state_read(state_path, STATE_LAYOUT),
               ReentrantLock(), ReentrantLock(), jobs,
               IdDict{Step, Vector{AbstractStepResult}}(),
               IdDict{Step, Channel{Vector{AbstractStepResult}}}(),
               ResourceBudget(memory_budget_mb),
               ResourceBudget(thread_budget),
               auto_spill, spill_threshold_bytes, spill_dir)

"""Read persisted step hashes from the state file."""
load_state()::Set{UInt64} = state_read(STATE_FILE[], STATE_LAYOUT)

"""Write the merged state set to disk."""
function save_state!(completed::Set{UInt64}, path::String=STATE_FILE[])
    state_write(path, completed, STATE_LAYOUT)
end

"""Mark a step complete in the run context's state set; thread-safe."""
function mark_complete!(ctx::RunContext, step::Step)
    h = step_hash(step)
    lock(ctx.state_lock) do
        push!(ctx.state, h)
    end
    nothing
end

"""
    clear_state!()

Remove the pipeline state file (`.pipeline_state`), forcing all steps to run on the next
execution regardless of freshness.

See also: [`is_fresh`](@ref), [`Force`](@ref)
"""
clear_state!() = isfile(STATE_FILE[]) && rm(STATE_FILE[])

state_contains(ctx::RunContext, h::UInt64) = lock(ctx.state_lock) do
    h in ctx.state || h in ctx.persisted
end

"""
    is_fresh(step::Step) -> Bool
    is_fresh(step::Step, ctx::RunContext) -> Bool

Check if a step can be skipped based on Make-like freshness rules.

# Freshness Rules
1. **Has inputs and outputs**: Fresh if all outputs exist and are newer than all inputs.
2. **Has only outputs**: Fresh if outputs exist and step was previously completed.
3. **Has only inputs**: Not fresh if any input file is missing; otherwise fresh if the step was previously completed (state cannot detect edited inputs without declared outputs).
4. **No file dependencies**: Fresh if step was previously completed (state-based tracking).

State is persisted in `.pipeline_state`. Completions during a run are batched and
written once when `run()` finishes.

See also: [`Force`](@ref), [`clear_state!`](@ref)
"""
is_fresh(step::Step) = is_fresh(step, RunContext())

function is_fresh(step::Step, ctx::RunContext)
    has_in, has_out = !isempty(step.inputs), !isempty(step.outputs)
    # Missing declared inputs always invalidate (including inputs-only steps that
    # otherwise rely on state alone — they cannot run without files on disk).
    has_in && !all(isfile, step.inputs) && return false
    has_out && !all(isfile, step.outputs) && return false
    has_in && has_out && return inputs_older_than_outputs(step)
    state_contains(ctx, step_hash(step))
end

function inputs_older_than_outputs(step::Step)
    oldest_out = minimum(mtime, step.outputs)
    for inp in step.inputs
        isfile(inp) || return false
        mtime(inp) >= oldest_out && return false
    end
    true
end
