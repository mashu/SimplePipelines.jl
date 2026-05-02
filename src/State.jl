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
end

RunContext(; verbose::Bool=false, jobs::Int=8, state_path::String=STATE_FILE[],
           memory_budget_mb::Int=0, thread_budget::Int=0) =
    RunContext(verbose, state_path, Set{UInt64}(), state_read(state_path, STATE_LAYOUT),
               ReentrantLock(), ReentrantLock(), jobs,
               IdDict{Step, Vector{AbstractStepResult}}(),
               IdDict{Step, Channel{Vector{AbstractStepResult}}}(),
               ResourceBudget(memory_budget_mb),
               ResourceBudget(thread_budget))

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
3. **No file dependencies**: Fresh if step was previously completed (state-based tracking).

State is persisted in `.pipeline_state`. Completions during a run are batched and
written once when `run()` finishes.

See also: [`Force`](@ref), [`clear_state!`](@ref)
"""
is_fresh(step::Step) = is_fresh(step, RunContext())

function is_fresh(step::Step, ctx::RunContext)
    has_in, has_out = !isempty(step.inputs), !isempty(step.outputs)
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
