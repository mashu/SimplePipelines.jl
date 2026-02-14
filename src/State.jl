# State persistence (Make-like freshness), concurrency and memory limits. Requires Types.jl, Macro.jl (for Step).

const STATE_FILE = Ref(".pipeline_state")
const state_loaded = Ref{Set{UInt64}}(Set{UInt64}())
const pending_completions = Ref{Set{UInt64}}(Set{UInt64}())
const run_depth = Ref{Int}(0)

step_hash(step::Step) = hash((step.name, step.work, step.inputs, step.outputs))

"""Read persisted step hashes from the state file."""
load_state()::Set{UInt64} = state_read(STATE_FILE[], STATE_LAYOUT)

current_state()::Set{UInt64} = run_depth[] >= 1 ? union(state_loaded[], pending_completions[]) : load_state()

function save_state!(completed::Set{UInt64})
    state_write(STATE_FILE[], completed, STATE_LAYOUT)
end

function mark_complete!(step::Step)
    h = step_hash(step)
    if run_depth[] >= 1
        push!(pending_completions[], h)
    else
        state_append(STATE_FILE[], h, STATE_LAYOUT) || save_state!(union(state_read(STATE_FILE[], STATE_LAYOUT), Set{UInt64}([h])))
    end
end

"""
    clear_state!()

Remove the pipeline state file (`.pipeline_state`), forcing all steps to run on the next execution regardless of freshness. The file uses the fixed binary layout defined in the `StateFormat` module.

See also: [`is_fresh`](@ref), [`Force`](@ref)
"""
clear_state!() = isfile(STATE_FILE[]) && rm(STATE_FILE[])

"""
    is_fresh(step::Step) -> Bool

Check if a step can be skipped based on Make-like freshness rules.

# Freshness Rules
1. **Has inputs and outputs**: Fresh if all outputs exist and are newer than all inputs
2. **Has only outputs**: Fresh if outputs exist and step was previously completed
3. **No file dependencies**: Fresh if step was previously completed (state-based tracking)

State is persisted in `.pipeline_state`. Completions during a run are batched and written once when `run()` finishes.

See also: [`Force`](@ref), [`clear_state!`](@ref)
"""
function is_fresh(step::Step)
    has_in, has_out = !isempty(step.inputs), !isempty(step.outputs)
    if has_out
        all(isfile, step.outputs) || return false
    end
    if has_in && has_out
        oldest_out = minimum(mtime, step.outputs)
        for inp in step.inputs
            isfile(inp) || return false
            mtime(inp) >= oldest_out && return false
        end
        return true
    end
    step_hash(step) in current_state()
end

const MAX_PARALLEL = Ref{Int}(8)
const MEMORY_LIMIT = Ref{UInt64}(0)

function wait_if_over_memory_limit()
    limit = MEMORY_LIMIT[]
    limit == 0 && return
    while Base.gc_live_bytes() > limit
        GC.gc()
        sleep(0.5)
    end
end
