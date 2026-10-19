# Freshness. A step with declared outputs is up to date when every output exists,
# no input is newer than the oldest output, and the outputs were last made by the
# same recipe (command template and inputs). Recipes are remembered in a small text
# file, `.pipeline_state`, mapping each output path to the recipe fingerprint. When
# no record exists (first run, deleted state file) timestamps alone decide.

@nospecialize   # see runtime.jl

const STATE_FILE = ".pipeline_state"
const STATE_HEADER = "# SimplePipelines state v2"

"""Recipe records of one state file plus the changes made during the current run."""
struct State
    path::String
    records::Dict{String,UInt64}
    changes::Dict{String,Union{UInt64,Nothing}}
end
State(path::AbstractString) = State(String(path), load_records(path), Dict{String,Union{UInt64,Nothing}}())

function load_records(path::AbstractString)
    records = Dict{String,UInt64}()
    isfile(path) || return records
    open(path) do io
        String(read(io, ncodeunits(STATE_HEADER))) == STATE_HEADER || return records
        readline(io)
        for line in eachline(io)
            tab = findfirst('\t', line)
            tab === nothing && continue
            recipe = tryparse(UInt64, SubString(line, 1, tab - 1); base=16)
            recipe === nothing || (records[line[nextind(line, tab):end]] = recipe)
        end
        records
    end
end

"""The changes recorded since the last save; the state forgets them (call under the run's lock)."""
take_changes!(st::State) = (changes = copy(st.changes); empty!(st.changes); changes)

"""Write `changes`, merged with whatever is on disk now, atomically."""
function save_state(path::String, changes::Dict{String,Union{UInt64,Nothing}})
    isempty(changes) && return nothing
    merged = load_records(path)
    for (output, recipe) in changes
        apply_change!(merged, output, recipe)
    end
    dir = dirname(path)
    isempty(dir) || mkpath(dir)
    tmp = string(path, ".", getpid(), ".tmp")
    open(tmp, "w") do io
        println(io, STATE_HEADER)
        for (output, recipe) in sort!(collect(merged); by=first)
            occursin('\n', output) || println(io, string(recipe; base=16, pad=16), '\t', output)
        end
    end
    mv(tmp, path; force=true)
    nothing
end
apply_change!(records, path, recipe::UInt64) = (records[path] = recipe)
apply_change!(records, path, ::Nothing) = delete!(records, path)

function remember!(st::State, step::Step)
    for o in step.outputs
        st.records[o] = step.recipe
        st.changes[o] = step.recipe
    end
end

function forget!(st::State, step::Step)
    for o in step.outputs
        delete!(st.records, o)
        st.changes[o] = nothing
    end
end

"""Adopt outputs that are up to date but have no record yet (made outside this tool)."""
function adopt!(st::State, step::Step)
    any(o -> !haskey(st.records, o), step.outputs) && remember!(st, step)
    nothing
end

"""
What the run knows about a step's paths, read under the run's lock: the recipe recorded
for each output (the step's own when there is no record) and which inputs an earlier
planned step will rebuild. The filesystem is checked afterwards, outside the lock.
"""
struct Recorded
    recipes::Vector{UInt64}
    pending::Vector{Bool}
end
recorded(st::State, pending::Set{String}, step::Step) =
    Recorded([get(st.records, o, step.recipe) for o in step.outputs], [i in pending for i in step.inputs])

"""Why `step` has to run, or `nothing` when it is up to date. Reads the filesystem (may throw)."""
function staleness(step::Step, rec::Recorded, forced::Bool)
    forced && return "forced"
    isempty(step.outputs) && return "no declared outputs"
    for o in step.outputs
        ispath(o) || return "missing output $o"
    end
    oldest = minimum(mtime, step.outputs)
    for (i, rebuilt) in zip(step.inputs, rec.pending)
        rebuilt && return "input $i will be rebuilt"
        ispath(i) || return "missing input $i"
        mtime(i) > oldest && return "input $i is newer than the outputs"
    end
    all(==(step.recipe), rec.recipes) || return "command or inputs changed since the last run"
    nothing
end

"""
    is_fresh(step::Step; state_path=".pipeline_state") -> Bool

Whether `step` would be skipped as up to date. Only steps with declared outputs can
be up to date; see [`Step`](@ref) for the rules.
"""
is_fresh(step::Step; state_path::AbstractString=STATE_FILE) =
    staleness(step, recorded(State(state_path), Set{String}(), step), false) === nothing

"""
    clear_state!(state_path=".pipeline_state")

Forget the recorded recipes. Outputs are then judged by timestamps only until steps
run again; use `run(...; force=true)` to rebuild everything.
"""
clear_state!(state_path::AbstractString=STATE_FILE) = (rm(state_path; force=true); nothing)

@specialize
