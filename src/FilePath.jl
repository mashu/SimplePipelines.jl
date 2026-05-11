# Explicit "value lives on disk" wrappers.
#
# `FilePath` is for *user-managed* output files: a step writes its result to a
# known location and returns `FilePath(path)`; downstream `materialize` reads
# raw bytes (the user typically extends it to deserialise into a typed object,
# e.g. `materialize(::FilePath) = CSV.read(...)`).
#
# `SpilledValue` is for *runtime-managed* tempfiles produced by the auto-spill
# pass: when a step's in-memory result exceeds `spill_threshold_bytes`, the
# runtime serialises it via `Base.Serialization` and replaces `r.result` with a
# `SpilledValue(path)`. `materialize` deserialises it back into the original
# Julia object. This keeps the per-run memo's RAM footprint bounded by the
# threshold × `jobs`, regardless of how many step results accumulate.

using Serialization: serialize, deserialize

"""
    FilePath(path::String)

Wrapper marking that a step's value lives on a *user-named* file at `path`.
Steps that write their output to a known path should return `FilePath(...)`
so the runtime holds only a path between steps. The default `materialize`
reads raw bytes; users can specialise for typed loading (CSV → DataFrame, etc.).
"""
struct FilePath
    path::String
end

Base.string(fp::FilePath) = fp.path
Base.show(io::IO, fp::FilePath) = print(io, "FilePath(", repr(fp.path), ")")

"""
    SpilledValue(path::String)

Wrapper marking that a step's value was *auto-spilled* to a runtime-managed
tempfile at `path`. Created by the runtime when `auto_spill` is on and a
step's result exceeds `spill_threshold_bytes`. `materialize(::SpilledValue)`
deserialises the value via `Base.Serialization`.

Distinct from [`FilePath`](@ref): `FilePath` is for user-named output files
(arbitrary contents, raw-bytes default loader), `SpilledValue` is for
serialised Julia objects the runtime owns.
"""
struct SpilledValue
    path::String
end

Base.string(s::SpilledValue) = s.path
Base.show(io::IO, s::SpilledValue) = print(io, "SpilledValue(", repr(s.path), ")")

"""
    SpilledStdout(path::String)

Wrapper for a shell step's captured stdout that was *streamed directly to disk*
rather than buffered in RAM. The runtime emits this when an `auto_spill`-enabled
shell step's output file exceeds `spill_threshold_bytes`, so peak memory is
independent of the command's output size. `materialize(::SpilledStdout)` reads
the file back as a `String`.

Distinct from [`SpilledValue`](@ref): `SpilledValue` stores a `Base.Serialization`
blob of an arbitrary Julia object; `SpilledStdout` is raw bytes from a captured
process — never serialised, never deserialised.
"""
struct SpilledStdout
    path::String
end

Base.string(s::SpilledStdout) = s.path
Base.show(io::IO, s::SpilledStdout) = print(io, "SpilledStdout(", repr(s.path), ")")

"""
    materialize(value)

Identity for plain values. For [`FilePath`](@ref), reads raw bytes from disk
(extend for typed loaders). For [`SpilledValue`](@ref), deserialises the value
back into the original Julia object. For [`SpilledStdout`](@ref), reads the
streamed-to-disk shell stdout back as a `String`.
"""
materialize(x) = x
materialize(fp::FilePath) = read(fp.path)
materialize(s::SpilledValue) = open(deserialize, s.path)
function materialize(s::SpilledStdout)
    str = read(s.path, String)
    isfile(s.path) && rm(s.path; force=true)
    str
end

"""
    materialize_table(fp::FilePath; kwargs...)

Read a CSV file from disk as a `DataFrame` (via CSV.jl). Load `CSV` and `DataFrames`
after SimplePipelines so the package extension is active; otherwise this function
has no methods and you get `MethodError`.

Keyword arguments are passed through to [`CSV.read`](https://csv.juliadata.org/stable/).
"""
function materialize_table end

# Serialise `value` to a tempfile in `dir` and return a SpilledValue.
function spill_to_disk(value, dir::AbstractString)
    isdir(dir) || mkpath(dir)
    path = joinpath(dir, "splpl_" * randstring_lower(12) * ".jls")
    open(io -> serialize(io, value), path, "w")
    SpilledValue(path)
end

function reserve_stdout_path(dir::AbstractString)
    isdir(dir) || mkpath(dir)
    joinpath(dir, "splpl_out_" * randstring_lower(12))
end

# Tiny lowercase-alphanumeric token for spill filenames; avoids pulling in
# UUIDs.jl just for a unique-enough name.
function randstring_lower(n::Int)
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    String([rand(chars) for _ in 1:n])
end
