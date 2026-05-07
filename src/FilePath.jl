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
    materialize(value)

Identity for plain values. For [`FilePath`](@ref), reads raw bytes from disk
(extend for typed loaders). For [`SpilledValue`](@ref), deserialises the value
back into the original Julia object.
"""
materialize(x) = x
materialize(fp::FilePath) = read(fp.path)
materialize(s::SpilledValue) = open(deserialize, s.path)

# Internal: serialise `value` to a tempfile in `dir` and return a SpilledValue.
function spill_to_disk(value, dir::AbstractString)
    isdir(dir) || mkpath(dir)
    path = joinpath(dir, "splpl_" * randstring_lower(12) * ".jls")
    open(io -> serialize(io, value), path, "w")
    SpilledValue(path)
end

# Tiny lowercase-alphanumeric token for spill filenames; avoids pulling in
# UUIDs.jl just for a unique-enough name.
function randstring_lower(n::Int)
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    String([rand(chars) for _ in 1:n])
end
