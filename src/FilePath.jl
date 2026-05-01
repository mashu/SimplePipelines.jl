# Explicit "value lives on disk" wrapper. A heavy step (e.g. one producing a
# multi-GB DataFrame) writes to disk and returns `FilePath("/tmp/x.tsv")`; the
# downstream consumer either materializes it (custom `materialize` method) or
# forwards the path. Keeps the in-memory result tiny instead of carrying the
# whole DataFrame across the pipeline.

"""
    FilePath(path::String)

Wrapper marking that a step's value lives on disk at `path`. Steps that produce
large data should write to a temp file and return `FilePath(...)` so the runtime
holds only a path between steps. Use `materialize(fp)` to load the value at
the consumer.
"""
struct FilePath
    path::String
end

Base.string(fp::FilePath) = fp.path
Base.show(io::IO, fp::FilePath) = print(io, "FilePath(", repr(fp.path), ")")

"""
    materialize(value)

Identity for plain values; specialized for `FilePath` to load from disk via
`read(::String)`. Users can extend `materialize(::FilePath)` for typed loaders
(e.g. CSV → DataFrame).
"""
materialize(x) = x
materialize(fp::FilePath) = read(fp.path)
