"""
    StateFormat

Fixed binary layout for the pipeline state file. Single source of truth for
offsets, sizes, and all I/O: memory-mapped file with random access to the hash array.

Layout (on disk):
  [0..8)   magic (8 bytes)
  [8..16)  count (UInt64, number of valid hashes)
  [16..)   hashes (max_hashes × sizeof(UInt64), mmap-able for random access)

All read/write/mmap of the state file goes through this module (state_init, state_read, state_write, state_append).
"""
module StateFormat

using Mmap

export StateFileLayout, STATE_LAYOUT
export layout_file_size, layout_read_header, layout_write_header, layout_validate_count
export state_init, state_read, state_write, state_append

"""
    StateFileLayout

Descriptor for the fixed binary layout of the state file. Holds magic bytes,
header length, offsets for count and hashes, and max capacity.
"""
struct StateFileLayout
    magic::Vector{UInt8}
    header_len::Int
    count_offset::Int
    hashes_offset::Int
    sizeof_hash::Int
    max_hashes::Int
end

const STATE_LAYOUT = StateFileLayout(
    b"SPstate\0",
    16,
    8,
    16,
    sizeof(UInt64),
    1_000_000,
)

layout_file_size(layout::StateFileLayout)::Int =
    layout.header_len + layout.max_hashes * layout.sizeof_hash

function layout_read_header(io, layout::StateFileLayout)::Tuple{Bool, UInt64}
    header = read(io, layout.header_len)
    magic_ok = length(header) >= layout.count_offset && header[1:length(layout.magic)] == layout.magic
    count = magic_ok && length(header) >= layout.header_len ?
        reinterpret(UInt64, header[layout.count_offset .+ (1:layout.sizeof_hash)])[1] : UInt64(0)
    (magic_ok, count)
end

function layout_write_header(io, layout::StateFileLayout, count::UInt64)
    write(io, layout.magic)
    write(io, count)
end

function layout_validate_count(layout::StateFileLayout, n::UInt64)::Bool
    n <= layout.max_hashes
end

#------------------------------------------------------------------------------
# State file I/O (memory-mapped, random access)
#------------------------------------------------------------------------------

"""Ensure the state file exists and has the correct size; create or truncate if needed."""
function state_init(path::AbstractString, layout::StateFileLayout=STATE_LAYOUT)
    if !isfile(path) || filesize(path) < layout_file_size(layout)
        open(path, "w") do io
            layout_write_header(io, layout, UInt64(0))
            truncate(io, layout_file_size(layout))
        end
    end
end

"""Read completed step hashes from the state file (memory-mapped, random access). Returns Set{UInt64}."""
function state_read(path::AbstractString, layout::StateFileLayout=STATE_LAYOUT)::Set{UInt64}
    isfile(path) || return Set{UInt64}()
    open(path, "r") do io
        filesize(io) < layout.header_len && return Set{UInt64}()
        magic_ok, n = layout_read_header(io, layout)
        (!magic_ok || !layout_validate_count(layout, n) || n == 0) && return Set{UInt64}()
        arr = Mmap.mmap(io, Vector{UInt64}, (layout.max_hashes,); grow=false)
        Set{UInt64}(@view arr[1:n])
    end
end

"""Write all completed hashes to the state file (capped at layout.max_hashes)."""
function state_write(path::AbstractString, completed::Set{UInt64}, layout::StateFileLayout=STATE_LAYOUT)
    state_init(path, layout)
    hashes = collect(Iterators.take(completed, layout.max_hashes))
    n = length(hashes)
    open(path, "r+") do io
        layout_write_header(io, layout, UInt64(n))
        arr = Mmap.mmap(io, Vector{UInt64}, (layout.max_hashes,); grow=false)
        for i in 1:n
            arr[i] = hashes[i]
        end
        Mmap.sync!(arr)
    end
end

"""Append one hash; returns false if at capacity (caller should state_write(union(state_read(), [h])))."""
function state_append(path::AbstractString, h::UInt64, layout::StateFileLayout=STATE_LAYOUT)::Bool
    state_init(path, layout)
    count = open(path, "r") do io
        magic_ok, n = layout_read_header(io, layout)
        magic_ok ? n : UInt64(layout.max_hashes)
    end
    count >= layout.max_hashes && return false
    open(path, "r+") do io
        seek(io, layout.hashes_offset)
        arr = Mmap.mmap(io, Vector{UInt64}, (layout.max_hashes,); grow=false)
        arr[count + 1] = h
        Mmap.sync!(arr)
        seek(io, layout.count_offset)
        write(io, count + 1)
    end
    true
end

end # module
