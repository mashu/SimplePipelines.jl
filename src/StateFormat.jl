"""
    StateFormat

Fixed binary layout for the pipeline state file. Single source of truth for
offsets and sizes; all read/write/mmap of the state file goes through this module.

Layout (on disk):
  [0..8)   magic (8 bytes)
  [8..16)  count (UInt64, number of valid hashes)
  [16..)   hashes (max_hashes × sizeof(UInt64), mmap-able for random access)
"""
module StateFormat

export StateFileLayout, STATE_LAYOUT
export layout_file_size, layout_read_header, layout_write_header, layout_validate_count

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

end # module
