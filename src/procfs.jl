# Exception-free readers for /proc and cgroup files, used by the guard process and by the driver
# (`memory_snapshot`, `effective_cpus`, `kill_sessions!`). Files are read with libc open/read/close
# and parsed by hand, so a vanished process or a missing file is a return value, never an exception.
# The readers are Linux-only; the three driver probes fall back to Base elsewhere.

const PROCFS_READ_FLAGS = Base.Filesystem.JL_O_RDONLY | Base.Filesystem.JL_O_CLOEXEC
const PROCFS_WRITE_FLAGS = Base.Filesystem.JL_O_WRONLY | Base.Filesystem.JL_O_CLOEXEC
const BYTES_PER_MIB = 1 << 20
const CGROUP_UNLIMITED = typemax(Int)      # bytes; also the value of a level never read
const CGROUP_LAYOUT_INTERVAL = 10.0        # seconds between re-reads of cgroup paths and limits
const SIGNAL_INT = 2
const SIGNAL_KILL = 9
const SIGNAL_TERM = 15
const ERRNO_ESRCH = 3

monotonic_seconds() = time_ns() / 1.0e9

# ---------------------------------------------------------------------------------------------
# Raw file access

# A NUL byte would make the Cstring conversion throw.
valid_path(path::String) = !occursin('\0', path)

"""Read all of `path` into `buf`, growing it as needed. Returns the number of bytes read, or -1
when the file cannot be opened or read."""
function read_file!(buf::Vector{UInt8}, path::String)
    valid_path(path) || return -1
    fd = ccall(:open, Cint, (Cstring, Cint, Cuint...), path, PROCFS_READ_FLAGS, 0)
    fd < 0 && return -1
    n = 0
    while true
        n == length(buf) && resize!(buf, max(4096, 2 * length(buf)))
        got = GC.@preserve buf ccall(:read, Cssize_t, (Cint, Ptr{UInt8}, Csize_t),
                                     fd, pointer(buf, n + 1), length(buf) - n)
        if got <= 0
            got < 0 && (n = -1)
            break
        end
        n += got
    end
    ccall(:close, Cint, (Cint,), fd)
    n
end

"""The contents of `path` as a String, or "" when it cannot be read."""
function read_text(buf::Vector{UInt8}, path::String)
    n = read_file!(buf, path)
    n > 0 ? String(buf[1:n]) : ""
end

"""An entry as libuv's `uv_fs_scandir_next` returns it (`uv_dirent_t`)."""
struct DirEntry
    name::Ptr{UInt8}
    kind::Cint
end

"""Replace `names` with the entries of directory `path`. False when it cannot be listed (for example
at the file descriptor limit), where `readdir` would throw."""
function list_directory!(names::Vector{String}, path::String)
    empty!(names)
    valid_path(path) || return false
    req = Libc.malloc(ccall(:uv_req_size, Csize_t, (Cint,), Base.UV_FS))
    req == C_NULL && return false
    n = ccall(:uv_fs_scandir, Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Cstring, Cint, Ptr{Cvoid}), C_NULL, req, path, 0, C_NULL)
    if n >= 0
        entry = Ref(DirEntry(C_NULL, 0))
        while ccall(:uv_fs_scandir_next, Cint, (Ptr{Cvoid}, Ptr{DirEntry}), req, entry) == 0
            push!(names, unsafe_string(entry[].name))
        end
    end
    ccall(:uv_fs_req_cleanup, Cvoid, (Ptr{Cvoid},), req)
    Libc.free(req)
    n >= 0
end

"""Write `text` to an existing file such as /proc/<pid>/oom_score_adj; false when that fails."""
function write_file(path::String, text::String)
    valid_path(path) || return false
    fd = ccall(:open, Cint, (Cstring, Cint, Cuint...), path, PROCFS_WRITE_FLAGS, 0)
    fd < 0 && return false
    n = ccall(:write, Cssize_t, (Cint, Ptr{UInt8}, Csize_t), fd, text, sizeof(text))
    ccall(:close, Cint, (Cint,), fd)
    n == sizeof(text)
end

# ---------------------------------------------------------------------------------------------
# Hand-written parsing over bytes: no `parse`, nothing throws on garbage

isdigit_byte(c::UInt8) = UInt8('0') <= c <= UInt8('9')
isblank_byte(c::UInt8) = c == UInt8(' ') || c == UInt8('\t')
isletter_byte(c::UInt8) = UInt8('A') <= c <= UInt8('Z') || UInt8('a') <= c <= UInt8('z')

function skip_blanks(buf::AbstractVector{UInt8}, i::Int, n::Int)
    while i <= n && isblank_byte(buf[i])
        i += 1
    end
    i
end

function skip_field(buf::AbstractVector{UInt8}, i::Int, n::Int)
    i = skip_blanks(buf, i, n)
    while i <= n && !isblank_byte(buf[i])
        i += 1
    end
    i
end

"""Parse the decimal integer at `i` in `buf[1:n]`, after blanks and an optional minus sign.
Returns `(value, next, found)`; `found` is false when there are no digits. Overflow saturates."""
function parse_int(buf::AbstractVector{UInt8}, i::Int, n::Int)
    i = skip_blanks(buf, i, n)
    negative = i <= n && buf[i] == UInt8('-')
    negative && (i += 1)
    start = i
    v = 0
    while i <= n && isdigit_byte(buf[i])
        d = Int(buf[i] - UInt8('0'))
        v = v > (typemax(Int) - d) ÷ 10 ? typemax(Int) : 10v + d
        i += 1
    end
    (negative ? -v : v, i, i > start)
end

"""The integer at the start of `s`, or 0 when `s` does not start with one."""
parse_int(s::AbstractString) = parse_int(codeunits(s), 1, ncodeunits(s))[1]

"""Parse a non-negative decimal such as `25.31` at `i`; -1.0 when there is no number."""
function parse_decimal(buf::AbstractVector{UInt8}, i::Int, n::Int)
    whole, i, found = parse_int(buf, i, n)
    found || return -1.0
    fraction, scale = 0.0, 0.1
    if i <= n && buf[i] == UInt8('.')
        i += 1
        while i <= n && isdigit_byte(buf[i])
            fraction += scale * Int(buf[i] - UInt8('0'))
            scale /= 10
            i += 1
        end
    end
    whole + fraction
end

function starts_with_at(buf::AbstractVector{UInt8}, i::Int, n::Int, key::String)
    k = ncodeunits(key)
    i + k - 1 <= n || return false
    for j in 1:k
        buf[i+j-1] == codeunit(key, j) || return false
    end
    true
end

"""Index just after `key` where it starts a line of `buf[1:n]`, or 0 when no line starts with it."""
function find_line(buf::AbstractVector{UInt8}, n::Int, key::String)
    i = 1
    while i <= n
        starts_with_at(buf, i, n, key) && return i + ncodeunits(key)
        while i <= n && buf[i] != UInt8('\n')
            i += 1
        end
        i += 1
    end
    0
end

"""The number after `key` on the line that starts with it, or -1 when there is none."""
function key_value(buf::AbstractVector{UInt8}, n::Int, key::String)
    i = find_line(buf, n, key)
    i == 0 && return -1
    v, _, found = parse_int(buf, i, n)
    found ? v : -1
end

function last_index(buf::AbstractVector{UInt8}, n::Int, c::UInt8)
    for i in n:-1:1
        buf[i] == c && return i
    end
    0
end

pid32(v::Int) = 0 <= v <= typemax(Int32) ? v % Int32 : Int32(0)

# ---------------------------------------------------------------------------------------------
# Processes

"""The fields of /proc/<pid>/stat the guard uses. `state == 0` marks an unreadable or garbled file."""
struct ProcInfo
    pid::Int32
    state::UInt8
    ppid::Int32
    pgrp::Int32
    sid::Int32
    starttime::Int        # field 22, in clock ticks since boot: tells a reused pid apart
end

const NO_PROC = ProcInfo(0, 0x00, 0, 0, 0, 0)

"""Parse /proc/<pid>/stat. `comm` (field 2) may contain spaces and parentheses, so the fields
after it are counted from the last ')'."""
function parse_stat(buf::AbstractVector{UInt8}, n::Int)
    paren = last_index(buf, n, UInt8(')'))
    paren == 0 && return NO_PROC
    pid, _, has_pid = parse_int(buf, 1, n)
    i = skip_blanks(buf, paren + 1, n)
    has_pid && i <= n && isletter_byte(buf[i]) || return NO_PROC
    state = buf[i]
    ppid, i, ok4 = parse_int(buf, i + 1, n)       # field 4
    pgrp, i, ok5 = parse_int(buf, i, n)           # field 5
    sid, i, ok6 = parse_int(buf, i, n)            # field 6
    for _ in 7:21
        i = skip_field(buf, i, n)
    end
    starttime, _, ok22 = parse_int(buf, i, n)     # field 22
    ok4 && ok5 && ok6 && ok22 || return NO_PROC
    ProcInfo(pid32(pid), state, pid32(ppid), pid32(pgrp), pid32(sid), starttime)
end
parse_stat(s::AbstractString) = parse_stat(codeunits(s), ncodeunits(s))

process_alive(p::ProcInfo) =
    p.pid > 0 && p.state != 0x00 && p.state != UInt8('Z') && p.state != UInt8('X') && p.state != UInt8('x')

all_digits(name::String) = !isempty(name) && all(isdigit_byte, codeunits(name))

"""Fill `procs` with every live (not zombie or dead) process found in /proc (`names` is scratch
space). False when /proc cannot be listed: `procs` is then empty, which must not be read as "no
process is running"."""
function scan_processes!(procs::Vector{ProcInfo}, buf::Vector{UInt8}, names::Vector{String})
    empty!(procs)
    list_directory!(names, "/proc") || return false
    for name in names
        all_digits(name) || continue
        n = read_file!(buf, string("/proc/", name, "/stat"))
        n > 0 || continue
        p = parse_stat(buf, n)
        process_alive(p) && push!(procs, p)
    end
    true
end

"""Non-reclaimable memory from /proc/<pid>/status, in kB: RssAnon + RssShmem + VmSwap.
File-backed pages (RssFile) are left out because the kernel can drop them."""
status_usage_kb(buf::AbstractVector{UInt8}, n::Int) =
    max(0, key_value(buf, n, "RssAnon:")) + max(0, key_value(buf, n, "RssShmem:")) +
    max(0, key_value(buf, n, "VmSwap:"))
status_usage_kb(s::AbstractString) = status_usage_kb(codeunits(s), ncodeunits(s))

function process_usage_kb(buf::Vector{UInt8}, pid::Integer)
    n = read_file!(buf, string("/proc/", pid, "/status"))
    n > 0 ? status_usage_kb(buf, n) : 0
end

"""Proportional non-reclaimable memory from /proc/<pid>/smaps_rollup, in kB:
Pss_Anon + Pss_Shmem + SwapPss, or Pss + SwapPss on kernels without the breakdown; -1 if neither."""
function rollup_pss_kb(buf::AbstractVector{UInt8}, n::Int)
    swap = max(0, key_value(buf, n, "SwapPss:"))
    anon = key_value(buf, n, "Pss_Anon:")
    anon >= 0 && return anon + max(0, key_value(buf, n, "Pss_Shmem:")) + swap
    pss = key_value(buf, n, "Pss:")
    pss >= 0 ? pss + swap : -1
end
rollup_pss_kb(s::AbstractString) = rollup_pss_kb(codeunits(s), ncodeunits(s))

"""PSS of a process in kB, falling back to the status sum when smaps_rollup cannot be read."""
function process_pss_kb(buf::Vector{UInt8}, pid::Integer)
    n = read_file!(buf, string("/proc/", pid, "/smaps_rollup"))
    kb = n > 0 ? rollup_pss_kb(buf, n) : -1
    kb >= 0 ? kb : process_usage_kb(buf, pid)
end

"""The command name of a process with blanks and control bytes replaced, safe inside one protocol
line; "?" when it cannot be read."""
function process_comm(buf::Vector{UInt8}, pid::Integer)
    n = read_file!(buf, string("/proc/", pid, "/comm"))
    n > 0 || return "?"
    bytes = buf[1:n]
    bytes[end] == UInt8('\n') && pop!(bytes)
    isempty(bytes) && return "?"
    map!(c -> c <= 0x20 || c == 0x7f ? UInt8('_') : c, bytes, bytes)
    String(bytes)
end

# ---------------------------------------------------------------------------------------------
# Host memory and pressure

"""(MemAvailable, MemTotal) in MiB from /proc/meminfo text; -1 for a missing field."""
function meminfo_mb(buf::AbstractVector{UInt8}, n::Int)
    available = key_value(buf, n, "MemAvailable:")
    total = key_value(buf, n, "MemTotal:")
    (available < 0 ? -1 : available ÷ 1024, total < 0 ? -1 : total ÷ 1024)
end
meminfo_mb(s::AbstractString) = meminfo_mb(codeunits(s), ncodeunits(s))

"""`full avg10` from a PSI file (/proc/pressure/memory or a cgroup's memory.pressure); -1.0 if absent."""
function psi_full_avg10(buf::AbstractVector{UInt8}, n::Int)
    i = find_line(buf, n, "full avg10=")
    i == 0 ? -1.0 : parse_decimal(buf, i, n)
end
psi_full_avg10(s::AbstractString) = psi_full_avg10(codeunits(s), ncodeunits(s))

# ---------------------------------------------------------------------------------------------
# Cgroups: discovery from /proc/self/cgroup and /proc/self/mountinfo, then a walk from this
# process's cgroup up to the mount point. Every function takes the texts and a filesystem root
# so tests can run it on fake trees.

abstract type CgroupVersion end
struct CgroupV1 <: CgroupVersion end
struct CgroupV2 <: CgroupVersion end
struct NoCgroup <: CgroupVersion end

"""The cgroup directories of one controller for this process: leaf first, mount point last."""
struct CgroupTree
    version::CgroupVersion
    dirs::Vector{String}
end

const NO_CGROUP_TREE = CgroupTree(NoCgroup(), String[])

struct CgroupMount
    root::String          # the subtree of the hierarchy mounted here
    point::String
    fstype::String
    options::String       # super options; v1 lists the controllers here
end

"""Undo mountinfo's octal escapes (`\\040` for a space)."""
function unescape_mount(s::AbstractString)
    bytes = codeunits(s)
    out = UInt8[]
    i = 1
    while i <= length(bytes)
        if bytes[i] == UInt8('\\') && i + 3 <= length(bytes) && all(c -> UInt8('0') <= c <= UInt8('7'), bytes[i+1:i+3])
            push!(out, ((Int(bytes[i+1]) - 48) * 64 + (Int(bytes[i+2]) - 48) * 8 + Int(bytes[i+3]) - 48) % UInt8)
            i += 4
        else
            push!(out, bytes[i])
            i += 1
        end
    end
    String(out)
end

"""The cgroup and cgroup2 mounts listed in mountinfo text."""
function cgroup_mounts(mountinfo::AbstractString)
    mounts = CgroupMount[]
    for line in split(mountinfo, '\n')
        fields = split(line, ' ')
        sep = something(findfirst(==("-"), fields), 0)
        sep > 5 && length(fields) >= sep + 3 || continue
        fstype = fields[sep+1]
        fstype == "cgroup" || fstype == "cgroup2" || continue
        push!(mounts, CgroupMount(unescape_mount(fields[4]), unescape_mount(fields[5]), fstype, fields[sep+3]))
    end
    mounts
end

"""(v1 path of `controller`, v2 path) from /proc/self/cgroup text; "" for each one absent."""
function cgroup_paths(cgroup::AbstractString, controller::String)
    v1, v2 = "", ""
    for line in split(cgroup, '\n')
        parts = split(line, ':'; limit=3)
        length(parts) == 3 || continue
        if parts[1] == "0" && isempty(parts[2])
            v2 = String(parts[3])
        elseif controller in split(parts[2], ',')
            v1 = String(parts[3])
        end
    end
    (v1, v2)
end

"""Whether `root` (a mount's root field) contains the cgroup `path`."""
path_under(root::AbstractString, path::AbstractString) =
    root == "/" || path == root || startswith(path, root * "/")

"""`path` relative to the mounted subtree `root`; "" when it is the subtree itself or outside it
(a container bind-mounts its own cgroup as the mount point)."""
function relative_cgroup(root::AbstractString, path::AbstractString)
    path_under(root, path) || return ""
    rel = root == "/" ? path : path[ncodeunits(root)+1:end]
    rel == "/" ? "" : String(rel)
end

function pick_mount(mounts::Vector{CgroupMount}, fstype::String, controller::String, path::String)
    fallback = 0
    for (i, m) in pairs(mounts)
        m.fstype == fstype || continue
        isempty(controller) || controller in split(m.options, ',') || continue
        path_under(m.root, path) && return i
        fallback == 0 && (fallback = i)
    end
    fallback
end

function walk_dirs(fsroot::AbstractString, m::CgroupMount, path::String)
    top = fsroot * m.point
    d = top * relative_cgroup(m.root, path)
    dirs = [d]
    while d != top && ncodeunits(d) > ncodeunits(top) && dirname(d) != d
        d = dirname(d)
        push!(dirs, d)
    end
    dirs
end

"""The directories of `controller` ("memory" or "cpu") for the process described by the texts of
/proc/self/cgroup and /proc/self/mountinfo, with `fsroot` prepended to every mount point ("" on
a real system). A v1 hierarchy carrying the controller wins over the v2 one, which is what a
hybrid system uses."""
function cgroup_tree(cgroup::AbstractString, mountinfo::AbstractString, fsroot::AbstractString, controller::String)
    v1, v2 = cgroup_paths(cgroup, controller)
    mounts = cgroup_mounts(mountinfo)
    if !isempty(v1)
        i = pick_mount(mounts, "cgroup", controller, v1)
        i > 0 && return CgroupTree(CgroupV1(), walk_dirs(fsroot, mounts[i], v1))
    end
    if !isempty(v2)
        i = pick_mount(mounts, "cgroup2", "", v2)
        i > 0 && return CgroupTree(CgroupV2(), walk_dirs(fsroot, mounts[i], v2))
    end
    NO_CGROUP_TREE
end

self_cgroup_tree(buf::Vector{UInt8}, controller::String) =
    cgroup_tree(read_text(buf, "/proc/self/cgroup"), read_text(buf, "/proc/self/mountinfo"), "", controller)

"""A single-number file in bytes: CGROUP_UNLIMITED for "max", -1 when unreadable or not a number."""
function bytes_file(buf::Vector{UInt8}, path::String)
    n = read_file!(buf, path)
    n > 0 || return -1
    starts_with_at(buf, 1, n, "max") && return CGROUP_UNLIMITED
    v, _, found = parse_int(buf, 1, n)
    found && v >= 0 ? v : -1
end

"""A value from a key/value file such as memory.stat, or -1."""
function stat_file(buf::Vector{UInt8}, path::String, key::String)
    n = read_file!(buf, path)
    n > 0 ? key_value(buf, n, key) : -1
end

# The memory limit of one level in bytes (-1 when unreadable); v2 counts memory.high too, since
# the kernel throttles and reclaims hard above it.
function level_limit(::CgroupV2, dir::String, buf::Vector{UInt8})
    hard = bytes_file(buf, dir * "/memory.max")
    hard < 0 && return -1
    high = bytes_file(buf, dir * "/memory.high")
    high < 0 ? hard : min(hard, high)
end
level_limit(::CgroupV1, dir::String, buf::Vector{UInt8}) = bytes_file(buf, dir * "/memory.limit_in_bytes")
level_limit(::NoCgroup, ::String, ::Vector{UInt8}) = -1

usage_file(::CgroupV2) = "/memory.current"
usage_file(::CgroupV1) = "/memory.usage_in_bytes"
inactive_key(::CgroupV2) = "inactive_file "
inactive_key(::CgroupV1) = "total_inactive_file "

"""One level of the memory cgroup walk, with the last values read successfully (bytes)."""
mutable struct CgroupLevel
    const dir::String
    limit::Int
    available::Int        # limit − working set, the kubelet rule
end

struct CgroupMemory
    version::CgroupVersion
    levels::Vector{CgroupLevel}
end

CgroupMemory(tree::CgroupTree) =
    CgroupMemory(tree.version, [CgroupLevel(d, CGROUP_UNLIMITED, CGROUP_UNLIMITED) for d in tree.dirs])

"""Re-read every level's limit; a level that cannot be read keeps its previous value."""
function read_limits!(cg::CgroupMemory, buf::Vector{UInt8})
    for level in cg.levels
        limit = level_limit(cg.version, level.dir, buf)
        limit >= 0 && (level.limit = limit)
    end
    cg
end

"""Re-read usage and inactive_file at every limited level: available = limit − (usage − inactive_file).
Page cache that can be dropped does not count against the limit. A failed read keeps the previous value."""
function read_usage!(cg::CgroupMemory, buf::Vector{UInt8})
    for level in cg.levels
        if level.limit == CGROUP_UNLIMITED
            level.available = CGROUP_UNLIMITED
            continue
        end
        usage = bytes_file(buf, level.dir * usage_file(cg.version))
        inactive = stat_file(buf, level.dir * "/memory.stat", inactive_key(cg.version))
        usage >= 0 && usage != CGROUP_UNLIMITED && inactive >= 0 || continue
        level.available = max(0, level.limit - max(0, usage - inactive))
    end
    cg
end

"""(available, limit) in MiB: the smallest over all levels (huge when nothing is limited)."""
function cgroup_memory_mb(cg::CgroupMemory)
    available = minimum((l.available for l in cg.levels); init=CGROUP_UNLIMITED)
    limit = minimum((l.limit for l in cg.levels); init=CGROUP_UNLIMITED)
    (available ÷ BYTES_PER_MIB, limit ÷ BYTES_PER_MIB)
end

# CPUs allowed by one level's quota, rounded up; 0 = no quota.
function level_cpus(::CgroupV2, dir::String, buf::Vector{UInt8})
    n = read_file!(buf, dir * "/cpu.max")
    n > 0 && !starts_with_at(buf, 1, n, "max") || return 0
    quota, i, ok = parse_int(buf, 1, n)
    period, _, ok2 = parse_int(buf, i, n)
    ok && ok2 && quota > 0 && period > 0 ? cld(quota, period) : 0
end
function level_cpus(::CgroupV1, dir::String, buf::Vector{UInt8})
    quota = int_file(buf, dir * "/cpu.cfs_quota_us")
    period = int_file(buf, dir * "/cpu.cfs_period_us")
    quota > 0 && period > 0 ? cld(quota, period) : 0
end
level_cpus(::NoCgroup, ::String, ::Vector{UInt8}) = 0

"""A single (possibly negative) integer file; -1 when unreadable."""
function int_file(buf::Vector{UInt8}, path::String)
    n = read_file!(buf, path)
    n > 0 || return -1
    v, _, found = parse_int(buf, 1, n)
    found ? v : -1
end

"""The smallest CPU quota on the path from the leaf to the mount point; 0 when there is none."""
function cgroup_cpus(tree::CgroupTree, buf::Vector{UInt8})
    cpus = 0
    for dir in tree.dirs
        c = level_cpus(tree.version, dir, buf)
        c > 0 && (cpus = cpus == 0 ? c : min(cpus, c))
    end
    cpus
end

# ---------------------------------------------------------------------------------------------
# The memory probe shared by the guard and `memory_snapshot`

"""What a memory source reports each tick: MiB, and PSI `full avg10` (-1.0 when unknown)."""
struct MemorySample
    available::Int
    total::Int
    psi::Float64
end

"""Linux: the host's MemAvailable/MemTotal bounded by every level of this process's memory cgroup.
Paths and limits are re-read every CGROUP_LAYOUT_INTERVAL; failed reads keep the previous values."""
mutable struct HostMemory
    const buf::Vector{UInt8}
    tree::CgroupTree
    cgroup::CgroupMemory
    available_mb::Int
    total_mb::Int
    meminfo_ok::Bool
    psi_path::String          # "" when PSI is not available
    psi::Float64
    layout_read_at::Float64
end

function HostMemory()
    m = HostMemory(Vector{UInt8}(undef, 8192), NO_CGROUP_TREE, CgroupMemory(NO_CGROUP_TREE),
                   Int(Sys.free_memory() ÷ BYTES_PER_MIB), Int(Sys.total_memory() ÷ BYTES_PER_MIB),
                   false, "", -1.0, -Inf)
    relayout!(m, monotonic_seconds())
    m.psi_path = find_psi_file(m)
    m
end

function relayout!(m::HostMemory, now::Float64)
    tree = self_cgroup_tree(m.buf, "memory")
    if tree.dirs != m.tree.dirs
        m.tree = tree
        m.cgroup = CgroupMemory(tree)
    end
    read_limits!(m.cgroup, m.buf)
    m.layout_read_at = now
    m
end

pressure_file(::CgroupV2, tree::CgroupTree) = first(tree.dirs) * "/memory.pressure"
pressure_file(::CgroupVersion, ::CgroupTree) = ""

function find_psi_file(m::HostMemory)
    for path in (pressure_file(m.tree.version, m.tree), "/proc/pressure/memory")
        isempty(path) && continue
        n = read_file!(m.buf, path)
        n > 0 && psi_full_avg10(m.buf, n) >= 0 && return path
    end
    ""
end

function sample_memory!(m::HostMemory, now::Float64)
    now - m.layout_read_at >= CGROUP_LAYOUT_INTERVAL && relayout!(m, now)
    n = read_file!(m.buf, "/proc/meminfo")
    if n > 0
        available, total = meminfo_mb(m.buf, n)
        if available >= 0 && total > 0
            m.available_mb, m.total_mb, m.meminfo_ok = available, total, true
        end
    end
    read_usage!(m.cgroup, m.buf)
    if !isempty(m.psi_path)
        k = read_file!(m.buf, m.psi_path)
        m.psi = k > 0 ? psi_full_avg10(m.buf, k) : -1.0
    end
    cg_available, cg_limit = cgroup_memory_mb(m.cgroup)
    MemorySample(min(m.available_mb, cg_available), min(m.total_mb, cg_limit), m.psi)
end

measures_memory(m::HostMemory) = m.meminfo_ok
has_psi(m::HostMemory) = !isempty(m.psi_path)

# ---------------------------------------------------------------------------------------------
# Signals

own_pgrp() = ccall(:getpgrp, Cint, ())
own_sid() = ccall(:getsid, Cint, (Cint,), 0)

"""Send `sig` to `target` (a pid, or -pgid for a process group). Refuses init, "every process"
(-1), this process, and this process's own group and session, so a stale or bogus id can never
take down the driver, the guard or the user's shell."""
function send_signal(target::Integer, sig::Integer)
    abs(target) > 1 || return false
    target == getpid() && return false
    target == -own_pgrp() && return false
    target == -own_sid() && return false
    ccall(:kill, Cint, (Cint, Cint), target, sig) == 0
end

"""Whether the process group `pgid` still has a process (kill(-pgid, 0); EPERM counts as alive)."""
group_alive(pgid::Integer) =
    ccall(:kill, Cint, (Cint, Cint), -pgid, 0) == 0 || Libc.errno() != ERRNO_ESRCH

# ---------------------------------------------------------------------------------------------
# Probes used by the driver

"""Memory available to this process tree and the constrained total, in MiB: `(available, total)`.
On Linux this is MemAvailable/MemTotal bounded by every cgroup level; elsewhere it falls back to
`Sys.free_memory`/`Sys.total_memory`, which the driver uses only for defaults."""
function memory_snapshot()
    fallback = (Int(Sys.free_memory() ÷ BYTES_PER_MIB), Int(Sys.total_memory() ÷ BYTES_PER_MIB))
    Sys.islinux() || return fallback
    m = HostMemory()
    s = sample_memory!(m, monotonic_seconds())
    measures_memory(m) ? (s.available, s.total) : fallback
end

"""The number of CPUs this process may use: its affinity mask, bounded by any cgroup CPU quota
(v2 cpu.max, v1 cfs quota/period) from its own cgroup up to the root."""
function effective_cpus()
    cpus = max(1, Int(ccall(:jl_effective_threads, Int32, ())))
    Sys.islinux() || return cpus
    buf = Vector{UInt8}(undef, 8192)
    quota = cgroup_cpus(self_cgroup_tree(buf, "cpu"), buf)
    quota > 0 ? clamp(quota, 1, cpus) : cpus
end

"""Whether `sid` can be a session some other process created: never init's, nor this process's own
session, process group or pid (a stale or bogus id must not take down the caller or its shell)."""
foreign_session(sid::Integer) = sid > 1 && sid != getpid() && sid != own_pgrp() && sid != own_sid()

"""SIGKILL every process of the sessions `sids`: each session's group, and on Linux every member
found in /proc with its descendants, the sessions that setsid'd descendants lead, and all of their
process groups. Sessions that are not foreign are skipped. Never throws; used by the driver's
atexit and guard-lost paths."""
function kill_sessions!(sids::AbstractVector{Int32})
    Sys.iswindows() && return nothing
    targets = filter(foreign_session, sids)
    # Found before anything is killed: a killed parent's children are reparented, and lost.
    doomed = Sys.islinux() ? session_processes(targets) : ProcInfo[]
    for sid in targets
        send_signal(-Int(sid), SIGNAL_KILL)     # needs no file descriptor, unlike the scan
    end
    for p in doomed
        send_signal(p.pid, SIGNAL_KILL)
        send_signal(-Int(p.pgrp), SIGNAL_KILL)
    end
    nothing
end

"""The processes of the sessions `sids` with their descendants and everything in the sessions
those lead, from one scan of /proc (none when /proc cannot be listed)."""
function session_processes(sids::Vector{Int32})
    procs = ProcInfo[]
    scan_processes!(procs, Vector{UInt8}(undef, 4096), String[]) || return procs
    sessions = Set{Int32}(sids)
    doomed = Set{Int32}()
    grown = true
    while grown
        grown = false
        for p in procs
            !(p.pid in doomed) && (p.sid in sessions || p.ppid in doomed) || continue
            push!(doomed, p.pid)
            p.pid == p.sid && foreign_session(p.sid) && push!(sessions, p.sid)   # it called setsid
            grown = true
        end
    end
    filter!(p -> p.pid in doomed, procs)
end
