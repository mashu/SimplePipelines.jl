# Tests for the resource guard (src/guard.jl with procfs.jl and policy.jl): the parsers, the
# cgroup walker on fake trees, `decide`, and end-to-end runs of the real guard process driven over
# its stdin/stdout. Included from test/runtests.jl, or run on its own: `julia test/guard.jl`.

if !isdefined(Main, :SP)
    using Test
    include(joinpath(@__DIR__, "..", "src", "guard.jl"))
end
const GD = isdefined(Main, :SP) ? Main.SP : Main      # where the guard's code lives

const TEST_MIB = 1 << 20

# ---------------------------------------------------------------------------------------------
# Helpers for decide()

test_step(id; members=1, usage=0, charge=usage, limit=0, started=Float64(id), step_deadline=Inf,
          node_deadline=Inf, leaders_dead_since=0.0, killed_at=0.0, grace=0.0, usage_at_kill=0) =
    GD.StepView(id, members, usage, charge, limit, started, step_deadline, node_deadline, leaders_dead_since,
                killed_at, grace, usage_at_kill)

test_view(steps; memory=true, available=10_000, total=10_000, min_free=500, reserve=500, psi=0.0,
          driver_growth=0, paused=false, low_count=0, psi_count=0, critical_since=0.0, driver_sigint_at=0.0,
          last_warn=-Inf) =
    GD.PolicyView(collect(GD.StepView, steps), memory, available, total, min_free, reserve, psi, driver_growth,
                  paused, low_count, psi_count, critical_since, driver_sigint_at, last_warn)

term_ids(r) = [t.id for t in r.terms]

# ---------------------------------------------------------------------------------------------
# Helpers for the cgroup walker

function put_file(root, path, text)
    mkpath(dirname(root * path))
    write(root * path, text)
end

const V2_MOUNTINFO = """
    22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
    30 22 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw,nsdelegate
    """

const V1_MOUNTINFO = """
    37 24 0:29 / /sys/fs/cgroup rw,relatime - tmpfs tmpfs rw,mode=755
    38 37 0:30 / /sys/fs/cgroup/cpu,cpuacct rw,relatime - cgroup cgroup rw,cpu,cpuacct
    41 37 0:33 / /sys/fs/cgroup/memory rw,relatime - cgroup cgroup rw,memory
    47 37 0:39 / /sys/fs/cgroup/unified rw,relatime - cgroup2 cgroup2 rw
    """

walk_memory(tree, buf=UInt8[]) = GD.cgroup_memory_mb(GD.read_usage!(GD.read_limits!(GD.CgroupMemory(tree), buf), buf))

# ---------------------------------------------------------------------------------------------
# Helpers for the end-to-end tests

struct TestGuard
    process::Base.Process
    input::Pipe
    lines::Channel{String}
    errors::String                  # the guard's stderr, shown if it fails
end

"""Start the real guard process as the driver does, with a fast tick. `main` replaces the entry
point, to run the fallback for systems without procfs."""
function start_test_guard(options::String...; main="guard_main()")
    julia = joinpath(Sys.BINDIR, Base.julia_exename())
    source = joinpath(@__DIR__, "..", "src", "guard.jl")
    cmd = `$julia --startup-file=no --history-file=no -O0 --threads=1 --color=no
           -e "include(popfirst!(ARGS)); $main" $source --interval=0.05 $options`
    input, output, errors = Pipe(), Pipe(), tempname()
    process = run(pipeline(cmd; stdin=input, stdout=output, stderr=errors); wait=false)
    close(input.out)
    close(output.in)
    lines = Channel{String}(Inf)
    @async begin
        for line in eachline(output)
            put!(lines, line)
        end
        close(lines)
    end
    TestGuard(process, input, lines, errors)
end

send_line(tg::TestGuard, line) = (println(tg.input, line); flush(tg.input))

"""The next line from the guard that satisfies `wanted` (a prefix or a predicate); "" on timeout."""
line_matcher(prefix::AbstractString) = line -> startswith(line, prefix)
line_matcher(predicate) = predicate

function guard_line(tg::TestGuard, wanted; timeout=5.0)
    matches = line_matcher(wanted)
    deadline = time() + timeout
    while time() < deadline
        if isready(tg.lines)
            line = take!(tg.lines)
            matches(line) && return line
        else
            sleep(0.01)
        end
    end
    ""
end

function stop_test_guard(tg::TestGuard)
    close(tg.input)
    timedwait(() -> process_exited(tg.process), 10.0)
    process_running(tg.process) && kill(tg.process, 9)
    show_guard_errors(tg)
end

"""Print the guard's stderr when it ended with an error rather than by EOF or a signal."""
function show_guard_errors(tg::TestGuard)
    process_exited(tg.process) && tg.process.exitcode != 0 && tg.process.termsignal == 0 || return
    println(stderr, "guard stderr:\n", read(tg.errors, String))
end

"""Start `cmd` in its own session (setsid), as the driver starts every leaf."""
spawn_session(cmd) = run(pipeline(Cmd(cmd; detach=true); stdin=devnull, stdout=devnull, stderr=devnull); wait=false)

exits_within(p, seconds) = timedwait(() -> process_exited(p), seconds) === :ok

"""A `sleep` duration no other process uses, so survivors can be looked up by command line."""
tagged_seconds(n) = "$n.$(getpid())"

cleanup(processes...) = foreach(p -> process_running(p) && kill(p, 9), processes)

"""Usage of step `id` in a `sample` line, or -1 when it is not listed."""
function sample_usage(fields, id)
    for f in fields[6:end]
        startswith(f, "$id:") && return GD.parse_int(f[length("$id:")+1:end])
    end
    -1
end

"""Whether a process whose command line is exactly `args` exists."""
function command_running(args::Vector{String})
    buf = UInt8[]
    want = join(args, '\0') * '\0'
    any(p -> GD.read_text(buf, "/proc/$(p.pid)/cmdline") == want, GD.scan_processes!(GD.ProcInfo[], buf))
end

# ---------------------------------------------------------------------------------------------

@testset "guard" begin
    @testset "integer and decimal parsing never throws" begin
        @test GD.parse_int("123") == 123
        @test GD.parse_int("  -1") == -1
        @test GD.parse_int("42kB") == 42
        @test GD.parse_int("") == 0
        @test GD.parse_int("abc") == 0
        @test GD.parse_int("-") == 0
        @test GD.parse_int("99999999999999999999999999") == typemax(Int)
        @test GD.parse_int("9223372036854771712") == 9223372036854771712
        b = codeunits("x 25.31 y")
        @test GD.parse_decimal(b, 2, length(b)) ≈ 25.31
        @test GD.parse_decimal(b, 8, length(b)) == -1.0
    end

    @testset "stat: comm with spaces and parentheses" begin
        p = GD.parse_stat("1234 ((a) b)) S 1 1234 1235 0 -1 4194560 26933 0 1389 0 30 13 0 0 20 0 5 0 1323283 749342720 4")
        @test p == GD.ProcInfo(1234, UInt8('S'), 1, 1234, 1235, 1323283)
        z = GD.parse_stat("77 (sh) Z 5 6 7 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 99 0 0")
        @test z.state == UInt8('Z') && !GD.process_alive(z)
        @test GD.process_alive(p)
        for garbage in ("", "garbage", "12 (x", "12 (x) ", "12 (x) S 1 2", "(x) S 1 2 3 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 5",
                        "12 (x) 7 1 2 3 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 5")
            @test GD.parse_stat(garbage) == GD.NO_PROC
        end
    end

    @testset "status, smaps_rollup, meminfo, PSI" begin
        status = "Name:\tperl\nVmRSS:\t  9000 kB\nRssAnon:\t  5000 kB\nRssFile:\t  3000 kB\nRssShmem:\t  1000 kB\nVmSwap:\t  24 kB\n"
        @test GD.status_usage_kb(status) == 6024                      # RssFile is left out
        @test GD.status_usage_kb("Name:\tkthreadd\nState:\tS\n") == 0  # kernel threads have no Rss lines
        rollup = "00400000-7ffd [rollup]\nRss:  900 kB\nPss:  700 kB\nPss_Anon:  400 kB\nPss_File:  250 kB\nPss_Shmem:  50 kB\nSwapPss:  10 kB\n"
        @test GD.rollup_pss_kb(rollup) == 460
        @test GD.rollup_pss_kb("Rss:  900 kB\nPss:  700 kB\nSwapPss:  10 kB\n") == 710   # older kernels
        @test GD.rollup_pss_kb("nothing here\n") == -1
        @test GD.meminfo_mb("MemTotal:       16479424 kB\nMemFree:  1 kB\nMemAvailable:   15778212 kB\n") == (15408, 16093)
        @test GD.meminfo_mb("MemTotal: 1024 kB\n") == (-1, 1)
        psi = "some avg10=1.50 avg60=0.00 avg300=0.00 total=298612\nfull avg10=25.25 avg60=0.00 avg300=0.00 total=298370\n"
        @test GD.psi_full_avg10(psi) ≈ 25.25
        @test GD.psi_full_avg10("some avg10=1.50 avg60=0.00\n") == -1.0
    end

    @testset "random bytes never throw" begin
        for _ in 1:200
            text = String(rand(UInt8(0):UInt8(127), rand(0:300)))
            b = codeunits(text)
            GD.parse_stat(b, length(b))
            GD.status_usage_kb(b, length(b))
            GD.rollup_pss_kb(b, length(b))
            GD.meminfo_mb(b, length(b))
            GD.psi_full_avg10(b, length(b))
            tree = GD.cgroup_tree(text, text, "/nonexistent", "memory")
            walk_memory(tree)
            GD.cgroup_cpus(tree, UInt8[])
        end
        @test true
    end

    @testset "file reader" begin
        mktempdir() do dir
            big = joinpath(dir, "big")
            write(big, repeat("0123456789", 30_000))
            buf = UInt8[]
            @test GD.read_file!(buf, big) == 300_000                # grows past its first size
            @test GD.read_file!(buf, joinpath(dir, "missing")) == -1
            @test GD.read_text(buf, joinpath(dir, "missing")) == ""
            @test GD.write_file(joinpath(dir, "big"), "x") && read(big, String)[1] == 'x'
            @test !GD.write_file(joinpath(dir, "nodir", "f"), "x")
        end
    end

    @testset "cgroup v2: working set, ancestors, memory.high" begin
        mktempdir() do root
            cg = "/sys/fs/cgroup"
            leaf = "$cg/user.slice/app"
            put_file(root, "$leaf/memory.max", "$(1000TEST_MIB)\n")
            put_file(root, "$leaf/memory.high", "max\n")
            put_file(root, "$leaf/memory.current", "$(900TEST_MIB)\n")
            # 90% of the usage is page cache the kernel can drop
            put_file(root, "$leaf/memory.stat", "anon 1\nactive_file 2\ninactive_file $(810TEST_MIB)\n")
            put_file(root, "$cg/user.slice/memory.max", "max\n")
            put_file(root, "$cg/user.slice/memory.high", "max\n")
            tree = GD.cgroup_tree("0::/user.slice/app\n", V2_MOUNTINFO, root, "memory")
            @test tree.version === GD.CgroupV2()
            @test tree.dirs == [root * leaf, root * "$cg/user.slice", root * cg]
            buf = UInt8[]
            mem = GD.read_usage!(GD.read_limits!(GD.CgroupMemory(tree), buf), buf)
            @test GD.cgroup_memory_mb(mem) == (910, 1000)

            # a missing file keeps the previous value
            rm(root * "$leaf/memory.current")
            rm(root * "$leaf/memory.max")
            put_file(root, "$leaf/memory.stat", "inactive_file 0\n")
            GD.read_usage!(GD.read_limits!(mem, buf), buf)
            @test GD.cgroup_memory_mb(mem) == (910, 1000)

            # only an ancestor is limited
            put_file(root, "$leaf/memory.max", "max\n")
            put_file(root, "$leaf/memory.current", "$(1500TEST_MIB)\n")
            put_file(root, "$cg/user.slice/memory.max", "$(2000TEST_MIB)\n")
            put_file(root, "$cg/user.slice/memory.current", "$(1500TEST_MIB)\n")
            put_file(root, "$cg/user.slice/memory.stat", "inactive_file $(500TEST_MIB)\n")
            @test walk_memory(tree) == (1000, 2000)

            # memory.high below memory.max is the effective limit
            put_file(root, "$cg/user.slice/memory.high", "$(1800TEST_MIB)\n")
            @test walk_memory(tree) == (800, 1800)

            # nothing limited: "max" everywhere is unlimited, not zero
            put_file(root, "$cg/user.slice/memory.max", "max\n")
            put_file(root, "$cg/user.slice/memory.high", "max\n")
            available, limit = walk_memory(tree)
            @test available == limit == GD.CGROUP_UNLIMITED ÷ TEST_MIB
        end
    end

    @testset "cgroup v1 on a hybrid system; huge values are unlimited" begin
        mktempdir() do root
            mem = "/sys/fs/cgroup/memory"
            huge = "9223372036854771712\n"
            for dir in ("$mem/batch/job", "$mem/batch", mem)
                put_file(root, "$dir/memory.limit_in_bytes", huge)
                put_file(root, "$dir/memory.usage_in_bytes", "$(3000TEST_MIB)\n")
                put_file(root, "$dir/memory.stat", "inactive_file 5\ntotal_inactive_file $(1000TEST_MIB)\n")
            end
            cgroup = "12:pids:/\n4:memory:/batch/job\n2:cpu,cpuacct:/\n0::/\n"
            tree = GD.cgroup_tree(cgroup, V1_MOUNTINFO, root, "memory")
            @test tree.version === GD.CgroupV1()
            @test tree.dirs == [root * "$mem/batch/job", root * "$mem/batch", root * mem]
            available, limit = walk_memory(tree)
            @test limit == 9223372036854771712 ÷ TEST_MIB
            @test min(16_000, limit) == 16_000           # disappears in the min with MemTotal
            put_file(root, "$mem/batch/memory.limit_in_bytes", "$(4000TEST_MIB)\n")
            @test walk_memory(tree) == (2000, 4000)      # 4000 − (3000 − 1000 inactive)
        end
    end

    @testset "cgroup: bind-mounted container root" begin
        mktempdir() do root
            # the container's own cgroup is mounted as /sys/fs/cgroup
            mountinfo = "500 400 0:26 /kubepods/pod1/c1 /sys/fs/cgroup ro,nosuid - cgroup2 cgroup2 rw\n"
            put_file(root, "/sys/fs/cgroup/memory.max", "$(512TEST_MIB)\n")
            put_file(root, "/sys/fs/cgroup/memory.current", "$(100TEST_MIB)\n")
            put_file(root, "/sys/fs/cgroup/memory.stat", "inactive_file 0\n")
            tree = GD.cgroup_tree("0::/kubepods/pod1/c1\n", mountinfo, root, "memory")
            @test tree.dirs == [root * "/sys/fs/cgroup"]
            @test walk_memory(tree) == (412, 512)
            # a path outside the mounted subtree also maps to the mount point
            other = GD.cgroup_tree("0::/elsewhere/x\n", mountinfo, root, "memory")
            @test other.dirs == [root * "/sys/fs/cgroup"]
            # escaped mount points
            spaced = GD.cgroup_tree("0::/\n", "30 22 0:26 / /my\\040cgroup rw - cgroup2 cgroup2 rw\n", root, "memory")
            @test spaced.dirs == [root * "/my cgroup"]
            @test GD.cgroup_tree("", mountinfo, root, "memory").dirs == String[]
            # a garbled mount point (empty) must not loop at "/"
            @test GD.cgroup_tree("0::/x/y\n", "30 22 0:26 /  rw - cgroup2 cgroup2 rw\n", "", "memory").dirs == ["/x/y", "/x", "/"]
        end
    end

    @testset "cgroup: CPU quota" begin
        mktempdir() do root
            cg = "/sys/fs/cgroup"
            put_file(root, "$cg/a/b/cpu.max", "150000 100000\n")
            put_file(root, "$cg/a/cpu.max", "max 100000\n")
            tree = GD.cgroup_tree("0::/a/b\n", V2_MOUNTINFO, root, "cpu")
            @test GD.cgroup_cpus(tree, UInt8[]) == 2           # 1.5 CPUs round up
            put_file(root, "$cg/a/cpu.max", "100000 100000\n")
            @test GD.cgroup_cpus(tree, UInt8[]) == 1           # an ancestor's quota is smaller
            put_file(root, "$cg/a/cpu.max", "max 100000\n")
            put_file(root, "$cg/a/b/cpu.max", "max 100000\n")
            @test GD.cgroup_cpus(tree, UInt8[]) == 0

            cpu = "/sys/fs/cgroup/cpu,cpuacct"
            put_file(root, "$cpu/job/cpu.cfs_quota_us", "300000\n")
            put_file(root, "$cpu/job/cpu.cfs_period_us", "100000\n")
            put_file(root, "$cpu/cpu.cfs_quota_us", "-1\n")
            put_file(root, "$cpu/cpu.cfs_period_us", "100000\n")
            v1 = GD.cgroup_tree("4:memory:/\n2:cpu,cpuacct:/job\n0::/\n", V1_MOUNTINFO, root, "cpu")
            @test v1.version === GD.CgroupV1()
            @test GD.cgroup_cpus(v1, UInt8[]) == 3
        end
    end

    @testset "decide: per-step limit, then SIGKILL after the grace" begin
        r = GD.decide(test_view([test_step(1; usage=150, limit=100), test_step(2; usage=90, limit=100)]), 10.0)
        @test r.terms == [GD.Term(1, :memory_limit, 1.0, false)]
        @test isempty(r.kill_now) && r.level === :ok && !r.paused
        dying = test_step(1; usage=150, limit=100, killed_at=10.0, grace=1.0, usage_at_kill=150)
        @test isempty(GD.decide(test_view([dying]), 10.5).kill_now)
        r = GD.decide(test_view([dying]), 11.0)
        @test isempty(r.terms) && r.kill_now == [1]
        # no grace at critical
        r = GD.decide(test_view([test_step(1; usage=150, limit=100)]; available=100), 10.0)
        @test GD.Term(1, :memory_limit, 0.0, false) in r.terms
    end

    @testset "decide: deadlines and leftovers" begin
        @test GD.decide(test_view([test_step(1; node_deadline=5.0, step_deadline=4.0)]), 5.0).terms ==
              [GD.Term(1, :node_timeout, 5.0, false)]
        @test GD.decide(test_view([test_step(1; step_deadline=5.0)]), 5.0).terms == [GD.Term(1, :step_timeout, 5.0, false)]
        @test isempty(GD.decide(test_view([test_step(1; step_deadline=5.0)]), 4.9).terms)
        # a step whose command has not started yet still has a deadline
        @test term_ids(GD.decide(test_view([test_step(1; members=0, step_deadline=5.0)]), 6.0)) == [1]
        # a finished step (leaders gone, no members) waits for `done`; no rule applies
        @test isempty(GD.decide(test_view([test_step(1; members=0, leaders_dead_since=1.0, step_deadline=5.0)]), 6.0).terms)
        leftover = test_step(1; leaders_dead_since=10.0)
        @test isempty(GD.decide(test_view([leftover]), 10.3).terms)
        @test GD.decide(test_view([leftover]), 10.5).terms == [GD.Term(1, :leftover, 1.0, false)]
    end

    @testset "decide: levels and pause hysteresis" begin
        level(a; paused=false) = (r = GD.decide(test_view([]; available=a, paused), 1.0); (r.level, r.paused))
        @test level(1500) == (:ok, false)
        @test level(1500; paused=true) == (:ok, false)
        @test level(1200) == (:tight, false)
        @test level(1200; paused=true) == (:tight, true)
        @test level(900) == (:low, true)
        @test GD.decide(test_view([]; available=10, memory=false), 1.0).level === :ok   # not measured
    end

    @testset "decide: pressure needs two samples and evicts the youngest that covers" begin
        steps = [test_step(1; usage=300, started=1.0), test_step(2; usage=300, started=2.0)]
        r = GD.decide(test_view(steps; available=400), 10.0)
        @test isempty(r.terms) && r.low_count == 1 && r.level === :low && r.paused
        r = GD.decide(test_view(steps; available=400, low_count=r.low_count), 10.1)
        @test r.level === :pressure
        @test r.terms == [GD.Term(2, :evicted, 1.0, false)]
    end

    @testset "decide: victim order" begin
        pool = [test_step(1; usage=500, charge=100, started=1.0),    # 400 over its charge
                test_step(2; usage=900, charge=800, started=2.0),    # 100 over
                test_step(3; usage=2000, charge=3000, started=3.0)]
        @test GD.victim_index(pool, 100) == 1
        pool = [test_step(1; usage=500, started=1.0), test_step(2; usage=300, started=2.0), test_step(3; usage=100, started=3.0)]
        @test GD.victim_index(pool, 250) == 2                        # youngest that covers the deficit
        @test GD.victim_index(pool, 1000) == 1                       # none covers: the largest
        r = GD.decide(test_view(pool; available=200, low_count=1), 1.0)
        @test r.level === :critical
    end

    @testset "decide: critical kills several victims at once" begin
        steps = [test_step(1; usage=100), test_step(2; usage=250), test_step(3; usage=120), test_step(4; usage=30)]
        r = GD.decide(test_view(steps; available=200), 1.0)      # deficit 300
        @test r.level === :critical
        @test r.terms == [GD.Term(2, :critical, 0.0, false), GD.Term(3, :critical, 0.0, false)]
        # a lone step is killed at critical, and the driver is told it ran alone
        r = GD.decide(test_view([test_step(1; usage=1000)]; available=200), 1.0)
        @test r.terms == [GD.Term(1, :critical, 0.0, true)]
        # terminating steps get SIGKILL at once
        dying = test_step(5; usage=400, killed_at=0.9, grace=5.0, usage_at_kill=400)
        @test 5 in GD.decide(test_view([dying]; available=100), 1.0).kill_now
        # PSI corroborates: two samples at ≥ 20 with A < 2·R_eff
        @test GD.decide(test_view([]; available=900, psi=25.0, psi_count=1), 1.0).level === :critical
        @test GD.decide(test_view([]; available=900, psi=25.0), 1.0).level === :low
        @test GD.decide(test_view([]; available=1200, psi=25.0, psi_count=5), 1.0).level === :tight
    end

    @testset "decide: outside pressure warns at most every 30 s and evicts nothing" begin
        steps = [test_step(1; usage=100), test_step(2; usage=50)]
        r = GD.decide(test_view(steps; available=300, low_count=1), 100.0)
        @test r.level === :pressure && r.paused && isempty(r.terms)
        @test occursin("other programs are using the memory", r.warning) && r.last_warn == 100.0
        @test occursin("use only 150 MiB", r.warning)
        r = GD.decide(test_view(steps; available=300, low_count=2, last_warn=90.0), 110.0)
        @test isempty(r.warning) && r.last_warn == 90.0 && isempty(r.terms)
        @test !isempty(GD.decide(test_view(steps; available=300, low_count=2, last_warn=70.0), 100.0).warning)
    end

    @testset "decide: a lone step is not evicted unless critical" begin
        r = GD.decide(test_view([test_step(1; usage=1000)]; available=400, low_count=1), 1.0)
        @test r.level === :pressure && isempty(r.terms) && isempty(r.warning)
    end

    @testset "decide: dying victims count as freed for 2 s" begin
        steps = [test_step(1; usage=300, killed_at=9.0, grace=1.0, usage_at_kill=300), test_step(2; usage=400)]
        r = GD.decide(test_view(steps; available=300, low_count=1), 10.0)
        @test r.low_count == 0 && isempty(r.terms)                   # A' = 300 + 300 ≥ 500
        r = GD.decide(test_view(steps; available=300, low_count=1), 11.5)
        @test r.terms == [GD.Term(2, :evicted, 1.0, false)]
        gone = [test_step(1; members=0, usage=0, killed_at=10.5, grace=1.0, usage_at_kill=300), test_step(2; usage=400)]
        @test GD.decide(test_view(gone; available=300, low_count=1), 11.0).low_count == 2   # its memory is really free now
    end

    @testset "decide: driver last resort timing" begin
        v(; growth=300, state...) = test_view([]; available=100, total=1000, driver_growth=growth, state...)
        r = GD.decide(v(), 10.0)
        @test r.driver === :none && r.critical_since == 10.0
        r = GD.decide(v(critical_since=10.0), 11.9)
        @test r.driver === :none
        r = GD.decide(v(critical_since=10.0), 12.0)
        @test r.driver === :sigint && r.driver_sigint_at == 12.0
        @test GD.decide(v(critical_since=10.0, driver_sigint_at=12.0), 13.0).driver === :none
        @test GD.decide(v(critical_since=10.0, driver_sigint_at=12.0), 14.0).driver === :sigkill
        @test GD.decide(v(critical_since=10.0; growth=200), 13.0).driver === :none      # Julia did not grow enough
        # something of ours is still killable: kill that first
        r = GD.decide(test_view([test_step(1; usage=500)]; available=100, total=1000, driver_growth=300, critical_since=1.0), 10.0)
        @test r.driver === :none && term_ids(r) == [1]
        r = GD.decide(test_view([]; available=5000, total=10_000, critical_since=10.0, driver_sigint_at=12.0), 13.0)
        @test r.critical_since == 0.0 && r.driver_sigint_at == 0.0 && r.driver === :none
    end

    @testset "reserve, growth and fast tick" begin
        @test GD.auto_min_free(1000) == 256
        @test GD.auto_min_free(13679) == 684
        @test GD.auto_min_free(100_000) == 2048
        @test GD.growth_rate([(0.0, 100), (0.5, 300), (1.0, 250), (1.1, 300)]) ≈ 500.0
        @test GD.growth_rate([(0.0, 100)]) == 0.0
        @test GD.effective_reserve(500, 300.0, 10_000) == 800
        @test GD.effective_reserve(500, 5000.0, 10_000) == 1500                 # capped at 10% T
        @test GD.effective_reserve(500, -50.0, 10_000) == 500
        @test GD.fast_tick(test_view([]; available=1400))
        @test !GD.fast_tick(test_view([]; available=1500))
        @test GD.fast_tick(test_view([test_step(1; usage=80, limit=100)]))
        @test !GD.fast_tick(test_view([test_step(1; usage=70, limit=100)]))
        @test GD.fast_tick(test_view([test_step(1; killed_at=1.0)]))
        @test GD.fast_tick(test_view([test_step(1; leaders_dead_since=1.0)]))
    end

    @testset "driver probes" begin
        available, total = GD.memory_snapshot()
        @test 0 < available <= total
        @test 1 <= GD.effective_cpus() <= Sys.CPU_THREADS
        if !Sys.iswindows()
            p = spawn_session(`sleep 31.25`)
            @test GD.kill_sessions!(Int32[getpid(p)]) === nothing
            @test exits_within(p, 5.0) && p.termsignal == 9
            @test GD.kill_sessions!(Int32[0, 1, -1]) === nothing           # refused, harmless
        end
    end

    if !Sys.iswindows()
        @testset "guard process without procfs (the macOS fallback)" begin
            tg = start_test_guard(; main="run_guard(GuardState(GroupBackend(), NoMemory(), 0.05))")
            ready = split(guard_line(tg, "ready"; timeout=60))
            @test ready[4] == "0" && ready[5] == "0"                   # memory and PSI not measured
            send_line(tg, "run 1 0 0")

            send_line(tg, "step 1 1 0 0 300 0 sleeper")
            started = time()
            sleeper = spawn_session(`sleep 30`)
            send_line(tg, "sess 1 $(getpid(sleeper))")
            @test exits_within(sleeper, 5.0)
            send_line(tg, "done 1")
            @test split(guard_line(tg, "gone 1"))[3] == "step_timeout"
            @test time() - started < 2.0

            # leftovers are looked for once the driver reports the leaders gone
            send_line(tg, "step 2 1 0 0 0 0 sh with leftover")
            seconds = tagged_seconds(31)
            shell = spawn_session(`sh -c "sleep $seconds & echo started"`)
            send_line(tg, "sess 2 $(getpid(shell))")
            @test exits_within(shell, 5.0)
            send_line(tg, "done 2")
            @test split(guard_line(tg, "gone 2"))[3] == "leftover"
            Sys.islinux() && @test !command_running(["sleep", seconds])

            send_line(tg, "step 3 1 0 0 0 0 sleeper")
            last = spawn_session(`sleep 30`)
            send_line(tg, "sess 3 $(getpid(last))")
            sample = split(guard_line(tg, l -> startswith(l, "sample") && occursin(" 3:", l)))
            @test sample[4] == "0" && sample[5] == "ok" && sample[end] == "3:0"
            close(tg.input)                                            # EOF: SIGKILL at once
            @test exits_within(last, 3.0) && last.termsignal == 9
            @test exits_within(tg.process, 10.0)
            show_guard_errors(tg)
            cleanup(sleeper, shell, last)
        end
    end

    if Sys.islinux()
        @testset "process readers" begin
            buf = UInt8[]
            procs = GD.scan_processes!(GD.ProcInfo[], buf)
            me = procs[findfirst(p -> p.pid == getpid(), procs)]
            @test me.ppid == ccall(:getppid, Cint, ()) && me.sid == ccall(:getsid, Cint, (Cint,), 0)
            @test GD.process_usage_kb(buf, getpid()) > 1000
            @test GD.process_pss_kb(buf, getpid()) > 1000
            @test GD.process_comm(buf, getpid()) == "julia"
            @test GD.process_usage_kb(buf, typemax(Int32)) == 0
        end

        @testset "guard process: limits, leftovers, adoption, timeouts" begin
            tg = start_test_guard()
            ready = split(guard_line(tg, "ready"; timeout=60))
            @test length(ready) == 5 && ready[4] == "1"
            @test startswith(guard_line(tg, "sample"), "sample")
            send_line(tg, "run 1 0 0")

            # 300 MiB touched with a 100 MiB limit
            send_line(tg, "step 1 1 100 100 0 0 perl big")
            started = time()
            big = spawn_session(`perl -e '$x = "a" x (300 * 2**20); sleep 30'`)
            send_line(tg, "sess 1 $(getpid(big))")
            @test exits_within(big, 5.0)
            send_line(tg, "done 1")
            gone = split(guard_line(tg, "gone 1"))
            @test gone[3] == "memory_limit"
            @test GD.parse_int(gone[4]) >= 100
            @test time() - started < 2.0

            # a background process outliving its shell
            send_line(tg, "step 2 1 0 0 0 0 sh with leftover")
            seconds = tagged_seconds(32)
            shell = spawn_session(`sh -c "sleep $seconds & echo started"`)
            send_line(tg, "sess 2 $(getpid(shell))")
            @test exits_within(shell, 5.0)
            send_line(tg, "done 2")
            started = time()
            line = guard_line(tg, "gone 2")
            @test split(line)[3] == "leftover"
            @test occursin(" sleep", line)
            @test time() - started < 2.5
            @test !command_running(["sleep", seconds])

            # a setsid'd child is adopted through its parent and terminated with the step
            send_line(tg, "step 3 1 0 0 0 0 setsid child")
            seconds = tagged_seconds(33)
            parent = spawn_session(`sh -c "setsid sleep $seconds & wait"`)
            send_line(tg, "sess 3 $(getpid(parent))")
            sleep(0.5)
            @test command_running(["sleep", seconds])
            send_line(tg, "term 3 interrupt")
            @test exits_within(parent, 5.0)
            send_line(tg, "done 3")
            @test split(guard_line(tg, "gone 3"))[3] == "interrupt"
            @test !command_running(["sleep", seconds])

            # a step timeout
            send_line(tg, "step 4 1 0 0 300 0 sleeper")
            started = time()
            sleeper = spawn_session(`sleep 30`)
            send_line(tg, "sess 4 $(getpid(sleeper))")
            @test exits_within(sleeper, 5.0)
            send_line(tg, "done 4")
            @test split(guard_line(tg, "gone 4"))[3] == "step_timeout"
            @test time() - started < 2.0

            # endrun terminates what a run left behind; done for an unknown id is answered at once
            send_line(tg, "run 2 0 0")
            send_line(tg, "step 5 2 0 0 0 0 abandoned")
            abandoned = spawn_session(`sleep 30`)
            send_line(tg, "sess 5 $(getpid(abandoned))")
            send_line(tg, "endrun 2")
            @test exits_within(abandoned, 5.0)
            send_line(tg, "done 5")
            @test split(guard_line(tg, "gone 5"))[3] == "interrupt"
            send_line(tg, "done 99")
            @test guard_line(tg, "gone 99") == "gone 99 none 0 0 0"
            send_line(tg, "bogus 1 2")
            @test startswith(guard_line(tg, "warn"), "warn the resource guard ignored")

            cleanup(big, shell, parent, sleeper, abandoned)
            stop_test_guard(tg)
            @test process_exited(tg.process)
        end

        @testset "guard process: stdin EOF kills tracked steps" begin
            tg = start_test_guard()
            @test !isempty(guard_line(tg, "ready"; timeout=60))
            send_line(tg, "run 1 0 0")
            send_line(tg, "step 1 1 0 0 0 0 sleeper")
            sleeper = spawn_session(`sleep 30`)
            send_line(tg, "sess 1 $(getpid(sleeper))")
            close(tg.input)
            @test exits_within(sleeper, 3.0) && sleeper.termsignal == 9
            @test exits_within(tg.process, 10.0)
            show_guard_errors(tg)
            cleanup(sleeper)
        end

        @testset "guard process: $name says bye and kills tracked steps" for (name, signal) in (("SIGTERM", 15), ("SIGINT", 2))
            tg = start_test_guard()
            @test !isempty(guard_line(tg, "ready"; timeout=60))
            send_line(tg, "run 1 0 0")
            send_line(tg, "step 1 1 0 0 0 0 sleeper")
            sleeper = spawn_session(`sleep 30`)
            send_line(tg, "sess 1 $(getpid(sleeper))")
            @test !isempty(guard_line(tg, l -> startswith(l, "sample") && occursin(" 1:", l)))
            kill(tg.process, 1)                                # SIGHUP is ignored
            sleep(0.3)
            @test process_running(tg.process) && process_running(sleeper)
            kill(tg.process, signal)
            @test guard_line(tg, "bye") == "bye interrupt"
            @test exits_within(sleeper, 3.0)
            @test exits_within(tg.process, 10.0)
            cleanup(sleeper)
        end

        @testset "guard process: memory pressure evicts the youngest; critical kills a lone step" begin
            mktempdir() do dir
                fake = joinpath(dir, "memory")
                set_memory(available) = (write(fake * ".new", "$available 10000 0.00\n"); mv(fake * ".new", fake; force=true))
                set_memory(8000)
                tg = start_test_guard("--fake-memory=$fake")
                ready = split(guard_line(tg, "ready"; timeout=60))
                @test ready[3] == "10000" && ready[4] == "1" && ready[5] == "1"
                send_line(tg, "run 1 0 0")
                touch50 = `perl -e '$x = "a" x (50 * 2**20); sleep 30'`        # about 100 MiB resident
                send_line(tg, "step 1 1 0 200 0 0 first")
                first = spawn_session(touch50)
                send_line(tg, "sess 1 $(getpid(first))")
                sleep(0.1)
                send_line(tg, "step 2 1 0 200 0 0 second")
                second = spawn_session(touch50)
                send_line(tg, "sess 2 $(getpid(second))")
                # both measured, and the growth term has decayed back near R = 5% of 10000
                settled(l) = (f = split(l); f[1] == "sample" && GD.parse_int(f[3]) < 600 &&
                              sample_usage(f, 1) >= 64 && sample_usage(f, 2) >= 64)
                @test !isempty(guard_line(tg, settled; timeout=10))
                set_memory(450)                    # pressure (A < R_eff), not critical (A ≥ R_eff/2)
                @test exits_within(second, 5.0)
                send_line(tg, "done 2")
                gone = split(guard_line(tg, "gone 2"))
                @test gone[3] == "evicted" && gone[6] == "0"
                sleep(0.5)
                @test process_running(first)       # a lone step is not evicted
                set_memory(100)                    # critical
                @test exits_within(first, 5.0) && first.termsignal == 9
                send_line(tg, "done 1")
                gone = split(guard_line(tg, "gone 1"))
                @test gone[3] == "critical" && gone[6] == "1"
                cleanup(first, second)
                stop_test_guard(tg)
            end
        end
    end
end
