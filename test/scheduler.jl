@testset "jobs caps running steps however deeply nested" begin
    in_tempdir() do _
        g = Gauge()
        inner(i) = reduce(&, [Step(Symbol("n$(i)_$j"), () -> busy(g)) for j in 1:3])
        quiet(reduce(&, [inner(i) for i in 1:3]); jobs=2)
        @test g.peak[] == 2
        g = Gauge()
        quiet(ForEach(1:6) do i; Step(:b, () -> busy(g)); end; jobs=3)
        @test g.peak[] == 3
        # shell steps run concurrently even on one Julia thread
        t = @elapsed quiet(reduce(&, [Step(Symbol("s$i"), sh"sleep 0.5") for i in 1:4]); jobs=4)
        @test t < 1.9
    end
end

@testset "threads and memory budgets" begin
    in_tempdir() do _
        g = Gauge()
        quiet(reduce(&, [Step(Symbol("t$i"), () -> busy(g); threads=2) for i in 1:4]); jobs=4)
        @test g.peak[] == 2
        g = Gauge()
        quiet(reduce(&, [Step(Symbol("m$i"), () -> busy(g); mem_mb=600) for i in 1:4]); jobs=4, mem_mb=1000)
        @test g.peak[] == 1
        g = Gauge()
        quiet(reduce(&, [Step(Symbol("tm$i"), () -> busy(g); threads=2, mem_mb=400) for i in 1:4]); jobs=4, mem_mb=1200)
        @test g.peak[] == 2
        # a request larger than the budget runs alone instead of deadlocking
        r = quiet(Step(:huge, () -> 1; threads=64, mem_mb=10^9); jobs=2, mem_mb=100)
        @test r.success
        # with_resources sets defaults for steps that declare none
        g = Gauge()
        quiet(with_resources(reduce(&, [Step(Symbol("w$i"), () -> busy(g)) for i in 1:4]); threads=2); jobs=4)
        @test g.peak[] == 2
        # rules pass their resources and {threads} to their steps
        mkpath("raw"); foreach(s -> write("raw/$s.txt", s), ["A", "B", "C", "D"])
        heavy = @rule heavy("raw/{s}.txt" => "h/{s}.txt"; threads=2) = "sleep 0.3; echo {threads} > {output}"
        t = @elapsed quiet(heavy; jobs=4)
        @test read("h/A.txt", String) == "2\n" && t > 0.55       # two at a time, not four
        @test_throws ArgumentError quiet(Step(:x, sh"true"); jobs=0)
    end
end

@testset "priority: finish one branch before starting the next" begin
    pool = SP.Pool(1, 0, false)
    order = Int[]
    lk = ReentrantLock()
    one = SP.Need(1, 0, 0, 0, :none, 0.0, false)
    first_grant = SP.acquire!(pool, [0], one, SP.Watchdog())     # occupy the only slot
    tasks = [Threads.@spawn begin
                 g = SP.acquire!(pool, [p], one, SP.Watchdog())
                 lock(() -> push!(order, p), lk)
                 SP.release!(pool, g)
             end for p in (3, 1, 2)]
    @test timedwait(() -> length(pool.queue) == 3, 10) === :ok     # all three are queued
    SP.release!(pool, first_grant)
    foreach(wait, tasks)
    @test order == [1, 2, 3]
    # end to end: samples are worked on a few at a time, not all first steps first
    in_tempdir() do _
        events = Tuple{Symbol,Int}[]
        lk2 = ReentrantLock()
        log(e) = lock(() -> push!(events, e), lk2)
        fe = ForEach(1:8) do i
            Step(:first, () -> (sleep(0.01); log((:first, i)))) >> Step(:second, () -> (sleep(0.01); log((:second, i))))
        end
        quiet(fe; jobs=1)
        open_samples, most = Set{Int}(), 0
        for (kind, i) in events
            kind === :first ? push!(open_samples, i) : delete!(open_samples, i)
            most = max(most, length(open_samples))
        end
        @test length(events) == 16
        @test most <= 3
    end
end

# Pool unit tests: memory admission with samples injected by hand.
need_of(charge; threads=1, exclusive=false) = SP.Need(threads, charge, 0, 0, :none, 0.0, exclusive)
sample_of(available, reserve; paused=false, usage=Dict{Int,Int}()) = SP.GuardSample(available, reserve, paused, usage)
admit(pool, n; watchdog=SP.Watchdog()) = Threads.@spawn SP.acquire!(pool, [0], n, watchdog)
settled(task) = timedwait(() -> istaskdone(task), 2.0) === :ok

@testset "memory admission" begin
    # promises count at once: two 20 GB charges do not both pass a 36 GB headroom
    pool = SP.Pool(8, 0, true, sample_of(40_000, 2_000))
    a, b = admit(pool, need_of(20_000)), admit(pool, need_of(20_000))
    sleep(0.2)
    @test count(istaskdone, (a, b)) == 1 && length(pool.held) == 1
    SP.release!(pool, fetch(istaskdone(a) ? a : b))
    @test settled(istaskdone(a) ? b : a)
    # measured usage frees the unused part of a charge
    g = fetch(admit(pool, need_of(500)))
    @test SP.headroom(pool) == 40_000 - 4_000 - 20_000 - 500
    SP.update_memory!(pool, sample_of(40_000, 2_000; usage=Dict(g.id => 300)), time())
    @test SP.headroom(pool) == 40_000 - 4_000 - 20_000 - 200
    foreach(h -> SP.release!(pool, h), copy(pool.held))
    # paused: even an idle pool starts nothing until memory is free again
    SP.update_memory!(pool, sample_of(1_000, 2_000; paused=true), time())
    t = admit(pool, need_of(10))
    sleep(0.2)
    @test !istaskdone(t)
    @test SP.update_memory!(pool, sample_of(1_000, 2_000; paused=true), time())           # "waiting for memory"
    @test !SP.update_memory!(pool, sample_of(1_000, 2_000; paused=true), time())          # once per 30 s
    SP.update_memory!(pool, sample_of(40_000, 2_000), time())
    @test settled(t)
    SP.release!(pool, fetch(t))
    # a stale sample pauses admission
    @test SP.fits(pool, need_of(10), time()) && !SP.fits(pool, need_of(10), time() + SP.STALE_SAMPLE + 1)
    # without the guard memory is not gated
    @test SP.fits(SP.Pool(1, 0, false), need_of(10^9), time() + 100)
end

@testset "exclusive waiters, refusals" begin
    pool = SP.Pool(4, 0, false)
    g = fetch(admit(pool, need_of(10)))
    solo = admit(pool, need_of(10; exclusive=true))
    sleep(0.2)
    @test !istaskdone(solo)                         # waits for the pool to empty
    SP.release!(pool, g)
    @test settled(solo)
    other = admit(pool, need_of(10))
    sleep(0.2)
    @test !istaskdone(other)                        # nothing joins an exclusive step
    SP.release!(pool, fetch(solo))
    @test settled(other)
    SP.release!(pool, fetch(other))
    # paused with nothing running for too long: waiters are refused, not stuck
    pool = SP.Pool(4, 0, true, sample_of(500, 1_000; paused=true))
    t = admit(pool, need_of(10))
    sleep(0.2)
    t0 = time()
    SP.update_memory!(pool, sample_of(500, 1_000; paused=true), t0)
    SP.update_memory!(pool, sample_of(500, 1_000; paused=true), t0 + SP.MEMORY_WAIT_LIMIT + 1)
    @test settled(t) && fetch(t) isa SP.Refusal && fetch(t).failure.kind === :out_of_memory
    # a stopped run refuses waiters and later requests
    pool = SP.Pool(1, 0, false)
    g = fetch(admit(pool, need_of(10)))
    t = admit(pool, need_of(10))
    sleep(0.2)
    SP.close_pool!(pool, StepFailure(:stopped, "stopped: the run was interrupted"))
    @test settled(t) && fetch(t).failure.kind === :stopped
    @test fetch(admit(pool, need_of(10))) isa SP.Refusal
    # an expired Timeout refuses the steps waiting under it
    pool = SP.Pool(1, 0, false)
    g = fetch(admit(pool, need_of(10)))
    w = SP.Watchdog(SP.Watchdog(), 100.0)
    t = admit(pool, need_of(10); watchdog=w)
    sleep(0.2)
    SP.mark_expired!(w)
    SP.refuse!(pool, SP.watchdog_expired, SP.TIMEOUT_REFUSAL)
    @test settled(t) && fetch(t).failure.kind === :timed_out
    @test fetch(admit(pool, need_of(10); watchdog=w)) isa SP.Refusal
end

@testset "need: one memory number per step" begin
    pool = SP.Pool(4, 12_800, false)
    scope(r=SP.Resources()) = SP.Scope(false, r, SP.Watchdog(), Int[], "")
    n = SP.need(Step(:a, sh"x"; threads=8, mem_mb=100_000, timeout=3), scope(), pool)
    @test (n.threads, n.limit_mb, n.charge_mb, n.declared_mb, n.source, n.timeout) == (4, 12_800, 12_800, 100_000, :clamped, 3.0)
    n = SP.need(Step(:b, sh"x"), scope(), pool)
    @test (n.threads, n.limit_mb, n.charge_mb, n.source) == (1, 0, SP.UNDECLARED_CHARGE_MB, :none)
    n = SP.need(Step(:c, sh"x"), scope(SP.Resources(threads=2, mem_mb=500, timeout=9)), pool)
    @test (n.threads, n.limit_mb, n.source, n.timeout) == (2, 500, :scope, 9.0)
    n = SP.need(Step(:d, sh"x"; mem_mb=700), scope(SP.Resources(mem_mb=500)), SP.Pool(4, 0, false))
    @test (n.limit_mb, n.source) == (700, :declared)
end
