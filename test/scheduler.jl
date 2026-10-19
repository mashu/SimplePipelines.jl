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
    pool = SP.Pool(1, 0)
    order = Int[]
    lk = ReentrantLock()
    first_grant = SP.acquire!(pool, [0], 1, 0)          # occupy the only slot
    tasks = [Threads.@spawn begin
                 g = SP.acquire!(pool, [p], 1, 0)
                 lock(() -> push!(order, p), lk)
                 SP.release!(pool, g)
             end for p in (3, 1, 2)]
    sleep(0.3)                                          # all three are queued
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
