@testset "Retry" begin
    in_tempdir() do _
        attempts = Threads.Atomic{Int}(0)
        flaky = Step(:flaky, () -> begin
            n = Threads.atomic_add!(attempts, 1) + 1
            n < 3 && error("transient $n")
            "ok on $n"
        end)
        r = quiet(flaky^5)
        @test r.success && r.value == "ok on 3" && attempts[] == 3
        @test statuses(r) == [:failed, :failed, :done]
        r = quiet(Retry(Step(:never, sh"false"), 2; delay=0.01))
        @test !r.success && length(r) == 2
        # a retried subtree re-runs only what failed
        upstream = Threads.Atomic{Int}(0)
        tries = Threads.Atomic{Int}(0)
        up = Step(:up, () -> (Threads.atomic_add!(upstream, 1); 1))
        down = Step(:down, x -> (Threads.atomic_add!(tries, 1) < 1 ? error("once") : x))
        r = quiet(Retry(up >> down, 3))
        @test r.success && upstream[] == 1 && tries[] == 2
    end
end

@testset "Fallback" begin
    in_tempdir() do _
        r = quiet(Step(:primary, sh"echo primary") | Step(:backup, sh"echo backup"))
        @test r.value == "primary\n" && length(r) == 1
        r = quiet(Step(:primary, sh"false") | Step(:backup, sh"echo backup"))
        @test r.success && r.value == "backup\n"           # recovered: the pipeline succeeds
        @test statuses(r) == [:failed, :done]
        r = quiet(sh"false" | sh"false" | sh"echo third")
        @test r.success && r.value == "third\n"
        r = quiet(Retry(Step(:p, sh"false"), 2) | Step(:safe, sh"echo safe"))
        @test r.success && r.value == "safe\n"
    end
end

@testset "Branch" begin
    in_tempdir() do _
        flag = Ref(true)
        b = Branch(() -> flag[], Step(:yes, sh"echo yes"), Step(:no, sh"echo no"))
        @test quiet(b).value == "yes\n"
        flag[] = false
        @test quiet(b).value == "no\n"
        @test quiet(@branch flag[] sh"echo t" sh"echo f").value == "f\n"
        # the condition can take the upstream value
        r = quiet((@step v = () -> 7) >> Branch(x -> x > 5, (@step big = x -> "big $x"), (@step small = x -> "small")))
        @test r.value == "big 7"
        r = quiet(Branch(() -> 1, sh"true", sh"true"))
        @test !r.success && occursin("must return a Bool", r[1].result.message)
        r = quiet(Branch(() -> error("bad condition"), sh"true", sh"true"))
        @test !r.success && occursin("bad condition", r[1].result.message)
    end
end

@testset "Timeout" begin
    in_tempdir() do _
        @test quiet(Timeout(Step(:fast, sh"echo quick"), 10)).value == "quick\n"
        # an overrunning command is terminated together with its children
        elapsed = @elapsed r = quiet(Timeout(Step(:slow, sh"sleep 40 | sleep 41"), 0.5))
        @test !r.success && elapsed < 5
        @test r[end].result.kind === :timed_out
        sleep(0.5)
        @test !success(pipeline(`pgrep -f "sleep 4[01]"`; stdout=devnull))
        # steps after the deadline are not started
        started = Ref(false)
        r = quiet(Timeout(Step(:slow2, sh"sleep 30") >> Step(:later, () -> (started[] = true)), 0.3))
        sleep(0.5)
        @test !r.success && !started[]
        # an exception inside is reported as a failure, not a timeout
        r = quiet(Timeout(Branch(() -> error("cond"), sh"true", sh"true"), 5))
        @test !r.success && r[1].result.kind === :exception
    end
end

@testset "Force" begin
    in_tempdir() do _
        s = @step make([] => "made.txt") = "date +%s%N >> {output}"
        quiet(s)
        @test statuses(quiet(s)) == [:skipped]
        @test statuses(quiet(Force(s))) == [:done]
        @test statuses(quiet(s; force=true)) == [:done]
        @test countlines("made.txt") == 3
    end
end
