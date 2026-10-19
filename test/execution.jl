@testset "shell steps" begin
    in_tempdir() do _
        r = quiet(Step(:hi, sh"echo hello"))
        @test r isa RunResult && success(r) && r.success
        @test r.value == "hello\n" && r[1].result == "hello\n" && r[1].status === :done
        @test quiet(Step(`echo backtick`)).value == "backtick\n"
        @test quiet(sh"echo direct").value == "direct\n"
        @test quiet(Step(:p, pipeline(`printf 'b\na\n'`, `sort`))).value == "a\nb\n"
        # strict mode: a failure in the middle of a pipe fails the step
        r = quiet(Step(:pipe, sh"false | cat"))
        @test !r.success && r[1].result.kind === :process_failed
        r = quiet(Step(:code, sh"echo oops >&2; exit 7"))
        @test r[1].result.message == "command exited with code 7"
        @test occursin("oops", r[1].result.detail)
        r = quiet(Step(:nope, `this-command-does-not-exist-123`))
        @test !r.success && r[1].result.kind === :exception
        # commands do not read the terminal
        @test quiet(Step(:stdin, sh"cat")).value == ""
        # commands that exit instantly never fail spuriously (regression: getpid race)
        fast = Parallel([Step(Symbol("f$i"), sh"true") for i in 1:40])
        @test all(_ -> quiet(fast; jobs=40).success, 1:10)
    end
end

@testset "bounded output capture" begin
    in_tempdir() do _
        r = quiet(Step(:flood, sh"yes hello"))                 # never ends on its own
        @test !r.success && r[1].result.kind === :stdout_limit
        r = quiet(Step(:noisy, sh"head -c 1000000 /dev/zero | tr '\0' e >&2; echo done"))
        @test r.success && r.value == "done\n"
        r = quiet(Step(:noisy_fail, sh"head -c 1000000 /dev/zero | tr '\0' e >&2; exit 1"))
        @test length(r[1].result.detail) <= SP.STDERR_TAIL + 4 && startswith(r[1].result.detail, "…")
    end
end

@testset "function steps and data flow" begin
    in_tempdir() do _
        # a zero-argument function after any step (regression: used to throw MethodError)
        r = quiet(sh"echo hi" >> (@step done = () -> "ok"))
        @test r.success && r.value == "ok"
        # one-argument functions receive the upstream value
        r = quiet((@step a = () -> 41) >> (@step b = x -> x + 1))
        @test r.value == 42 && statuses(r) == [:done, :done]
        # Parallel produces the vector of branch values, in order
        r = quiet(((@step x = () -> 2) & (@step y = () -> 3)) >> (@step s = v -> sum(v)))
        @test r.value == 5
        r = quiet((@step v = () -> [1, 2, 3]) >> sum)
        @test r.value == 6
        # declared outputs are what flows downstream
        r = quiet((@step w([] => "w.txt") = "echo w > {output}") >> (@step n = paths -> paths))
        @test r.value == ["w.txt"]
        # functions bound to paths
        write("in.txt", "abc")
        up = @step up("in.txt" => "out/up.txt") = (i, o) -> write(o, uppercase(read(i, String)))
        r = quiet(up)
        @test r.success && read("out/up.txt", String) == "ABC"      # output directory created
        @test r.value == ["out/up.txt"]
        # a function may return a command, which then runs
        x = "dynamic"
        r = quiet(@step dyn = () -> sh("echo $x"))
        @test r.value == "dynamic\n"
        r = quiet(@step dynpath("in.txt" => "copy.txt") = (i, o) -> sh("cp $i $o"))
        @test r.success && read("copy.txt", String) == "abc"
        # ... or a node, which runs as a sub-pipeline
        r = quiet(@step gen = () -> ForEach(["a", "b"]) do s; @step e = sh("echo $s"); end)
        @test r.success
        @test r.value == ["a\n", "b\n"]
        @test sort(labels(r)) == ["e[a]", "e[b]", "gen"]
        # values are never swapped for disk wrappers (regression: auto-spill changed types)
        big = collect(1:3_000_000)
        r = quiet((@step big = () -> big) >> (@step total = v -> sum(v)))
        @test r.value == sum(big)
        r = quiet(@step nothing_step = () -> nothing)
        @test r.success && r.value === nothing
    end
end

@testset "failures" begin
    in_tempdir() do _
        r = quiet(Step(:throws, () -> error("intentional")))
        f = r[1].result
        @test !r.success && f isa StepFailure && f.kind === :exception
        @test occursin("intentional", f.message) && occursin("[1]", f.detail)
        r = quiet(Step(:need, sh"true", ["missing.txt"], String[]))
        @test r[1].result.kind === :missing_input
        r = quiet(@step noout("in.txt" => "never.txt") = sh"true")
        @test r[1].result.kind === :missing_input
        write("in.txt", "x")
        r = quiet(@step noout2("in.txt" => "never.txt") = sh"true")
        @test r[1].result.kind === :missing_output
        # a failing step stops its sequence
        ran = Ref(false)
        r = quiet(Step(:bad, sh"false") >> Step(:after, () -> (ran[] = true)))
        @test !ran[] && length(r) == 1 && !r.success && r.value === nothing
        # but not independent branches
        r = quiet(Step(:bad, sh"false") & Step(:good, sh"echo ok"))
        @test !r.success && Set(statuses(r)) == Set([:failed, :done])
        # partial outputs of a failed step are removed, so they never look up to date
        s = @step partial("in.txt" => "p.txt") = "echo partial > {output}; exit 1"
        r = quiet(s)
        @test !isfile("p.txt") && occursin("removed incomplete output: p.txt", r[1].result.detail)
        @test !is_fresh(s)
        # outputs that existed before and were not touched are kept
        write("old.txt", "old")
        sleep(1.1)
        r = quiet(@step keep("in.txt" => "old.txt") = sh"exit 1"; force=true)
        @test isfile("old.txt")
    end
end

@testset "memoization" begin
    in_tempdir() do _
        # a step shared by several branches runs once
        count = Threads.Atomic{Int}(0)
        shared = Step(:shared, () -> (Threads.atomic_add!(count, 1); "s"))
        r = quiet((shared >> Step(:a, x -> "a" * x)) & (shared >> Step(:b, x -> "b" * x)))
        @test count[] == 1 && r.value == ["as", "bs"]
        @test nstatus(r, :done) == 3
        # a shared failure is not executed twice either
        fails = Threads.Atomic{Int}(0)
        flaky = Step(:flaky, () -> (Threads.atomic_add!(fails, 1); error("no")))
        quiet((flaky >> Step(:a, sh"true")) & (flaky >> Step(:b, sh"true")))
        @test fails[] == 1
        # distinct steps declaring the same outputs run once (no race on the file)
        make() = @step idx([] => "ref.idx") = "sleep 0.2; echo idx >> {output}"
        r = quiet(make() & make() & make())
        @test countlines("ref.idx") == 1 && length(r) == 1
        # shared sub-DAGs are not re-traversed (regression: exponential time)
        node = Step(:root, () -> 1)
        for i in 1:24
            node = (node >> Step(Symbol("l$i"), x -> x)) & (node >> Step(Symbol("r$i"), x -> x))
        end
        elapsed = @elapsed r = quiet(node)
        @test r.success && length(r) == 49 && elapsed < 10
    end
end

