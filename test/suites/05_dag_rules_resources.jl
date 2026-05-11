@testset "DAG sharing: shared sub-step runs once across parallel branches" begin
    # Two parallel branches both start with the same Step instance.
    # The runtime should execute it once and let the second branch reuse the result.
    counter = Threads.Atomic{Int}(0)
    shared = Step(:shared, () -> (Threads.atomic_add!(counter, 1); "shared"))
    downstream_a = Step(:da, x -> "a:" * x)
    downstream_b = Step(:db, x -> "b:" * x)
    pipeline = (shared >> downstream_a) & (shared >> downstream_b)
    results = run(pipeline, verbose=false, force=true)
    @test counter[] == 1
    @test all(r -> r.success, results)
    # Both downstreams should have received the shared output.
    outs = [r.result for r in results if r.success]
    @test "a:shared" in outs && "b:shared" in outs
end

@testset "RunContext is per-run and thread-safe" begin
    # Each call to run() builds its own context, so two concurrent runs do not share state.
    s1 = Step(:r1, () -> 1)
    s2 = Step(:r2, () -> 2)
    ctx1 = SimplePipelines.RunContext()
    ctx2 = SimplePipelines.RunContext()
    @test ctx1 !== ctx2
    @test ctx1.state !== ctx2.state
    @test ctx1.memo !== ctx2.memo

    # Concurrent mark_complete! into a single context must not corrupt the set.
    ctx = SimplePipelines.RunContext()
    steps_to_mark = [Step(Symbol("conc_$i"), () -> i) for i in 1:200]
    Threads.@threads for s in steps_to_mark
        SimplePipelines.mark_complete!(ctx, s)
    end
    @test length(ctx.state) == 200
end

@testset "Freshness uses stable identity across sessions" begin
    # For named steps with declared outputs, freshness should not depend on a per-session
    # gensym — i.e. constructing the step in a fresh "session" still hits the cache.
    clear_state!()
    mktempdir() do dir
        cd(dir) do
            make_step() = @step build(["src.txt"] => ["out.txt"]) = sh"cp src.txt out.txt"
            write("src.txt", "hello")
            run(make_step(), verbose=false)
            @test isfile("out.txt")
            # Rebuild the step (new instance, same name/inputs/outputs) and confirm freshness.
            @test is_fresh(make_step())
        end
    end
    clear_state!()
end

@testset "Sequence data passing is deterministic" begin
    # When the upstream step has declared outputs, those paths are passed downstream
    # — never a heuristic look at the result string.
    mktempdir() do dir
        cd(dir) do
            producer = @step prod([] => ["data.tsv"]) = sh"echo row > data.tsv"
            consumer_input = Ref{Any}(nothing)
            consumer = Step(:cons, paths -> (consumer_input[] = paths; "ok"))
            run(producer >> consumer, verbose=false, force=true)
            @test consumer_input[] == ["data.tsv"]
        end
    end

    # No declared outputs: pass the upstream result.
    producer2 = Step(:p2, () -> "payload")
    seen = Ref{Any}(nothing)
    consumer2 = Step(:c2, x -> (seen[] = x; x))
    run(producer2 >> consumer2, verbose=false, force=true)
    @test seen[] == "payload"
end

@testset "Branch lifts Cmd and Function operands" begin
    b = Branch(() -> true, `echo true`, `echo false`)
    @test b.if_true isa Step
    @test b.if_false isa Step
    results = run(b, verbose=false, force=true)
    @test results[1].success
end

@testset "FilePath wrapper" begin
    fp = FilePath("/tmp/foo.tsv")
    @test fp isa FilePath
    @test fp.path == "/tmp/foo.tsv"
    @test string(fp) == "/tmp/foo.tsv"
    @test occursin("FilePath", sprint(show, fp))
    # Default materialize is identity for plain values; reads the file for FilePath.
    @test materialize(42) == 42
    dir = mktempdir()
    try
        p = joinpath(dir, "x.txt")
        write(p, "hello")
        @test materialize(FilePath(p)) == codeunits("hello")
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "Auto-spill: large results round-trip via SpilledValue" begin
    # Small result stays in RAM (no spill).
    small = Step(:small, () -> [1, 2, 3])
    rs = run(small, verbose=false, force=true)
    @test rs[1].result isa Vector{Int}
    @test rs[1].result == [1, 2, 3]

    # Large result gets spilled; downstream materialize reconstructs it.
    spill_dir = mktempdir()
    try
        big_data = collect(1:5_000_000)        # ~40 MB of Int — exceeds 10 MB default
        big = Step(:big, () -> big_data)
        results = run(big, verbose=false, force=true, spill_dir=spill_dir)
        @test results[1].result isa SpilledValue
        @test isfile(results[1].result.path)
        @test startswith(basename(results[1].result.path), "splpl_")
        @test materialize(results[1].result) == big_data

        # Threshold tunable: very small threshold spills tiny values too.
        tiny = Step(:tiny, () -> [1, 2, 3])
        r2 = run(tiny, verbose=false, force=true,
                 spill_threshold_bytes=1, spill_dir=spill_dir)
        @test r2[1].result isa SpilledValue
        @test materialize(r2[1].result) == [1, 2, 3]
    finally
        rm(spill_dir; recursive=true, force=true)
    end

    # auto_spill=false bypasses the hook entirely, even for big results.
    big_data2 = collect(1:5_000_000)
    big2 = Step(:big2, () -> big_data2)
    r3 = run(big2, verbose=false, force=true, auto_spill=false)
    @test r3[1].result isa Vector{Int}
    @test r3[1].result == big_data2

    # FilePath returned by the user is left untouched (already on disk).
    spill_dir2 = mktempdir()
    try
        user_path = joinpath(spill_dir2, "user.bin")
        write(user_path, repeat(b"x", 20_000_000))
        keep_fp = Step(:keep, () -> FilePath(user_path))
        r4 = run(keep_fp, verbose=false, force=true,
                 spill_threshold_bytes=1, spill_dir=spill_dir2)
        @test r4[1].result isa FilePath
        @test r4[1].result.path == user_path
    finally
        rm(spill_dir2; recursive=true, force=true)
    end

    # Failure path: error strings are not spilled (no .success).
    boom = Step(:boom, () -> error("nope"))
    r5 = run(boom, verbose=false, force=true,
             spill_threshold_bytes=1)
    @test !r5[1].success
    @test r5[1].result isa StepFailure
    @test occursin("nope", string(r5[1].result))

    # nothing return is preserved.
    nothing_step = Step(:n, () -> nothing)
    r6 = run(nothing_step, verbose=false, force=true,
             spill_threshold_bytes=1)
    @test r6[1].result === nothing
end

@testset "Shell stdout streams to disk (no IOBuffer balloon)" begin
    ss = SpilledStdout("/nonexistent/path")
    @test ss isa SpilledStdout
    @test string(ss) == "/nonexistent/path"
    @test occursin("SpilledStdout", sprint(show, ss))

    spill_dir = mktempdir()
    try
        small = Step(:small_sh, sh"echo hello")
        r = run(small, verbose=false, force=true, spill_dir=spill_dir)
        @test r[1].success
        @test r[1].result isa String
        @test strip(r[1].result) == "hello"
        @test isempty(filter(f -> startswith(f, "splpl_out_"), readdir(spill_dir)))

        big = Step(:big_sh, sh"yes hello | head -c 200000")
        rb = run(big, verbose=false, force=true,
                 spill_dir=spill_dir, spill_threshold_bytes=1024)
        @test rb[1].success
        @test rb[1].result isa SpilledStdout
        @test isfile(rb[1].result.path)
        @test startswith(basename(rb[1].result.path), "splpl_out_")
        @test filesize(rb[1].result.path) == 200000
        mat = materialize(rb[1].result)
        @test mat isa String
        @test length(mat) == 200000

        inmem = Step(:inmem_sh, sh"echo small")
        ri = run(inmem, verbose=false, force=true,
                 auto_spill=false, spill_dir=spill_dir)
        @test ri[1].result isa String
        @test strip(ri[1].result) == "small"

        bad = Step(:bad_sh, sh"echo to-stderr 1>&2; exit 7")
        rf = run(bad, verbose=false, force=true, spill_dir=spill_dir)
        @test !rf[1].success
        @test rf[1].result isa StepFailure
        @test occursin("to-stderr", string(rf[1].result))
        @test isempty(filter(f -> startswith(f, "splpl_out_"), readdir(spill_dir)))
    finally
        rm(spill_dir; recursive=true, force=true)
    end
end

@testset "Memory-safe defaults" begin
    # Auto-detected defaults must be > 0 and bounded by host capacity.
    @test default_jobs() >= 1
    @test default_jobs() <= max(1, Threads.nthreads())
    @test default_jobs() <= 8
    @test default_thread_budget() == default_jobs()
    # 50% of total RAM as MB. Should be at least a few MB on any real box.
    @test default_memory_budget_mb() > 0
    @test default_memory_budget_mb() <= floor(Int, Sys.total_memory() / 1_000_000)

    # A default-construction RunContext picks up the safe defaults.
    ctx = SimplePipelines.RunContext()
    @test ctx.jobs == default_jobs()
    @test ctx.memory_budget.capacity == default_memory_budget_mb()
    @test ctx.thread_budget.capacity == default_thread_budget()

    # User can still pass 0 to disable either cap.
    ctx0 = SimplePipelines.RunContext(jobs=0, memory_budget_mb=0, thread_budget=0)
    @test ctx0.jobs == 0
    @test ctx0.memory_budget.capacity == 0
    @test ctx0.thread_budget.capacity == 0
end

@testset "with_resources / Resourced" begin
    s = @step compute = `echo done`
    r = with_resources(s; mem_mb=128, threads=2)
    @test r isa Resourced
    @test r.resources.mem_mb == 128
    @test r.resources.threads == 2
    # Lifts Cmd operand
    rc = with_resources(`echo cmd`; mem_mb=10)
    @test rc isa Resourced
    @test rc.node isa Step
    # Runs and produces success
    results = run(r, verbose=false, force=true)
    @test results[1].success
end

@testset "Memory budget gates parallel branches" begin
    # Two heavy branches each requesting half the budget should run, but two
    # branches requesting the full budget each must serialize.
    active = Threads.Atomic{Int}(0)
    peak = Threads.Atomic{Int}(0)
    watch = (_) -> begin
        cur = Threads.atomic_add!(active, 1) + 1
        old = peak[]
        while cur > old
            Threads.atomic_cas!(peak, old, cur) == old && break
            old = peak[]
        end
        sleep(0.05)
        Threads.atomic_sub!(active, 1)
        "ok"
    end

    steps_heavy = [with_resources(Step(Symbol("h$i"), () -> watch(i)); mem_mb=600) for i in 1:4]
    node = reduce(&, steps_heavy)
    run(node, verbose=false, force=true, memory_budget_mb=1000, jobs=4)
    # With a 1000 MB budget and four 600 MB branches, at most one runs at a time.
    @test peak[] == 1
end

@testset "@rule and resolve: build DAG from output targets" begin
    dir = mktempdir()
    try
        cd(dir) do
            # Three rules forming a chain: source → align → index
            source = @rule source([] => "raw/{sample}.txt") =
                "mkdir -p raw && echo {sample} > {output}"
            align = @rule align("raw/{sample}.txt" => "out/{sample}.aln") =
                "mkdir -p out && cat {input} | tr a-z A-Z > {output}"
            index = @rule index("out/{sample}.aln" => "out/{sample}.aln.idx") =
                "wc -c {input} > {output}"

            plan = resolve([source, align, index],
                           ["out/A.aln.idx", "out/B.aln.idx"])
            @test plan isa AbstractNode
            results = run(plan, verbose=false)
            @test all(r -> r.success, results)
            @test isfile("out/A.aln.idx")
            @test isfile("out/B.aln.idx")
            @test isfile("out/A.aln")
            @test isfile("out/B.aln")
            @test isfile("raw/A.txt")
            @test isfile("raw/B.txt")

            # Re-running with everything fresh should produce no work.
            results2 = run(plan, verbose=false)
            @test all(r -> r.success || (r.result !== nothing && contains(string(r.result), "up to date")), results2)

            # An existing file short-circuits resolution; nothing to plan (see `resolve` docstring).
            @test resolve([source], ["raw/A.txt"]) isa NoWork
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "@rule: missing rule for target raises" begin
    r = @rule a("in/{x}.txt" => "out/{x}.txt") = "cp {input} {output}"
    @test_throws ErrorException resolve([r], ["nowhere/Z.bin"])
end

@testset "Rule wildcard substitution" begin
    # match_pattern + substitute roundtrip
    rx, names = SimplePipelines.pattern_to_regex("data/{kind}/{id}.fq")
    @test names == ["kind", "id"]
    m = SimplePipelines.match_pattern("data/{kind}/{id}.fq", "data/raw/sample01.fq")
    @test m == Dict("kind" => "raw", "id" => "sample01")
    @test SimplePipelines.match_pattern("data/{kind}/{id}.fq", "no/match.bam") === nothing
    s = SimplePipelines.substitute("out/{id}_{kind}.bam", m)
    @test s == "out/sample01_raw.bam"
    @test SimplePipelines.fill_special("cmd {input} > {output}",
                                       ["a", "b"], ["c"]) == "cmd a b > c"
    @test SimplePipelines.fill_special("cmd {input[1]} {output[1]}",
                                       ["a", "b"], ["c"]) == "cmd a c"
end

@testset "Rule sharing: shared dep resolved once" begin
    dir = mktempdir()
    try
        cd(dir) do
            # Two different terminal targets share a single common dep file.
            shared = @rule shared([] => "shared.txt") = "echo shared > {output}"
            # Two leaves both depending on shared.txt.
            use_a = @rule use_a("shared.txt" => "a.out") = "cp {input} {output}"
            use_b = @rule use_b("shared.txt" => "b.out") = "cp {input} {output}"

            plan = resolve([shared, use_a, use_b], ["a.out", "b.out"])
            results = run(plan, verbose=false)
            @test all(r -> r.success, results)
            @test isfile("a.out") && isfile("b.out") && isfile("shared.txt")
            # Each visiting branch reports the same StepResult instance for the
            # shared dep — i.e. the runtime executed it exactly once and shared
            # the result via the in-flight channel.
            shared_results = [r for r in results if r.step.name == :shared]
            @test !isempty(shared_results)
            ids = [objectid(r) for r in shared_results]
            @test all(==(ids[1]), ids)
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "Reduce: synthetic step is stable across calls" begin
    # The reducer step is cached on the Reduce node so state hashing and DAG
    # dedup see the same Step instance across visits.
    a = @step a = `echo a`
    b = @step b = `echo b`
    red = Reduce(xs -> length(xs), a & b; name=:counter)
    @test red.reduce_step.name == :counter

    results = run(red, verbose=false, force=true)
    @test results[end].success
    @test results[end].step === red.reduce_step
end

@testset "resolve: returns NoWork when all targets exist" begin
    dir = mktempdir()
    try
        cd(dir) do
            rule_a = @rule a([] => "a.txt") = "echo a > {output}"
            # Pre-create the target so resolve has nothing to do.
            write("a.txt", "already there")
            plan = resolve([rule_a], ["a.txt"])
            @test plan isa NoWork
            results = run(plan, verbose=false)
            @test isempty(results)
            @test count_steps(plan) == 0
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "@rule: rejects unbalanced wildcards" begin
    # `{smaple}` typo on the input must not silently produce empty substitutions.
    @test_throws ErrorException begin
        @rule bad("in/{smaple}.fq" => "out/{sample}.bam") = "cp {input} {output}"
    end
end

@testset "thread_budget gates parallel branches" begin
    # Four branches each requesting 2 threads under a 2-thread budget should
    # serialize: peak concurrency stays at 1.
    active = Threads.Atomic{Int}(0)
    peak = Threads.Atomic{Int}(0)
    bump_peak! = () -> begin
        cur = Threads.atomic_add!(active, 1) + 1
        old = peak[]
        while cur > old
            Threads.atomic_cas!(peak, old, cur) == old && break
            old = peak[]
        end
        sleep(0.04)
        Threads.atomic_sub!(active, 1)
        "ok"
    end
    nodes = AbstractNode[
        with_resources(Step(Symbol("t$i"), bump_peak!); threads=2)
        for i in 1:4
    ]
    run(reduce(&, nodes), verbose=false, force=true, thread_budget=2, jobs=4)
    @test peak[] == 1
end

@testset "memory and thread budgets compose" begin
    # A resourced node must fit both budgets before it runs. Here memory would
    # allow three concurrent branches and threads would allow two, so the
    # composed limit is two.
    active = Threads.Atomic{Int}(0)
    peak = Threads.Atomic{Int}(0)
    bump_peak! = () -> begin
        cur = Threads.atomic_add!(active, 1) + 1
        old = peak[]
        while cur > old
            Threads.atomic_cas!(peak, old, cur) == old && break
            old = peak[]
        end
        sleep(0.04)
        Threads.atomic_sub!(active, 1)
        "ok"
    end
    nodes = AbstractNode[
        with_resources(Step(Symbol("mt$i"), bump_peak!); mem_mb=400, threads=2)
        for i in 1:4
    ]
    run(reduce(&, nodes), verbose=false, force=true,
        memory_budget_mb=1200, thread_budget=4, jobs=4)
    @test peak[] == 2
end

@testset "resources release after resourced node throws" begin
    bad = with_resources(
        Branch(() -> error("boom"), Step(:unused_true, `true`), Step(:unused_false, `true`));
        mem_mb=1000,
        threads=2,
    )
    good_ran = Threads.Atomic{Int}(0)
    good = with_resources(
        Step(:after_resource_failure, () -> (Threads.atomic_add!(good_ran, 1); "ok"));
        mem_mb=1000,
        threads=2,
    )

    results = run(bad & good, verbose=false, force=true,
                  memory_budget_mb=1000, thread_budget=2, jobs=2)

    @test good_ran[] == 1
    @test any(r -> !r.success &&
                   r.result isa StepFailure &&
                   r.result.kind == :resource_exception &&
                   occursin("boom", string(r.result)),
              results)
    @test any(r -> r.success && r.step.name == :after_resource_failure, results)
end

@testset "Vector-backed Sequence/Parallel scales" begin
    # 100 children must build without tuple-type blow-up.
    # Use `Step(...)` here: `@step Symbol("step_i") = ...` parses as a call, so the macro
    # would treat `Symbol` as the step name and the string as an input path.
    many = [Step(Symbol("step_$i"), `true`) for i in 1:100]
    seq = reduce(>>, many)
    par = reduce(&, many)
    @test seq isa Sequence
    @test par isa Parallel
    @test length(seq.nodes) == 100
    @test length(par.nodes) == 100
    # Build a wider DAG and run a small slice to make sure dispatch still works.
    small_par = reduce(&, many[1:5])
    results = run(small_par, verbose=false, force=true)
    @test length(results) == 5 && all(r -> r.success, results)
end

@testset "show_result_inline dispatches on result type" begin
    # Display path for non-String results no longer hits `isa String` branching;
    # multi-line show works for Int/Vector/etc.
    s_int = Step(:n, () -> 42)
    results = run(s_int, verbose=false, force=true)
    io = IOBuffer()
    show(io, MIME("text/plain"), results[1])
    out = String(take!(io))
    @test occursin("StepResult: n", out)
    @test occursin("result:", out)
end

@testset "expand: Cartesian product over wildcards" begin
    # Single template, single wildcard
    @test expand("out/{s}.bam"; s=["A", "B", "C"]) ==
          ["out/A.bam", "out/B.bam", "out/C.bam"]

    # Single template, two wildcards — last wildcard varies fastest
    @test expand("out/{s}.{ext}"; s=["A", "B"], ext=["bam", "bai"]) ==
          ["out/A.bam", "out/A.bai", "out/B.bam", "out/B.bai"]

    # Multiple templates with shared wildcards (templates outer, combos inner)
    @test expand(["raw/{s}.fq", "qc/{s}.html"]; s=["A", "B"]) ==
          ["raw/A.fq", "raw/B.fq", "qc/A.html", "qc/B.html"]

    # Numeric wildcard values are stringified
    @test expand("step_{n}.txt"; n=1:3) ==
          ["step_1.txt", "step_2.txt", "step_3.txt"]

    # No wildcards: the template is returned unchanged (per-template copy)
    @test expand("static.txt") == ["static.txt"]
    @test expand(["a.txt", "b.txt"]) == ["a.txt", "b.txt"]

    # NamedTuple form (instead of kwargs) reaches the same path
    @test expand("{s}_R{r}.fq", (s=["A"], r=[1, 2])) ==
          ["A_R1.fq", "A_R2.fq"]

    # Missing wildcard substitution errors
    @test_throws ErrorException expand("{missing}.txt"; other=["x"])
end

@testset "Workflow: registry runs end-to-end" begin
    dir = mktempdir()
    try
        cd(dir) do
            wf = Workflow(name="rnaseq")
            # `push!` dispatches on item type:
            push!(wf,
                  @rule source([] => "raw/{s}.txt") =
                      "mkdir -p raw && echo {s} > {output}",
                  @rule align("raw/{s}.txt" => "out/{s}.aln") =
                      "mkdir -p out && tr a-z A-Z < {input} > {output}")
            # Vector of targets gets appended:
            push!(wf, expand("out/{s}.aln"; s=["A", "B"]))

            @test length(wf.rules) == 2
            @test wf.targets == ["out/A.aln", "out/B.aln"]

            node = plan(wf)
            @test node isa AbstractNode

            results = run(wf, verbose=false)
            @test all(r -> r.success, results)
            @test isfile("out/A.aln") && isfile("out/B.aln")

            # `targets` kwarg overrides the registry's default targets
            only_a = run(wf, verbose=false, targets=["out/A.aln"])
            @test all(r -> r.success, only_a)
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "Workflow: empty registry errors with clear message" begin
    wf = Workflow(name="empty")
    @test_throws ErrorException plan(wf)
    push!(wf, @rule x([] => "out.txt") = "echo x > {output}")
    # Rules but no targets: also errors
    @test_throws ErrorException plan(wf)
    push!(wf, "out.txt")
    @test plan(wf) isa AbstractNode
end

@testset "Liftable union: cross-type operators still work" begin
    # The 12-method-per-operator overload set was collapsed to 4 specific
    # AbstractNode methods + a single Liftable union fallback. All the same
    # cross-type combinations must still build the right node.
    f = () -> "func"
    c = `echo cmd`
    s = @step a = `echo a`
    @test (c >> c) isa Sequence
    @test (f >> f) isa Sequence
    @test (c >> f) isa Sequence
    @test (f >> c) isa Sequence
    @test (c >> s) isa Sequence
    @test (s >> c) isa Sequence
    @test (f >> s) isa Sequence
    @test (s >> f) isa Sequence
    @test (c & f) isa Parallel
    @test (s & c) isa Parallel
    @test (c | f) isa Fallback
    @test (s | c) isa Fallback
    @test (c^3) isa Retry
    @test (f^3) isa Retry
end
