@testset "ForEach over collection" begin
    # ForEach(items) is lazy: block runs only when pipeline runs, not at construction
    items = ["a", "b", "c"]
    parallel = ForEach(items) do x
        Step(Symbol(x), `echo $x`)
    end
    @test parallel isa ForEach
    @test length(parallel.source) == 3
    @test count_steps(parallel) == 0  # lazy: nodes not expanded until run

    # Single item still returns ForEach (one item), runs as one node
    single = ForEach(["only"]) do x
        Step(:only, `echo $x`)
    end
    @test single isa ForEach
    @test length(single.source) == 1

    # Execute: block runs here, then steps run
    results = run(parallel, verbose=false)
    @test length(results) == 3
    @test all(r -> r.success, results)
end

@testset "Reduce" begin
    # Basic reduce - combine parallel outputs
    a = @step a = `echo "output_a"`
    b = @step b = `echo "output_b"`
    
    r = Reduce(a & b, name=:combine) do outputs
        join(outputs, ",")
    end
    
    results = run(r, verbose=false, force=true)
    @test length(results) == 3  # a, b, reduce
    @test all(res -> res.success, results)
    @test contains(results[end].result, "output_a")
    @test contains(results[end].result, "output_b")
    
    # Reduce with function
    r2 = Reduce(length, a & b)
    results2 = run(r2, verbose=false)
    @test results2[end].success
    @test results2[end].result == 2  # 2 outputs (reducer return value stored as-is)
    
    # Reduce in pipeline
    fetch = @step fetch = `echo "data"`
    analyze_a = @step aa = `echo "result_a"`
    analyze_b = @step ab = `echo "result_b"`
    report = @step report = `echo "done"`
    
    pipeline = fetch >> Reduce(outputs -> join(outputs, "+"), analyze_a & analyze_b) >> report
    results3 = run(pipeline, verbose=false)
    @test all(res -> res.success, results3)

    # Reduce when one parallel branch fails: reducer step gets failure result
    ok = @step ok = `echo ok`
    fail_step = @step fail = `false`
    r_fail = Reduce(ok & fail_step, name=:after_fail) do outputs
        join(outputs, ",")
    end
    results_fail = run(r_fail, verbose=false, force=true)
    @test length(results_fail) == 3  # ok, fail_step, reduce
    @test !results_fail[2].success
    @test !results_fail[3].success
    @test occursin("upstream", string(results_fail[3].result)) || occursin("Reduce", string(results_fail[3].result))
    
    # Utilities
    @test count_steps(r) == 3  # a, b, reduce
    @test length(steps(r)) == 2  # a, b (reduce is synthetic)
    
    # show
    @test contains(sprint(show, r), "Reduce")
    
    # print_dag
    print_dag(r)
    @test true
end

@testset "Pipe (|>)" begin
    # Single result: left output becomes right input
    first_step = @step first = `echo "piped"`
    process_input(x) = (x isa String && contains(x, "piped") ? "ok" : "bad")
    second_step = @step process = process_input
    pipe = first_step |> second_step
    @test pipe isa SimplePipelines.Pipe
    @test pipe.first === first_step
    @test pipe.second === second_step
    results = run(pipe, verbose=false)
    @test length(results) == 2
    @test all(r -> r.success, results)
    @test results[2].result == "ok"
    @test count_steps(pipe) == 2
    @test length(steps(pipe)) == 2

    # Multi-result (ForEach): right step receives vector of branch outputs
    per_file = ForEach(["x", "y", "z"]) do x
        Step(Symbol("pipe_", x), `echo $x`)
    end
    combine_outputs(outputs) = join(sort([strip(String(o)) for o in outputs]), ",")
    reducer_step = @step combine = combine_outputs
    pipe2 = per_file |> reducer_step
    results2 = run(pipe2, verbose=false, force=true)
    @test length(results2) == 4  # 3 branch results + 1 combine result
    @test all(r -> r.success, results2)
    @test results2[end].result == "x,y,z"
end

@testset "Sequence (>>) data passing" begin
    # First step output is passed as input to the next (function) step
    gen() = "downloaded_path"
    process(path) = path * "_processed"
    s1 = @step gen = gen
    s2 = @step process = process
    seq = s1 >> s2
    results = run(seq, verbose=false)
    @test length(results) == 2 && all(r -> r.success, results)
    @test results[2].result == "downloaded_path_processed"
end

@testset "SameInputPipe (>>>)" begin
    # Both steps receive the same context input (e.g. branch id in ForEach)
    first_fn(x) = "a_$(x)"
    second_fn(x) = "b_$(x)"
    pipeline = ForEach([10, 20]) do id
        first_step = @step first = first_fn
        second_step = @step second = second_fn
        first_step >>> second_step
    end
    @test pipeline isa ForEach
    results = run(pipeline, verbose=false, force=true)
    @test length(results) == 4  # 2 branches × 2 steps
    @test all(r -> r.success, results)
    out = [r.result for r in results]
    @test "a_10" in out && "b_10" in out && "a_20" in out && "b_20" in out
    # Same-input: second step got the id, not first's output
    @test !("b_a_10" in out) && !("b_a_20" in out)
end

@testset "BroadcastPipe (.>>)" begin
    # .>> attaches the next step to each branch; each branch runs as branch >> step (no wait for all)
    fe = ForEach([1, 2]) do x
        Step(Symbol("echo_", x), `echo $x`)
    end
    process(x) = "got_" * strip(String(x))
    step = @step process = process
    bp = fe .>> step
    @test bp isa BroadcastPipe
    results = run(bp, verbose=false, force=true)
    @test length(results) == 4  # 2 branches × (echo + process)
    @test all(r -> r.success, results)
    out = [r.result for r in results]
    @test "got_1" in out && "got_2" in out
    # Versus |> which runs one process on the vector [out1, out2]
    pipe = fe |> step
    results2 = run(pipe, verbose=false, force=true)
    @test length(results2) == 3  # 2 branch results + 1 combine step
    @test results2[end].result != "got_1"  # combined, not per-branch
end

@testset "ForEach" begin
    # Create temp dir with test files
    dir = mktempdir()
    touch(joinpath(dir, "donor1_R1.fq.gz"))
    touch(joinpath(dir, "donor2_R1.fq.gz"))
    touch(joinpath(dir, "donor3_R1.fq.gz"))
    
    # ForEach is lazy: block runs only when run(pipeline) is called
    cd(dir) do
        pipeline = ForEach("{sample}_R1.fq.gz") do sample
            Step(Symbol("process_", sample), `echo $sample`)
        end
        @test pipeline isa ForEach
        @test count_steps(pipeline) == 0  # lazy: not expanded until run

        # Execute and verify (block runs here)
        results = run(pipeline, verbose=false)
        @test length(results) == 3
        @test all(r -> r.success, results)
        outputs = sort([r.result for r in results])
        @test "donor1\n" in outputs
        @test "donor2\n" in outputs
        @test "donor3\n" in outputs
    end

    # Auto-lift Cmd
    cd(dir) do
        pipeline = ForEach("{sample}_R1.fq.gz") do sample
            `echo $sample`
        end
        @test pipeline isa ForEach
        results = run(pipeline, verbose=false)
        @test all(r -> r.success, results)
    end

    # Single file match: still returns ForEach node (lazy)
    touch(joinpath(dir, "only_one.txt"))
    cd(dir) do
        node = ForEach("{name}_one.txt") do name
            Step(Symbol(name), `echo $name`)
        end
        @test node isa ForEach
        results = run(node, verbose=false)
        @test length(results) == 1 && results[1].success
    end

    # Nested directories with multiple wildcards
    mkdir(joinpath(dir, "projectA"))
    mkdir(joinpath(dir, "projectB"))
    touch(joinpath(dir, "projectA", "sample1.csv"))
    touch(joinpath(dir, "projectA", "sample2.csv"))
    touch(joinpath(dir, "projectB", "sample3.csv"))

    cd(dir) do
        pipeline = ForEach("{project}/{sample}.csv") do project, sample
            Step(Symbol(project, "_", sample), `echo $project $sample`)
        end
        @test pipeline isa ForEach
        results = run(pipeline, verbose=false)
        @test all(r -> r.success, results)
    end

    # Directory prefix before wildcard
    mkdir(joinpath(dir, "tsv"))
    touch(joinpath(dir, "tsv", "filtered-donor1.tsv.gz"))
    touch(joinpath(dir, "tsv", "filtered-donor2.tsv.gz"))
    cd(dir) do
        pipeline = ForEach("tsv/filtered-{donor}.tsv.gz") do donor
            Step(Symbol("process_", donor), `echo $donor`)
        end
        @test pipeline isa ForEach
        results = run(pipeline, verbose=false, force=true)
        @test all(r -> r.success, results)
        outputs = sort([r.result for r in results])
        @test "donor1\n" in outputs
        @test "donor2\n" in outputs
    end

    # No matches surfaces as a structured StepFailure (kind=:no_matches), not a raw exception.
    cd(dir) do
        pipeline = ForEach("{x}_nonexistent.xyz") do x
            Step(:x, `echo`)
        end
        results = run(pipeline, verbose=false)
        @test length(results) == 1
        @test !results[1].success
        @test results[1].result isa SimplePipelines.StepFailure
        @test results[1].result.kind === :no_matches
    end
    
    # Error on pattern without wildcard
    @test_throws ErrorException ForEach("no_wildcard.txt") do
        Step(:x, `echo`)
    end

    # ForEach block must return a node, not nothing (error when run)
    dir = mktempdir()
    try
        cd(dir) do
            write("a.txt", "a")
            pipeline = ForEach("{x}.txt") do x
                # block returns nothing
            end
            @test_throws ErrorException run(pipeline, verbose=false)
        end
    finally
        try; rm(dir; recursive=true); catch; end
    end
end

@testset "Operator composability" begin
    a = @step a = `echo a`
    b = @step b = `echo b`
    c = @step c = `echo c`
    d = @step d = `echo d`
    
    # All operators compose
    p1 = (a >> b) | c
    @test p1 isa Fallback
    
    p2 = (a & b)^3
    @test p2 isa Retry
    
    p3 = Timeout(a | b, 5.0) >> c
    @test p3 isa Sequence
    
    p4 = (a^2 | b) >> (c & d)
    @test p4 isa Sequence
    
    p5 = Branch(() -> true, a^2, b | c) >> d
    @test p5 isa Sequence
    
    # Complex nested composition
    complex = (
        (a^3 | b) >> 
        Timeout(c & d, 10.0) >> 
        Branch(() -> true, a, b)
    )
    @test complex isa Sequence
    
    # All should execute
    results = run(p1, verbose=false)
    @test !isempty(results)
end

