using SimplePipelines
using Test

@testset "SimplePipelines" begin
    @testset "sh string macro" begin
        # sh"..." enables shell features like redirection and pipes
        cmd = sh"echo hello > /tmp/test.txt"
        @test cmd isa Cmd
        @test cmd.exec == ["sh", "-c", "echo hello > /tmp/test.txt"]
        
        # Works in pipelines
        pipeline = sh"echo hello" >> sh"cat"
        @test pipeline isa Sequence
        
        # Actually runs with shell features (relative path so Windows backslashes don't break sh -c)
        dir = mktempdir()
        cd(dir) do
            p = sh("echo test > sh_test_out.txt")
            step = Step(:test, p)
            result = SimplePipelines.run_node(step, SimplePipelines.Silent())
            @test result[1].success
            @test isfile("sh_test_out.txt") && strip(read("sh_test_out.txt", String)) == "test"
        end
        rm(dir; recursive=true)
    end
    
    @testset "Step creation" begin
        # From Cmd
        s1 = Step(`echo hello`)
        @test s1.work == `echo hello`
        
        # Named step
        s2 = Step(:mystep, `echo world`)
        @test s2.name == :mystep
        
        # With dependencies (vector inputs)
        s3 = Step(:process, `cat input.txt`, ["input.txt"], ["output.txt"])
        @test s3.inputs == ["input.txt"]
        @test s3.outputs == ["output.txt"]
        
        # From function
        f = () -> 42
        s4 = Step(f)
        @test s4.work() == 42
    end
    
    @testset "@step macro" begin
        # Anonymous
        s1 = @step `echo test`
        @test s1.work == `echo test`
        
        # Named
        s2 = @step mystep = `echo named`
        @test s2.name == :mystep
        
        # With dependencies
        s3 = @step process("in.txt" => "out.txt") = `cat in.txt`
        @test s3.name == :process
        @test s3.inputs == ["in.txt"]
        @test s3.outputs == ["out.txt"]
        
        # Function step
        g() = "result"
        s4 = @step compute = g
        @test s4.name == :compute
    end
    
    @testset "Sequence operator >>" begin
        s1 = @step a = `echo a`
        s2 = @step b = `echo b`
        s3 = @step c = `echo c`
        f = () -> "func"
        
        # Basic sequence
        seq = s1 >> s2
        @test seq isa Sequence
        @test length(seq.nodes) == 2
        
        # Chaining (Sequence >> AbstractNode)
        seq3 = s1 >> s2 >> s3
        @test length(seq3.nodes) == 3
        
        # Direct Cmd chaining
        seq_cmd = `echo 1` >> `echo 2`
        @test seq_cmd isa Sequence
        
        # AbstractNode >> Sequence
        seq_ns = s1 >> (s2 >> s3)
        @test length(seq_ns.nodes) == 3
        
        # Sequence >> Sequence
        seq_ss = (s1 >> s2) >> (s2 >> s3)
        @test length(seq_ss.nodes) == 4
        
        # AbstractNode >> Cmd
        seq_nc = s1 >> `echo cmd`
        @test seq_nc isa Sequence
        
        # Cmd >> Sequence
        seq_cs = `echo cmd` >> (s1 >> s2)
        @test length(seq_cs.nodes) == 3
        
        # Function >> Function
        seq_ff = f >> f
        @test seq_ff isa Sequence
        
        # Function >> AbstractNode
        seq_fn = f >> s1
        @test seq_fn isa Sequence
        
        # AbstractNode >> Function
        seq_nf = s1 >> f
        @test seq_nf isa Sequence
        
        # Function >> Sequence
        seq_fs = f >> (s1 >> s2)
        @test length(seq_fs.nodes) == 3
        
        # Sequence >> Function
        seq_sf = (s1 >> s2) >> f
        @test length(seq_sf.nodes) == 3
        
        # Cmd >> Function
        seq_cf = `echo cmd` >> f
        @test seq_cf isa Sequence
        
        # Function >> Cmd
        seq_fc = f >> `echo cmd`
        @test seq_fc isa Sequence
    end
    
    @testset "Parallel operator &" begin
        s1 = @step a = `echo a`
        s2 = @step b = `echo b`
        s3 = @step c = `echo c`
        f = () -> "func"
        
        # Basic parallel
        par = s1 & s2
        @test par isa Parallel
        @test length(par.nodes) == 2
        
        # Chaining (Parallel & AbstractNode)
        par3 = s1 & s2 & s3
        @test length(par3.nodes) == 3
        
        # AbstractNode & Parallel
        par_np = s1 & (s2 & s3)
        @test length(par_np.nodes) == 3
        
        # Parallel & Parallel
        par_pp = (s1 & s2) & (s2 & s3)
        @test length(par_pp.nodes) == 4
        
        # Cmd & Cmd
        par_cc = `echo 1` & `echo 2`
        @test par_cc isa Parallel
        
        # Cmd & AbstractNode
        par_cn = `echo cmd` & s1
        @test par_cn isa Parallel
        
        # AbstractNode & Cmd
        par_nc = s1 & `echo cmd`
        @test par_nc isa Parallel
        
        # Cmd & Parallel
        par_cp = `echo cmd` & (s1 & s2)
        @test length(par_cp.nodes) == 3
        
        # Parallel & Cmd (via chaining)
        par_pc = (s1 & s2) & `echo cmd`
        @test length(par_pc.nodes) == 3
        
        # Function & Function
        par_ff = f & f
        @test par_ff isa Parallel
        
        # Function & AbstractNode
        par_fn = f & s1
        @test par_fn isa Parallel
        
        # AbstractNode & Function
        par_nf = s1 & f
        @test par_nf isa Parallel
        
        # Function & Parallel
        par_fp = f & (s1 & s2)
        @test length(par_fp.nodes) == 3
        
        # Parallel & Function
        par_pf = (s1 & s2) & f
        @test length(par_pf.nodes) == 3
        
        # Cmd & Function
        par_cf = `echo cmd` & f
        @test par_cf isa Parallel
        
        # Function & Cmd
        par_fc = f & `echo cmd`
        @test par_fc isa Parallel
    end
    
    @testset "Constructors" begin
        s1 = @step a = `echo a`
        s2 = @step b = `echo b`
        s3 = @step c = `echo c`
        
        # Sequence vararg constructor
        seq = Sequence(s1, s2, s3)
        @test length(seq.nodes) == 3
        
        # Parallel vararg constructor
        par = Parallel(s1, s2, s3)
        @test length(par.nodes) == 3
        
        # Pipeline vararg constructor
        p = Pipeline(s1, s2, s3, name="vararg")
        @test p.name == "vararg"
        @test count_steps(p.root) == 3
    end
    
    @testset "Complex DAG" begin
        # Diamond pattern: a -> (b & c) -> d
        a = @step a = `echo a`
        b = @step b = `echo b`
        c = @step c = `echo c`
        d = @step d = `echo d`
        
        dag = a >> (b & c) >> d
        @test dag isa Sequence
        @test dag.nodes[1] isa Step
        @test dag.nodes[2] isa Parallel
        @test dag.nodes[3] isa Step
    end
    
    @testset "Pipeline" begin
        s1 = @step `echo hello`
        s2 = @step `echo world`
        
        p = Pipeline(s1 >> s2, name="test")
        @test p.name == "test"
        @test count_steps(p.root) == 2
        
        # Default name
        p2 = Pipeline(s1 >> s2)
        @test p2.name == "pipeline"
    end
    
    @testset "Step execution" begin
        # Simple echo command
        s = @step test = `echo "hello world"`
        results = run(s, verbose=false)
        
        @test length(results) == 1
        @test results[1].success == true
        @test contains(results[1].output, "hello world")
    end
    
    @testset "Sequence execution" begin
        seq = `echo first` >> `echo second`
        results = run(seq, verbose=false)
        
        @test length(results) == 2
        @test all(r -> r.success, results)
    end
    
    @testset "Parallel execution" begin
        # Verify parallel structure executes correctly
        par = `echo "a"` & `echo "b"` & `echo "c"`
        
        results = run(par, verbose=false)
        
        @test length(results) == 3
        @test all(r -> r.success, results)
    end
    
    @testset "Julia function steps" begin
        counter = Ref(0)
        f = () -> (counter[] += 1; "done")
        
        s = Step(:increment, f)
        results = run(s, verbose=false)
        
        @test results[1].success
        @test counter[] == 1
        
        # Function returning nothing
        f_nothing = () -> nothing
        s_nothing = Step(:nothing_func, f_nothing)
        results_nothing = run(s_nothing, verbose=false)
        @test results_nothing[1].success
        @test results_nothing[1].output == ""
    end
    
    @testset "Verbose execution" begin
        s = @step test = `echo "verbose test"`
        
        # Just run with verbose=true to exercise the code path
        results = run(s, verbose=true)
        @test results[1].success
    end
    
    @testset "Verbose parallel" begin
        par = `echo a` & `echo b`
        
        # Just run with verbose=true to exercise the code path
        results = run(par, verbose=true)
        @test all(r -> r.success, results)
    end
    
    @testset "Utility functions" begin
        a = @step a = `echo a`
        b = @step b = `echo b`
        c = @step c = `echo c`
        
        dag = a >> (b & c)
        
        @test count_steps(dag) == 3
        @test length(steps(dag)) == 3
        
        # count_steps for single step
        @test count_steps(a) == 1
    end
    
    @testset "Dry run" begin
        dag = `echo a` >> (`echo b` & `echo c`) >> `echo d`
        
        # Dry run with verbose (exercises print_dag)
        results = run(dag, dry_run=true, verbose=true)
        @test isempty(results)
        
        # Dry run without verbose
        results = run(dag, dry_run=true, verbose=false)
        @test isempty(results)
    end
    
    @testset "print_dag" begin
        a = Step(:a, `echo a`, ["in.txt"], ["out.txt"])
        b = @step b = `echo b`
        c = @step c = `echo c`
        
        dag = a >> (b & c)
        
        # Just call print_dag to exercise code path
        print_dag(dag)
        @test true  # If we get here, it worked
    end
    
    @testset "Base.show" begin
        a = @step a = `echo a`
        b = @step b = `echo b`
        
        seq = a >> b
        par = a & b
        p = Pipeline(seq, name="test")
        
        # Test show methods
        @test contains(sprint(show, a), "Step")
        @test contains(sprint(show, a), ":a")
        @test contains(sprint(show, seq), "Sequence")
        @test contains(sprint(show, par), "Parallel")
        @test contains(sprint(show, p), "Pipeline")
        @test contains(sprint(show, p), "test")
    end
    
    @testset "Base.run" begin
        s = @step test = `echo "run test"`
        p = Pipeline(s, name="run_test")
        
        # Base.run runs the pipeline
        results = run(p, verbose=false)
        @test length(results) == 1
        @test results[1].success
    end
    
    @testset "Error handling - missing input" begin
        # Cmd step with missing input file
        s_cmd = Step(:cmd_missing, `cat nonexistent.txt`, ["nonexistent_file_12345.txt"], String[])
        results = run(s_cmd, verbose=false)
        @test !results[1].success
        @test contains(results[1].output, "Missing input file")
        
        # Function step with missing input file
        s_func = Step(:func_missing, () -> "test", ["nonexistent_file_12345.txt"], String[])
        results_func = run(s_func, verbose=false)
        @test !results_func[1].success
        @test contains(results_func[1].output, "Missing input file")
    end
    
    @testset "Error handling - command failure" begin
        # Command that fails
        s = @step fail = `false`
        results = run(s, verbose=false)
        @test !results[1].success
        @test contains(results[1].output, "Error")
    end
    
    @testset "Error handling - function throws" begin
        # Function that throws
        s = Step(:throws, () -> error("intentional error"))
        results = run(s, verbose=false)
        @test !results[1].success
        @test contains(results[1].output, "intentional error")
    end
    
    @testset "Error handling - verbose failure" begin
        s = @step fail = `false`
        
        # Run with verbose to exercise failure printing
        results = run(s, verbose=true)
        @test !results[1].success
    end
    
    @testset "Sequence stops on failure" begin
        # First step fails, second should not run
        counter = Ref(0)
        fail_step = @step fail = `false`
        count_step = Step(:count, () -> (counter[] += 1; "done"))
        
        seq = fail_step >> count_step
        results = run(seq, verbose=false)
        
        @test length(results) == 1  # Only first step ran
        @test !results[1].success
        @test counter[] == 0  # Second step never executed
    end
    
    @testset "Type stability" begin
        # Verify operations produce concrete types
        a = @step a = `echo a`
        b = @step b = `echo b`
        
        seq = a >> b
        @test isconcretetype(typeof(seq))
        
        par = a & b
        @test isconcretetype(typeof(par))
        
        # Pipeline should be concrete
        p = Pipeline(seq, name="test")
        @test isconcretetype(typeof(p))
    end
    
    @testset "Retry" begin
        # Retry that succeeds on first try
        s = @step ok = `echo "success"`
        r = Retry(s, 3)
        results = run(r, verbose=false)
        @test length(results) == 1
        @test results[1].success
        
        # Retry with delay
        r2 = Retry(s, 2, delay=0.01)
        results2 = run(r2, verbose=false)
        @test results2[1].success
        
        # Retry that always fails
        fail = @step fail = `false`
        r3 = Retry(fail, 2)
        results3 = run(r3, verbose=false)
        @test !results3[1].success
        
        # Utilities
        @test count_steps(r) == 1
        @test length(steps(r)) == 1
        
        # show
        @test contains(sprint(show, r), "Retry")
    end
    
    @testset "Fallback operator |" begin
        success_step = @step ok = `echo "primary"`
        fail_step = @step fail = `false`
        fallback_step = @step fallback = `echo "fallback"`
        
        # Primary succeeds - fallback not used
        f1 = success_step | fallback_step
        results1 = run(f1, verbose=false)
        @test length(results1) == 1
        @test results1[1].success
        @test contains(results1[1].output, "primary")
        
        # Primary fails - fallback used
        f2 = fail_step | fallback_step
        results2 = run(f2, verbose=false)
        @test results2[end].success
        @test contains(results2[end].output, "fallback")
        
        # Chain fallbacks
        f3 = fail_step | fail_step | success_step
        @test f3 isa Fallback
        
        # With Cmd directly
        f4 = `false` | `echo "backup"`
        @test f4 isa Fallback
        
        # Utilities
        @test count_steps(f1) == 2
        @test length(steps(f1)) == 2
        
        # show
        @test contains(sprint(show, f1), "|")
    end
    
    @testset "Branch" begin
        flag = Ref(true)
        
        true_branch = @step true_branch = `echo "true path"`
        false_branch = @step false_branch = `echo "false path"`
        
        b = Branch(() -> flag[], true_branch, false_branch)
        
        # Condition true
        flag[] = true
        results1 = run(b, verbose=false)
        @test results1[1].success
        @test contains(results1[1].output, "true path")
        
        # Condition false
        flag[] = false
        results2 = run(b, verbose=false)
        @test results2[1].success
        @test contains(results2[1].output, "false path")
        
        # Utilities
        @test count_steps(b) == 1  # max of branches
        @test length(steps(b)) == 2  # both branches
        
        # show
        @test contains(sprint(show, b), "Branch")
    end
    
    @testset "Retry + Fallback composition" begin
        # Combine: retry a flaky step, if all retries fail use fallback
        flaky = @step flaky = `false`
        safe = @step safe = `echo "safe"`
        
        pipeline = Retry(flaky, 2) | safe
        results = run(pipeline, verbose=false)
        
        # Should have run flaky twice, then safe
        @test results[end].success
        @test contains(results[end].output, "safe")
    end
    
    @testset "print_dag for new types" begin
        s = @step s = `echo test`
        r = Retry(s, 3, delay=1.0)
        f = s | s
        b = Branch(() -> true, s, s)
        t = Timeout(s, 5.0)
        
        # Just verify they don't error
        print_dag(r)
        print_dag(f)
        print_dag(b)
        print_dag(t)
        @test true
    end
    
    @testset "Retry ^ operator" begin
        s = @step s = `echo "test"`
        
        # ^ creates Retry
        r = s^3
        @test r isa Retry
        @test r.max_attempts == 3
        
        # Works with Cmd directly
        r2 = `echo "cmd"`^2
        @test r2 isa Retry
        @test r2.max_attempts == 2
        
        # Composable with fallback
        fallback = @step fb = `echo "fallback"`
        pipeline = s^3 | fallback
        @test pipeline isa Fallback
        
        # Run it
        results = run(r, verbose=false)
        @test results[1].success
    end
    
    @testset "Timeout" begin
        # Fast step completes
        fast = @step fast = `echo "quick"`
        t = Timeout(fast, 5.0)
        results = run(t, verbose=false)
        @test results[1].success
        
        # Utilities
        @test count_steps(t) == 1
        @test length(steps(t)) == 1
        
        # show
        @test contains(sprint(show, t), "Timeout")
        
        # Composable
        fallback = @step fb = `echo "fallback"`
        pipeline = Timeout(fast, 5.0)^3 | fallback
        @test pipeline isa Fallback
    end
    
    @testset "Map" begin
        # Basic map over items
        items = ["a", "b", "c"]
        parallel = Map(items) do x
            Step(Symbol(x), `echo $x`)
        end
        @test parallel isa Parallel
        @test count_steps(parallel) == 3
        
        # Single item returns single node
        single = Map(["only"]) do x
            Step(:only, `echo $x`)
        end
        @test single isa Step
        
        # Execute map
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
        
        results = run(r, verbose=false)
        @test length(results) == 3  # a, b, reduce
        @test all(res -> res.success, results)
        @test contains(results[end].output, "output_a")
        @test contains(results[end].output, "output_b")
        
        # Reduce with function
        r2 = Reduce(length, a & b)
        results2 = run(r2, verbose=false)
        @test results2[end].success
        @test results2[end].output == "2"  # 2 outputs
        
        # Reduce in pipeline
        fetch = @step fetch = `echo "data"`
        analyze_a = @step aa = `echo "result_a"`
        analyze_b = @step ab = `echo "result_b"`
        report = @step report = `echo "done"`
        
        pipeline = fetch >> Reduce(outputs -> join(outputs, "+"), analyze_a & analyze_b) >> report
        results3 = run(pipeline, verbose=false)
        @test all(res -> res.success, results3)
        
        # Utilities
        @test count_steps(r) == 3  # a, b, reduce
        @test length(steps(r)) == 2  # a, b (reduce is synthetic)
        
        # show
        @test contains(sprint(show, r), "Reduce")
        
        # print_dag
        print_dag(r)
        @test true
    end
    
    @testset "ForEach" begin
        # Create temp dir with test files
        dir = mktempdir()
        touch(joinpath(dir, "donor1_R1.fq.gz"))
        touch(joinpath(dir, "donor2_R1.fq.gz"))
        touch(joinpath(dir, "donor3_R1.fq.gz"))
        
        # Single wildcard - creates parallel branches
        cd(dir) do
            pipeline = ForEach("{sample}_R1.fq.gz") do sample
                Step(Symbol("process_", sample), `echo $sample`)
            end
            @test pipeline isa Parallel
            @test count_steps(pipeline) == 3
            
            # Execute and verify
            results = run(pipeline, verbose=false)
            @test length(results) == 3
            @test all(r -> r.success, results)
            outputs = sort([r.output for r in results])
            @test "donor1\n" in outputs
            @test "donor2\n" in outputs
            @test "donor3\n" in outputs
        end
        
        # Auto-lift Cmd - simpler syntax!
        cd(dir) do
            pipeline = ForEach("{sample}_R1.fq.gz") do sample
                `echo $sample`  # Just return Cmd, auto-wrapped to Step
            end
            @test pipeline isa Parallel
            results = run(pipeline, verbose=false)
            @test all(r -> r.success, results)
        end
        
        # Single file match returns single node (not Parallel)
        touch(joinpath(dir, "only_one.txt"))
        cd(dir) do
            node = ForEach("{name}_one.txt") do name
                Step(Symbol(name), `echo $name`)
            end
            @test node isa Step
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
            @test pipeline isa Parallel
            @test count_steps(pipeline) == 3
            
            results = run(pipeline, verbose=false)
            @test all(r -> r.success, results)
        end
        
        # Error on no matches
        cd(dir) do
            @test_throws ErrorException ForEach("{x}_nonexistent.xyz") do x
                Step(:x, `echo`)
            end
        end
        
        # Error on pattern without wildcard
        @test_throws ErrorException ForEach("no_wildcard.txt") do
            Step(:x, `echo`)
        end
        
        rm(dir; recursive=true)
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
end
