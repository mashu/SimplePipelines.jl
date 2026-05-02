using SimplePipelines
using Test

# Clear any persisted state before running tests
clear_state!()

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
            result = SimplePipelines.run_node(step, SimplePipelines.RunContext())
            @test result[1].success
            @test isfile("sh_test_out.txt") && strip(read("sh_test_out.txt", String)) == "test"
        end
        rm(dir; recursive=true)
    end

    @testset "sh_pipe: OS-pipe folding" begin
        # sh_pipe folds adjacent shell commands into one OS-level pipeline; only
        # the final stage's stdout is captured.
        p = sh_pipe(sh"echo hello world", sh"tr a-z A-Z")
        @test p isa Base.AbstractCmd
        @test !(p isa Cmd)               # OrCmds, not a flat Cmd

        # Step{<:AbstractCmd} runs the pipeline and captures the tail.
        step = Step(:upper, p)
        results = SimplePipelines.run_node(step, SimplePipelines.RunContext())
        @test results[1].success
        @test strip(results[1].result) == "HELLO WORLD"

        # @step recognises sh_pipe (no thunking) and produces a Step{<:AbstractCmd}.
        s = @step pipe_inline = sh_pipe(sh"printf 'x\ny\nz\n'", sh"sort -r")
        @test s.work isa Base.AbstractCmd
        results2 = run(s, verbose=false, force=true)
        @test results2[1].success
        @test strip(results2[1].result) == "z\ny\nx"

        # With declared inputs/outputs the path checks still apply at the Step
        # boundary (not between pipeline stages).
        dir = mktempdir()
        try
            cd(dir) do
                write("in.txt", "b\na\nc\n")
                stp = @step sorter(["in.txt"] => ["out.txt"]) =
                    sh_pipe(sh"cat in.txt", sh"sort > out.txt")
                results3 = run(stp, verbose=false, force=true)
                @test results3[1].success
                @test isfile("out.txt")
                @test read("out.txt", String) == "a\nb\nc\n"
            end
        finally
            rm(dir; recursive=true, force=true)
        end

        # The synthetic Step{<:AbstractCmd} also slots into >> / & via node_operand.
        @test (sh_pipe(sh"echo a", sh"cat") >> sh"echo done") isa Sequence
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
        results = run(s, verbose=false, force=true)
        
        @test length(results) == 1
        @test results[1].success == true
        @test contains(results[1].result, "hello world")
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
        results = run(s, verbose=false, force=true)
        
        @test results[1].success
        @test counter[] == 1
        
        # Function returning nothing (output stored as-is, not coerced to "")
        f_nothing = () -> nothing
        s_nothing = Step(:nothing_func, f_nothing)
        results_nothing = run(s_nothing, verbose=false, force=true)
        @test results_nothing[1].success
        @test results_nothing[1].result === nothing
    end

    @testset "Execute: path checks and error paths" begin
        # Missing output path: step declares output but command does not create it
        dir = mktempdir()
        cd(dir) do
            step = @step bad_out(["in.txt"] => ["out.txt"]) = sh"echo x"
            write("in.txt", "x")
            results = run(step, verbose=false, force=true)
            @test !results[1].success
            @test occursin("Missing output", results[1].result) || occursin("out.txt", results[1].result)
        end
        rm(dir; recursive=true)

        # Missing input path
        step_missing_in = Step(:need_in, `echo x`, ["/nonexistent/in.txt"], [])
        results_in = run(step_missing_in, verbose=false, force=true)
        @test !results_in[1].success
        @test occursin("Missing input", results_in[1].result)

        # Step with invalid work type returns a failure StepResult with a clear message,
        # not a MethodError (the helpful execute fallback handles it).
        bad_results = SimplePipelines.run_node(Step(:bad, 42), SimplePipelines.RunContext(), false)
        @test length(bad_results) == 1
        @test !bad_results[1].success
        @test occursin("Step work must be", bad_results[1].result)
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

    @testset "Coverage: verbose log paths and colored DAG" begin
        # Run with verbose=true to hit log_skip (fresh step)
        clear_state!()
        s = @step cov_skip = `echo ok`
        run(s, verbose=true, force=true)
        run(s, verbose=true)  # second run skips -> log_skip
        clear_state!()

        # Verbose retry (log_retry)
        r = Retry(`false` | `echo fallback`, 2)
        run(r, verbose=true)

        # Verbose fallback (log_fallback when primary fails)
        f = `false` | `echo backup`
        run(f, verbose=true)

        # Verbose + sh(fn) to hit log_cmd(Verbose(), ::String)
        s_sh = @step with_sh = sh(() -> "echo from_sh")
        run(s_sh, verbose=true, force=true)

        # Verbose branch (log_branch)
        b = Branch(() -> true, Step(`echo yes`), Step(`echo no`))
        run(b, verbose=true)
        b2 = Branch(() -> false, Step(`echo yes`), Step(`echo no`))
        run(b2, verbose=true)

        # Verbose timeout (log_timeout)
        t = Timeout(Step(`echo done`), 5.0)
        run(t, verbose=true)

        # Verbose reduce (log_reduce)
        red = Reduce(join, `echo a` & `echo b`)
        run(red, verbose=true)

        # Verbose force (log_force)
        fr = Force(Step(`echo forced`))
        run(fr, verbose=true)

        # DAG with unnamed steps (step_label shows command)
        dag = `echo a` >> (`echo b` & `echo c`) >> `echo d`
        io = IOBuffer()
        print_dag(io, dag, 0)
        out = String(take!(io))
        @test occursin("echo", out)
        # Long command triggers work_label truncation (exec > 3 parts: "echo a b …")
        long_step = Step(`echo a b c d e`)
        io2 = IOBuffer()
        print_dag(io2, long_step, 0)
        out2 = String(take!(io2))
        @test occursin("…", out2) || occursin("echo", out2)
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
        
        # Just call print_dag to exercise code path (color=true to stdout)
        print_dag(dag)
        # print_dag(io, node, 0) uses color=false internally
        io = IOBuffer()
        print_dag(io, dag, 0)
        @test length(String(take!(io))) > 0
        @test true  # If we get here, it worked
    end

    @testset "is_fresh and clear_state!" begin
        clear_state!()
        # Step with no file deps: freshness is state-based
        s = @step nofiles = `echo ok`
        @test !is_fresh(s)
        run(s, verbose=false, force=true)
        @test is_fresh(s)
        # State roundtrip: run writes state, load_state reads it
        @test !isempty(SimplePipelines.load_state())
        # State file exists with valid layout (header + at least one hash slot)
        @test isfile(SimplePipelines.STATE_FILE[])
        @test filesize(SimplePipelines.STATE_FILE[]) >= 16
        # clear_state! removes state so step is no longer fresh
        clear_state!()
        @test !is_fresh(s)

        # File-based freshness: inputs and outputs
        dir = mktempdir()
        cd(dir) do
            write("in.txt", "x")
            step = @step process(["in.txt"] => ["out.txt"]) = sh"cp in.txt out.txt"
            @test !is_fresh(step)  # out.txt missing
            run(step, verbose=false, force=true)
            @test is_fresh(step)   # out exists and newer than in
            # Touch input so it's newer than output -> not fresh
            run(`touch in.txt`)
            @test !is_fresh(step)
        end
        rm(dir; recursive=true)

        # load_state with invalid file returns empty set (skip on Windows: file can stay locked)
        if !Sys.iswindows()
            clear_state!()
            write(SimplePipelines.STATE_FILE[], "not-a-number\n")
            @test isempty(SimplePipelines.load_state())
            clear_state!()
        end
    end

    @testset "StateFormat and state persistence" begin
        SF = SimplePipelines.StateFormat
        L = SF.STATE_LAYOUT

        # Layout helpers
        @test SF.layout_file_size(L) == 16 + L.max_hashes * 8
        @test SF.layout_validate_count(L, UInt64(0))
        @test SF.layout_validate_count(L, UInt64(L.max_hashes))
        @test !SF.layout_validate_count(L, UInt64(L.max_hashes + 1))

        # Header read/write roundtrip
        io = IOBuffer()
        SF.layout_write_header(io, L, UInt64(42))
        seekstart(io)
        magic_ok, count = SF.layout_read_header(io, L)
        @test magic_ok
        @test count == UInt64(42)

        # Invalid header: wrong magic
        bad_io = IOBuffer(vcat(b"BADmagic", zeros(UInt8, 8)))
        seekstart(bad_io)
        magic_ok2, count2 = SF.layout_read_header(bad_io, L)
        @test !magic_ok2
        @test count2 == UInt64(0)

        # save_state! / load_state roundtrip in temp dir (avoid touching default .pipeline_state)
        dir = mktempdir()
        old_path = SimplePipelines.STATE_FILE[]
        try
            SimplePipelines.STATE_FILE[] = joinpath(dir, ".pipeline_state")
            SimplePipelines.save_state!(Set(UInt64[1, 2, 3]))
            loaded = SimplePipelines.load_state()
            @test loaded == Set(UInt64[1, 2, 3])
            @test length(loaded) == 3
        finally
            SimplePipelines.STATE_FILE[] = old_path
            rm(dir; recursive=true)
        end

        # load_state with missing file returns empty
        dir2 = mktempdir()
        old_path2 = SimplePipelines.STATE_FILE[]
        try
            SimplePipelines.STATE_FILE[] = joinpath(dir2, "nonexistent_state")
            @test !isfile(SimplePipelines.STATE_FILE[])
            @test isempty(SimplePipelines.load_state())
        finally
            SimplePipelines.STATE_FILE[] = old_path2
            rm(dir2; recursive=true)
        end
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

    @testset "Display (print_dag and MIME text/plain)" begin
        a = @step a = `echo a`
        b = @step b = `echo b`
        c = @step c = `echo c`

        # print_dag(io, node, indent) writes DAG to io
        io = IOBuffer()
        print_dag(io, a, 0)
        out = String(take!(io))
        @test occursin("a", out)
        @test !occursin("Sequence", out)

        seq = a >> b
        io = IOBuffer()
        print_dag(io, seq, 0)
        out = String(take!(io))
        @test occursin("Sequence", out)
        @test occursin("a", out)
        @test occursin("b", out)

        par = a & b
        io = IOBuffer()
        print_dag(io, par, 0)
        out = String(take!(io))
        @test occursin("Parallel", out)
        @test occursin("a", out)
        @test occursin("b", out)

        ret = a^2
        io = IOBuffer()
        print_dag(io, ret, 0)
        out = String(take!(io))
        @test occursin("Retry", out)
        @test occursin("a", out)

        fall = a | b
        io = IOBuffer()
        print_dag(io, fall, 0)
        out = String(take!(io))
        @test occursin("Fallback", out)
        @test occursin("a", out)
        @test occursin("b", out)
        @test occursin("├─", out)  # tree branches
        @test occursin("└─", out)

        br = Branch(() -> true, a, b)
        io = IOBuffer()
        print_dag(io, br, 0)
        out = String(take!(io))
        @test occursin("Branch", out)
        @test occursin("✓", out)  # true branch marker
        @test occursin("✗", out)  # false branch marker

        tmo = Timeout(a, 5.0)
        io = IOBuffer()
        print_dag(io, tmo, 0)
        out = String(take!(io))
        @test occursin("Timeout", out)
        @test occursin("5.0", out)
        @test occursin("a", out)

        red = Reduce((xs -> join(xs, "\n")), a & b)
        io = IOBuffer()
        print_dag(io, red, 0)
        out = String(take!(io))
        @test occursin("Reduce", out)
        @test occursin("Parallel", out)
        @test occursin("a", out)
        @test occursin("b", out)

        # show(io, MIME("text/plain"), node) uses print_dag
        p = Pipeline(seq, name="mytest")
        io = IOBuffer()
        show(io, MIME("text/plain"), p)
        out = String(take!(io))
        @test occursin("Pipeline", out)
        @test occursin("mytest", out)
        @test occursin("Sequence", out)
        @test occursin("a", out)
        @test occursin("b", out)

        io = IOBuffer()
        show(io, MIME("text/plain"), seq)
        out = String(take!(io))
        @test occursin("Sequence", out)
        @test occursin("a", out)
        @test occursin("b", out)

        # print_dag(node; indent=0) and print_dag(io, node, indent) entry points
        io = IOBuffer()
        print_dag(io, (a >> b) >> c, 0)
        out = String(take!(io))
        @test occursin("Sequence", out)
        @test occursin("a", out)
        @test occursin("b", out)
        @test occursin("c", out)
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
        @test contains(results[1].result, "Missing input")
        
        # Function step with missing input file
        s_func = Step(:func_missing, () -> "test", ["nonexistent_file_12345.txt"], String[])
        results_func = run(s_func, verbose=false)
        @test !results_func[1].success
        @test contains(results_func[1].result, "Missing input")
    end
    
    @testset "Error handling - command failure" begin
        # Command that fails
        s = @step fail = `false`
        results = run(s, verbose=false)
        @test !results[1].success
        @test contains(results[1].result, "Error")
    end
    
    @testset "Error handling - function throws" begin
        # Function that throws
        s = Step(:throws, () -> error("intentional error"))
        results = run(s, verbose=false)
        @test !results[1].success
        @test contains(results[1].result, "intentional error")
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

        # Retry actually re-executes after a failure (memo is invalidated
        # between attempts; without that, the second attempt would short-circuit
        # to the cached failure and never run the function again).
        attempts = Threads.Atomic{Int}(0)
        flaky = Step(:flaky, () -> begin
            n = Threads.atomic_add!(attempts, 1) + 1
            n < 3 && error("transient $n")
            "ok-on-attempt-$n"
        end)
        r4 = Retry(flaky, 5)
        results4 = run(r4, verbose=false, force=true)
        @test attempts[] == 3
        @test results4[end].success
        @test results4[end].result == "ok-on-attempt-3"
    end

    @testset "Shared failing step memoises (no concurrent re-execution)" begin
        # When a step fails and is reached from multiple branches in the same run,
        # the second visit must observe the cached failure — not start a duplicate
        # execution. This is the (pop, put) race that memoising failures fixes.
        runs = Threads.Atomic{Int}(0)
        flaky = Step(:flaky, () -> begin
            Threads.atomic_add!(runs, 1)
            error("nope")
        end)
        # Two parallel branches both touch `flaky` first.
        a = @step a = `echo a`
        b = @step b = `echo b`
        pipeline = (flaky >> a) & (flaky >> b)
        results = run(pipeline, verbose=false, force=true)
        @test runs[] == 1
        # Both branches saw the same failed StepResult instance via the in-flight
        # channel, so the result vector contains the same object twice.
        flakies = [r for r in results if r.step.name == :flaky]
        @test length(flakies) >= 1
        @test all(==(objectid(flakies[1])), [objectid(r) for r in flakies])
        @test all(r -> !r.success, flakies)
    end

    @testset "Fallback operator |" begin
        success_step = @step ok = `echo "primary"`
        fail_step = @step fail = `false`
        fallback_step = @step fallback = `echo "fallback"`
        
        # Primary succeeds - fallback not used
        f1 = success_step | fallback_step
        results1 = run(f1, verbose=false, force=true)
        @test length(results1) == 1
        @test results1[1].success
        @test contains(results1[1].result, "primary")
        
        # Primary fails - fallback used
        f2 = fail_step | fallback_step
        results2 = run(f2, verbose=false, force=true)
        @test results2[end].success
        @test contains(results2[end].result, "fallback")
        
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
        results1 = run(b, verbose=false, force=true)
        @test results1[1].success
        @test contains(results1[1].result, "true path")
        
        # Condition false
        flag[] = false
        results2 = run(b, verbose=false, force=true)
        @test results2[1].success
        @test contains(results2[1].result, "false path")
        
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
        results = run(pipeline, verbose=false, force=true)
        
        # Should have run flaky twice, then safe
        @test results[end].success
        @test contains(results[end].result, "safe")
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

        # Works with Function (node_operand then ^)
        f = () -> "ok"
        r3 = f^2
        @test r3 isa Retry
        @test r3.max_attempts == 2
        results_f = run(r3, verbose=false, force=true)
        @test results_f[1].success
        @test results_f[1].result == "ok"
        
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
        @test occursin("upstream", results_fail[3].result) || occursin("Reduce", results_fail[3].result)
        
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

        # Error on no matches when run (lazy: no match check at construction)
        cd(dir) do
            pipeline = ForEach("{x}_nonexistent.xyz") do x
                Step(:x, `echo`)
            end
            @test_throws ErrorException run(pipeline, verbose=false)
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
                @test all(r -> r.success || (r.result !== nothing && contains(r.result, "up to date")), results2)

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
end

# Documenter `jldoctest` blocks in `docs/src/` (see `doctests.md`). Spawn a fresh Julia with a
# normal `JULIA_LOAD_PATH` (the `Pkg.test` sandbox hides `Pkg` from the parent process).
@testset "Documentation (Documenter doctest :only)" begin
    root = dirname(@__DIR__)
    docs = joinpath(root, "docs")
    make_jl = joinpath(docs, "make.jl")
    code = string(
        "import Pkg; ",
        "Pkg.activate(", repr(docs), "); ",
        "Pkg.develop(path=", repr(root), "); ",
        "Pkg.instantiate(); ",
        "ENV[\"DOCUMENTER_DOCTEST\"]=\"only\"; ",
        "include(", repr(make_jl), ")",
    )
    cmd = `$(Base.julia_cmd()) -e $(code)`
    ok = withenv("JULIA_LOAD_PATH" => "@:@stdlib") do
        success(run(cmd))
    end
    @test ok
end
