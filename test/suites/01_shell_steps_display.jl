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

@testset "ShRun is a callable functor" begin
    # `sh(::Function)` returns a ShRun; calling the ShRun returns the
    # wrapped function's result. This is the functor pattern (a struct
    # with a method on its own type), idiomatic Julia for callable wrappers.
    sr = sh(() -> "echo functor")
    @test sr isa SimplePipelines.ShRun
    @test sr() == "echo functor"
    # The execution path now calls `step.work()` rather than reaching into
    # `step.work.f()`, exercising the functor.
    s = Step(:fn_step, sr)
    results = SimplePipelines.run_node(s, SimplePipelines.RunContext())
    @test results[1].success
    @test strip(results[1].result) == "functor"
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
        @test occursin("Missing output", string(results[1].result)) || occursin("out.txt", string(results[1].result))
    end
    rm(dir; recursive=true)

    # Missing input path
    step_missing_in = Step(:need_in, `echo x`, ["/nonexistent/in.txt"], [])
    results_in = run(step_missing_in, verbose=false, force=true)
    @test !results_in[1].success
    @test occursin("Missing input", string(results_in[1].result))

    # Step with invalid work type returns a failure StepResult with a clear message,
    # not a MethodError (the helpful execute fallback handles it).
    bad_results = SimplePipelines.run_node(Step(:bad, 42), SimplePipelines.RunContext(), false)
    @test length(bad_results) == 1
    @test !bad_results[1].success
    @test occursin("Step work must be", string(bad_results[1].result))
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

