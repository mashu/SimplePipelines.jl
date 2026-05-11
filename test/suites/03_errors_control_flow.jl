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
    @test contains(string(results[1].result), "Missing input")
    
    # Function step with missing input file
    s_func = Step(:func_missing, () -> "test", ["nonexistent_file_12345.txt"], String[])
    results_func = run(s_func, verbose=false)
    @test !results_func[1].success
    @test contains(string(results_func[1].result), "Missing input")
end

@testset "Error handling - command failure" begin
    # Command that fails
    s = @step fail = `false`
    results = run(s, verbose=false)
    @test !results[1].success
    @test contains(string(results[1].result), "Error")
end

@testset "Error handling - function throws" begin
    # Function that throws
    s = Step(:throws, () -> error("intentional error"))
    results = run(s, verbose=false)
    @test !results[1].success
    @test contains(string(results[1].result), "intentional error")
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

    # @branch expands to the same run-time behaviour as Branch(() -> ...)
    bmacro = @branch true `echo macro_yes` `echo macro_no`
    rm = run(bmacro, verbose=false, force=true)
    @test rm[1].success
    @test contains(rm[1].result, "macro_yes")
    bmacro2 = @branch flag[] true_branch false_branch
    flag[] = true
    @test contains(run(bmacro2, verbose=false, force=true)[1].result, "true path")
    flag[] = false
    @test contains(run(bmacro2, verbose=false, force=true)[1].result, "false path")
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

