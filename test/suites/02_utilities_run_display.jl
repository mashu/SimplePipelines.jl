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

        # Inputs only (no declared outputs): not fresh if an input disappears,
        # even when the step hash remains in persisted state.
        write("only_in.txt", "x")
        step_in_only = @step lint(["only_in.txt"]) = sh"test -f only_in.txt"
        @test !is_fresh(step_in_only)
        run(step_in_only, verbose=false, force=true)
        @test is_fresh(step_in_only)
        rm("only_in.txt")
        @test !is_fresh(step_in_only)
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

