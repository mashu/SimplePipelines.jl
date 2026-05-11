@testset "run report callback" begin
    seen = Ref{Union{Nothing,Tuple{Vector{SimplePipelines.AbstractStepResult},String}}}(nothing)
    run(Pipeline(@step x = `true`); verbose=false, force=true,
        report=(res; pipeline) -> (seen[] = (res, pipeline)))
    @test seen[] !== nothing
    res, name = seen[]
    @test res[1].success
    @test name == "pipeline"
end

@testset "Branch condition receives upstream context" begin
    left = @step emit = () -> "hello"
    okstep = Step(:ok, (x -> uppercase(String(x))))
    badstep = @step bad = `false`
    right = Branch((ctx) -> length(ctx) > 2, okstep, badstep)
    r = run(left >> right, verbose=false, force=true)
    @test r[1].success && r[2].success
    @test r[2].result == "HELLO"
end

@testset "Timeout StepFailure kinds" begin
    # run_node throws before children (not caught by per-step run_safely) → :inner_exception
    sa = @step a = `true`
    sb = @step b = `true`
    inner_branch = Branch(() -> error("cond"), sa, sb)
    r1 = run(Timeout(inner_branch, 10.0), verbose=false, force=true)
    @test !r1[1].success
    @test r1[1].result isa StepFailure
    @test r1[1].result.kind == :inner_exception

    slow = @step zzz = sh"sleep 120"
    r2 = run(Timeout(slow, 0.2), verbose=false, force=true)
    @test !r2[1].success
    @test r2[1].result isa StepFailure
    @test r2[1].result.kind == :timed_out
end

@testset "materialize_table extension (CSV + DataFrames)" begin
    using CSV
    using DataFrames
    dir = mktempdir()
    try
        path = joinpath(dir, "t.csv")
        write(path, "a,b\n1,2\n")
        df = materialize_table(FilePath(path))
        @test df isa DataFrame
        @test nrow(df) == 1 && df[1, :a] == 1
    finally
        rm(dir; recursive=true, force=true)
    end
end
