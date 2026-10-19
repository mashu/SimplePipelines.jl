@testset "Step construction" begin
    s = Step(:a, sh"echo a")
    @test s.name === :a && s.work == sh"echo a" && isempty(s.inputs) && isempty(s.outputs)
    @test Step(:b, "echo b").work == sh"echo b"                       # String is a shell command
    @test Step(`echo c`).work == `echo c`
    f = () -> 1
    @test Step(:f, f).work === f
    # path form: templates are rendered and functions bound to paths
    t = Step(:t, "sort {input} > {output}", ["in.txt"], ["out.txt"])
    @test t.work == sh"sort in.txt > out.txt"
    g = (i, o) -> nothing
    c = Step(:c, g, "in.txt", "out.txt")
    @test c.work isa SP.Call && c.work.args == ("in.txt", "out.txt")
    @test Step(:r, "x -t {threads}", String[], String[]; threads=4).work.script == "x -t 4"
    @test Step(:r, sh"echo", [], []).inputs == String[]
    # invalid work fails at construction, not at run time
    @test_throws ArgumentError Step(:bad, 42)
    @test_throws ArgumentError Step(:bad, sh"a" >> sh"b")
    @test_throws ArgumentError Step(:bad, "x", [1], [])
    @test_throws ArgumentError Step(:bad, "x"; threads=-1)
    @test_throws ErrorException Step(:bad, "awk '{print}' {input}", ["a"], [])
end

@testset "@step forms" begin
    a = @step a = sh"echo a"
    @test a.name === :a
    @test (@step sh"echo anon").work == sh"echo anon"
    b = @step b("in.txt" => "out.txt") = "cp {input} {output}"
    @test b.inputs == ["in.txt"] && b.outputs == ["out.txt"] && b.work == sh"cp in.txt out.txt"
    c = @step c(["a", "b"] => ["c"]; threads=2, mem_mb=10) = "cat {input} > {output} # {threads}"
    @test c.resources == SP.Resources(2, 10) && c.work.script == "cat a b > c # 2"
    d = @step d(; threads=3) = "x -t {threads}"
    @test d.work.script == "x -t 3"
    e = @step e("only_in.txt") = sh"test -f only_in.txt"
    @test e.inputs == ["only_in.txt"] && isempty(e.outputs)
    e2 = @step e2("x.txt", "y.txt") = sh"true"
    @test e2.inputs == ["x.txt", "y.txt"]
    n = 4
    k = @step k(; threads=n) = "x"
    @test k.resources.threads == 4
    # a call is deferred until the step runs; command builders run immediately
    calls = Ref(0)
    count_call() = (calls[] += 1; "counted")
    lazy = @step lazy = count_call()
    @test calls[] == 0 && lazy.work isa Function
    eager = @step eager = sh("echo " * "x")
    @test eager.work == sh"echo x"
    @test (@step p = pipeline(`echo a`, `cat`)).work isa Base.AbstractCmd
    # wildcard paths make a Rule
    @test (@step r("raw/{s}.fq" => "out/{s}.bam") = "cp {input} {output}") isa Rule
    @test (@step g(gather("bam/{s}.bam") => "all.txt") = "cat {input} > {output}") isa Rule
    @test_throws LoadError @eval @step x(a => b) = (sh"a", sh"b")
end

@testset "labels" begin
    @test SP.step_label(Step(:a, sh"x")) == "a"
    @test SP.step_label(Step(sh"echo hello")) == "echo hello"
    @test SP.step_label(Step(:a, sh"x"), "s=A") == "a[s=A]"
    long = Step(sh("echo " * repeat("x", 100)))
    @test endswith(SP.step_label(long), "…")
    @test SP.step_label(Step(() -> 1)) == "julia function"
end

@testset "composition operators" begin
    a, b, c = (Step(Symbol(x), sh"true") for x in "abc")
    @test (a >> b) isa Sequence
    @test length((a >> b >> c).nodes) == 3                  # flattened
    @test length((a >> (b >> c)).nodes) == 3
    @test length(((a >> b) >> (b >> c)).nodes) == 4
    @test (a & b) isa Parallel && length((a & b & c).nodes) == 3
    @test (a | b) isa Fallback
    @test (a^3) isa Retry && (a^3).max_attempts == 3
    f = () -> 1
    @test (a >> f) isa Sequence && (f >> a) isa Sequence
    @test (sh"x" >> sh"y") isa Sequence && (sh"x" & sh"y") isa Parallel
    @test (sh"x" | f) isa Fallback && (sh"x"^2) isa Retry
    @test (a >> "echo s") isa Sequence
    @test (`echo a` >> a) isa Sequence
    # `>>` binds tighter than `&`: parenthesize parallel groups
    @test (a & b >> c) isa Parallel
    @test ((a & b) >> c) isa Sequence
    # no type piracy: Base behaviour of Cmd and Function operands is untouched
    @test (`echo a` & `echo b`) isa Base.AndCmds
    @test_throws MethodError (() -> 1) >> (() -> 2)
    @test_throws MethodError `echo a` >> `echo b`
    # constructors
    @test length(Sequence(a, b, c).nodes) == 3
    @test length(Parallel([a, b]).nodes) == 2
    @test Parallel(sh"x", "y").nodes[2] isa Step
    @test_throws ArgumentError Retry(a, 0)
    @test_throws ArgumentError Timeout(a, 0)
    @test_throws ArgumentError Timeout(a, -1.0)
    @test_throws ArgumentError Sequence(a, 42)
    @test Pipeline(a; name="x").name == "x"
end

@testset "steps and count_steps" begin
    a, b, c = (Step(Symbol(x), sh"true") for x in "abc")
    dag = (a >> b) & (a >> c)
    @test steps(dag) == [a, b, c]
    @test count_steps(dag) == 3
    @test count_steps(Retry(a | b, 2)) == 2
    @test count_steps(ForEach(x -> sh"true", [1, 2])) == 0          # lazy
end
