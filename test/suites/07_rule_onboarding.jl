@testset "rule check reports wildcard shape" begin
    rule = @rule align("raw/{sample}.fq" => "out/{sample}.bam") =
        "bwa mem ref.fa {input} > {output}"
    c = check(rule)
    @test c.rule === rule
    @test c.wildcards == ["sample"]
    @test c.placeholders == ["{input}", "{output}"]

    io = IOBuffer()
    show(io, MIME("text/plain"), c)
    text = String(take!(io))
    @test occursin("Rule check: align", text)
    @test occursin("raw/{sample}.fq", text)
    @test occursin("{output}", text)
end

@testset "rule check instantiates one target" begin
    rule = @rule align("raw/{sample}.fq" => "out/{sample}.bam") =
        "bwa mem ref.fa {input} > {output}"
    c = check(rule, "out/A.bam")
    @test c.target == "out/A.bam"
    @test c.wildcards == Dict("sample" => "A")
    @test c.inputs == ["raw/A.fq"]
    @test c.outputs == ["out/A.bam"]
    @test c.command == "bwa mem ref.fa raw/A.fq > out/A.bam"

    @test_throws ErrorException check(rule, "qc/A.html")
end

@testset "function rule check does not execute work" begin
    called = Ref(false)
    rule = @rule dynamic("raw/{sample}.fq" => "out/{sample}.txt") =
        (inputs, outputs, wildcards) -> begin
            called[] = true
            "echo $(wildcards["sample"]) > $(outputs[1])"
        end
    c = check(rule, "out/A.txt")
    @test !called[]
    @test isempty(c.command)
    @test occursin("would receive", c.function_note)
end

@testset "@targets expands wildcard targets directly" begin
    @test (@targets "out/{sample}.bam" sample=["A", "B"]) ==
          ["out/A.bam", "out/B.bam"]
    @test (@targets ["raw/{sample}.fq", "qc/{sample}.html"] sample=["A", "B"]) ==
          ["raw/A.fq", "raw/B.fq", "qc/A.html", "qc/B.html"]
end

@testset "@workflow block collects rules and targets" begin
    wf = @workflow "rnaseq" begin
        @rule source([] => "raw/{sample}.fq") =
            "mkdir -p raw && echo {sample} > {output}"

        @rule align("raw/{sample}.fq" => "out/{sample}.bam") =
            "mkdir -p out && cp {input} {output}"

        @targets "out/{sample}.bam" sample=["A", "B"]
    end

    @test wf isa Workflow
    @test wf.name == "rnaseq"
    @test length(wf.rules) == 2
    @test wf.targets == ["out/A.bam", "out/B.bam"]

    e = explain(wf; target="out/A.bam")
    @test e.target == "out/A.bam"
    @test [s.rule.name for s in e.steps] == [:source, :align]
    @test e.steps[end].dependencies == ["raw/A.fq"]

    io = IOBuffer()
    show(io, MIME("text/plain"), e)
    text = String(take!(io))
    @test occursin("Workflow explanation", text)
    @test occursin("Rule: align", text)
    @test occursin("sample => A", text)
end

@testset "@workflow rejects unsupported entries" begin
    @test_throws LoadError begin
        @eval @workflow "bad" begin
            x = 1
        end
    end
end
