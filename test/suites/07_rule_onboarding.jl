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
    @test occursin("Template check: align", text)
    @test occursin("raw/{sample}.fq", text)
    @test occursin("{output}", text)
    @test occursin("discovery:", text)
    @test occursin("No runnable matches", text)
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

@testset "check(rule) lists disk previews without manual target" begin
    dir = mktempdir()
    try
        cd(dir) do
            mkpath("tsv")
            write("tsv/A.tsv", "x\n")
            write("tsv/B.tsv", "y\n")
            r = @rule filt("tsv/{sample}.tsv" => "out/{sample}.tsv") =
                "cp {input} {output}"
            c = check(r; limit=10)
            @test length(c.instantiations) == 2
            targets = Set(String(i.target) for i in c.instantiations)
            @test targets == Set(["out/A.tsv", "out/B.tsv"])
            inputs = Set(String(only(i.inputs)) for i in c.instantiations)
            @test inputs == Set(["tsv/A.tsv", "tsv/B.tsv"])
            io = IOBuffer()
            show(io, MIME("text/plain"), c)
            text = String(take!(io))
            @test occursin("previews:", text)
            @test occursin("preview 1", text)
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "check(rule; limit) truncates many disk matches" begin
    dir = mktempdir()
    try
        cd(dir) do
            mkpath("tsv")
            for i in 1:25
                write("tsv/sample_$i.tsv", "x\n")
            end
            r = @rule many("tsv/{sample}.tsv" => "out/{sample}.tsv") = "cp {input} {output}"
            c = check(r; limit=3)
            @test length(c.instantiations) == 3
            @test occursin("25 runnable", c.discovery_note)
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "@targets expands wildcard targets directly" begin
    @test (@targets "out/{sample}.bam" sample=["A", "B"]) ==
          ["out/A.bam", "out/B.bam"]
    @test (@targets ["raw/{sample}.fq", "qc/{sample}.html"] sample=["A", "B"]) ==
          ["raw/A.fq", "raw/B.fq", "qc/A.html", "qc/B.html"]
end

@testset "@workflow block collects rules and targets" begin
    wf = @workflow "rnaseq" begin
        @step source([] => "raw/{sample}.fq") =
            "mkdir -p raw && echo {sample} > {output}"

        @step align("raw/{sample}.fq" => "out/{sample}.bam") =
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

@testset "@step wildcard template discovers inputs" begin
    dir = mktempdir()
    try
        cd(dir) do
            mkpath("tsv")
            mkpath("out")
            write("tsv/A.tsv", "id\tV_errors\n1\t0\n2\t2\n")
            write("tsv/B.tsv", "id\tV_errors\n3\t1\n4\t3\n")

            filter_v = @step filter_v("tsv/{sample}.tsv" => "out/{sample}.tsv") =
                function(input_file, output_file)
                    kept = String[]
                    for line in eachline(input_file)
                        parts = split(line, '\t')
                        if parts[1] == "id" || parse(Int, parts[2]) <= 1
                            push!(kept, line)
                        end
                    end
                    write(output_file, join(kept, "\n") * "\n")
                    output_file
                end

            @test filter_v isa Rule
            c = check(filter_v, "out/A.tsv")
            @test c.inputs == ["tsv/A.tsv"]
            @test c.outputs == ["out/A.tsv"]

            results = run(filter_v, verbose=false, force=true)
            @test all(r -> r.success, results)
            @test read("out/A.tsv", String) == "id\tV_errors\n1\t0\n"
            @test read("out/B.tsv", String) == "id\tV_errors\n3\t1\n"
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "run(rule; wildcards_filter) skips discovery matches" begin
    dir = mktempdir()
    try
        cd(dir) do
            mkpath("tsv")
            mkpath("out")
            write("tsv/filtered-sampleA_45_1.tsv.gz", "id\tV_errors\tJ_errors\n1\t0\t0\n")
            write("tsv/filtered-sampleA_45_101.tsv.gz", "id\tV_errors\tJ_errors\n2\t0\t0\n")

            r = @step copy_tsv(
                "tsv/filtered-{donor}_{mid}_{rep}.tsv.gz" =>
                    "out/filtered-{donor}_{mid}_{rep}.tsv.gz",
            ) = function (input_file, output_file)
                mkpath(dirname(output_file))
                write(output_file, read(input_file, String))
                output_file
            end

            results = run(
                r;
                verbose=false,
                force=true,
                wildcards_filter = wc -> parse(Int, wc["rep"]) < 100,
            )
            @test all(r -> r.success, results)
            @test isfile("out/filtered-sampleA_45_1.tsv.gz")
            @test !isfile("out/filtered-sampleA_45_101.tsv.gz")

            c = check(r; wildcards_filter = wc -> parse(Int, wc["rep"]) < 100)
            @test length(c.instantiations) == 1
            @test only(c.instantiations).target == "out/filtered-sampleA_45_1.tsv.gz"
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "piped rules build per-wildcard DAGs" begin
    dir = mktempdir()
    try
        cd(dir) do
            mkpath("raw")
            write("raw/sampleA_1.txt", "hello")
            write("raw/sampleA_101.txt", "skip")
            mkpath("out")
            write("out/collapsed-sampleA_1.txt", "stale")

            filter_text = @step filter_text(
                "raw/{sample}_{rep}.txt" => "mid/filtered-{sample}_{rep}.txt",
            ) = function (input_file, output_file)
                mkpath(dirname(output_file))
                write(output_file, uppercase(read(input_file, String)))
                output_file
            end

            collapse_text = @step collapse_text(
                "mid/filtered-{sample}_{rep}.txt" => "out/collapsed-{sample}_{rep}.txt",
            ) = function (input_file, output_file)
                mkpath(dirname(output_file))
                write(output_file, read(input_file, String) * "!")
                output_file
            end

            chain = filter_text |> collapse_text
            @test chain isa RuleChain

            results = run(
                chain;
                verbose=false,
                force=true,
                wildcards_filter = wc -> parse(Int, wc["rep"]) < 100,
            )
            @test all(r -> r.success, results)
            @test read("mid/filtered-sampleA_1.txt", String) == "HELLO"
            @test read("out/collapsed-sampleA_1.txt", String) == "HELLO!"
            @test !isfile("mid/filtered-sampleA_101.txt")
            @test !isfile("out/collapsed-sampleA_101.txt")
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "rule pipe accepts function sink" begin
    dir = mktempdir()
    try
        cd(dir) do
            mkpath("raw")
            write("raw/a.txt", "one")
            write("raw/b.txt", "two")

            copy_text = @step copy_text("raw/{sample}.txt" => "out/{sample}.txt") =
                function (input_file, output_file)
                    mkpath(dirname(output_file))
                    write(output_file, read(input_file, String))
                    output_file
                end

            summarize_outputs = outputs -> begin
                paths = sort!(reduce(vcat, outputs))
                write("summary.txt", join(paths, "\n") * "\n")
                "summary.txt"
            end

            pipe = copy_text |> summarize_outputs
            @test pipe isa RuleValuePipe

            results = run(pipe; verbose=false, force=true)
            @test all(r -> r.success, results)
            @test read("summary.txt", String) == "out/a.txt\nout/b.txt\n"
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "@step concrete paths remain Step" begin
    concrete = @step concrete("input.tsv" => "output.tsv") = input -> input
    @test concrete isa Step
    @test concrete.inputs == ["input.tsv"]
    @test concrete.outputs == ["output.tsv"]
end

@testset "@rule function supports single input and output work" begin
    dir = mktempdir()
    try
        cd(dir) do
            mkpath("in")
            write("in/A.txt", "hello")
            r = @rule copy_text("in/{sample}.txt" => "out/{sample}.txt") =
                function(input_file, output_file)
                    mkpath(dirname(output_file))
                    write(output_file, uppercase(read(input_file, String)))
                    output_file
                end

            results = run(r, verbose=false, force=true)
            @test all(r -> r.success, results)
            @test read("out/A.txt", String) == "HELLO"
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end
