@testset "ForEach over a collection" begin
    in_tempdir() do _
        fe = ForEach(["a", "b", "c"]) do x
            @step echo = sh("echo $x")
        end
        @test fe isa ForEach && count_steps(fe) == 0         # lazy
        r = quiet(fe)
        @test r.value == ["a\n", "b\n", "c\n"]
        @test sort(labels(r)) == ["echo[a]", "echo[b]", "echo[c]"]
        r = quiet(ForEach(1:4) do i; @step sq = () -> i^2; end >> sum)
        @test r.value == 30
        # the block may return commands or functions directly
        @test quiet(ForEach(x -> sh("echo $x"), [1])).value == ["1\n"]
        @test quiet(ForEach(x -> (() -> x * 2), [1, 2])).value == [2, 4]
        # an empty collection is fine
        r = quiet(ForEach(x -> sh"true", String[]))
        @test r.success && isempty(r.value)
        # a block that returns nothing is a reported failure, not an exception
        r = quiet(ForEach(x -> nothing, [1]))
        @test !r.success && occursin("forget to return", r[1].result.message)
        # non-string items get index tags
        r = quiet(ForEach(x -> sh"true", [(1, 2)]))
        @test labels(r) == ["true[#1]"]
    end
end

@testset "ForEach over files" begin
    in_tempdir() do _
        mkpath("fastq")
        for s in ["d1", "d2", "d3"]
            write("fastq/$(s)_R1.fq.gz", s)
        end
        fe = ForEach("fastq/{donor}_R1.fq.gz") do donor
            @step count = sh("cat fastq/$(donor)_R1.fq.gz")
        end
        r = quiet(fe)
        @test r.success && r.value == ["d1", "d2", "d3"]
        @test sort(labels(r)) == ["count[donor=d1]", "count[donor=d2]", "count[donor=d3]"]
        # several wildcards arrive as several arguments
        mkpath("p/A"); mkpath("p/B")
        write("p/A/s1.csv", ""); write("p/A/s2.csv", ""); write("p/B/s3.csv", "")
        r = quiet(ForEach("p/{project}/{sample}.csv") do project, sample
            @step tag = () -> "$project-$sample"
        end)
        @test r.value == ["A-s1", "A-s2", "B-s3"]
        # discovery happens when the pipeline runs, so it sees files made just before
        make = @step make([] => "late/x.txt") = "echo x > {output}"
        r = quiet(make >> ForEach("late/{name}.txt") do name; @step seen = () -> name; end)
        @test r.value == ["x"]
        # no match is a failure (usually a wrong path)
        r = quiet(ForEach("nothing/{x}.txt") do x; sh"true"; end)
        @test !r.success && r[1].result.kind === :no_matches
        @test_throws ArgumentError ForEach(x -> x, "no_wildcard.txt")
    end
end
