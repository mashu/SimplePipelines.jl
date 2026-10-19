@testset "shell commands" begin
    s = sh"echo $HOME | wc -c"
    @test s isa SP.Shell
    @test s.script == "echo \$HOME | wc -c"       # no Julia interpolation
    x = "A"
    @test sh("echo $(x)").script == "echo A"
    @test SP.shell_quote("raw/A_R1.fq.gz") == "raw/A_R1.fq.gz"
    @test SP.shell_quote("my file.txt") == "'my file.txt'"
    @test SP.shell_quote("it's") == "'it'\\''s'"
    @test SP.shell_quote("") == "''"
    @test first(SP.command(s).exec) in (something(Sys.which("bash"), "sh"), "sh")
end

@testset "patterns: match and substitute" begin
    p = SP.Pattern("data/{kind}/{id}.fq")
    @test SP.wildcard_names(p) == ["kind", "id"]
    @test SP.match_path(p, "data/raw/s01.fq") == Dict("kind" => "raw", "id" => "s01")
    @test SP.match_path(p, "data/raw/sub/s01.fq") === nothing      # one segment per wildcard
    @test SP.match_path(p, "other/raw/s01.fq") === nothing
    # regex metacharacters in literals are literal
    q = SP.Pattern("out/{s}.(v1)+.txt")
    @test SP.match_path(q, "out/A.(v1)+.txt") == Dict("s" => "A")
    @test SP.match_path(q, "out/A.v1.txt") === nothing
    # a repeated wildcard must capture the same value
    r = SP.Pattern("{s}/{s}.bam")
    @test SP.match_path(r, "A/A.bam") == Dict("s" => "A")
    @test SP.match_path(r, "A/B.bam") === nothing
    @test SP.substitute("out/{id}_{kind}.bam", Dict("id" => "s1", "kind" => "raw")) == "out/s1_raw.bam"
    @test_throws ErrorException SP.substitute("out/{missing}.bam", Dict("id" => "s1"))
    @test SP.bind_known("bam/{s}_{r}.bam", Dict("s" => "A")) == "bam/A_{r}.bam"
end

@testset "discover" begin
    in_tempdir() do _
        mkpath("raw/p1"); mkpath("raw/p2")
        write("raw/p1/A_R1.fq", ""); write("raw/p1/B_R1.fq", ""); write("raw/p2/C_R1.fq", "")
        write("raw/p1/A_R2.fq", ""); write("raw/p1/.hidden_R1.fq", "")
        found = discover("raw/{project}/{sample}_R1.fq")
        @test [(w.project, w.sample) for w in found] == [("p1", "A"), ("p1", "B"), ("p2", "C")]
        @test found[1]["sample"] == "A"
        @test haskey(found[1], :project)
        @test isempty(discover("nothing/{x}.txt"))
        @test length(discover("./raw/p1/{s}_R1.fq")) == 2
        @test length(discover(joinpath(pwd(), "raw", "p2", "{s}_R1.fq"))) == 1   # absolute
    end
end

@testset "expand" begin
    @test expand("out/{s}.bam"; s=["A", "B", "C"]) == ["out/A.bam", "out/B.bam", "out/C.bam"]
    @test expand("{s}_R{r}.fq"; s=["A", "B"], r=1:2) == ["A_R1.fq", "A_R2.fq", "B_R1.fq", "B_R2.fq"]
    @test expand("{s}_R{r}.fq"; r=1:2, s=["A", "B"]) == ["A_R1.fq", "A_R2.fq", "B_R1.fq", "B_R2.fq"]
    @test expand(["raw/{s}.fq", "qc/{s}.html"]; s=["A", "B"]) == ["raw/A.fq", "raw/B.fq", "qc/A.html", "qc/B.html"]
    @test expand("x/{s}.txt"; s="A") == ["x/A.txt"]                 # a string is one value
    @test expand("static.txt") == ["static.txt"]
    @test expand("{s}_R{r}.fq", (s=["A"], r=[1, 2])) == ["A_R1.fq", "A_R2.fq"]
    @test_throws ErrorException expand("{missing}.txt"; other=["x"])
    @test_throws ErrorException expand("{s}.txt"; s=["A"], smaple=["B"])   # typo is an error
end

@testset "shell templates" begin
    ctx = SP.TemplateContext([["a.fq"], ["b 1.fq", "b2.fq"]], ["out.bam"], Dict("sample" => "S 1"), 4, 100)
    @test SP.render("tool {input} > {output}", ctx) == "tool a.fq 'b 1.fq' b2.fq > out.bam"
    @test SP.render("{input[2]} {output[1]} -t {threads} -m {mem_mb}", ctx) == "'b 1.fq' b2.fq out.bam -t 4 -m 100"
    @test SP.render("--rg {sample}", ctx) == "--rg 'S 1'"
    @test SP.render("awk '{{print}}' \${HOME} {x y}", ctx) == "awk '{print}' \${HOME} {x y}"
    @test_throws ErrorException SP.render("awk '{print}'", ctx)
    @test_throws ErrorException SP.render("{input[3]}", ctx)
    @test_throws ErrorException SP.render("{threads[1]}", ctx)
end
