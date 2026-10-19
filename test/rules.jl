@testset "Rule validation" begin
    r = @rule align("raw/{s}.fq" => "bam/{s}.bam"; threads=4) = "bwa -t {threads} {input} > {output}"
    @test r isa Rule && r.name === :align && SP.rule_wildcards(r) == ["s"]
    @test r.resources.threads == 4
    # misspelled wildcard
    err = try; @rule bad("raw/{smaple}.fq" => "out/{sample}.bam") = "cp {input} {output}"; catch e; e; end
    @test err isa ArgumentError && occursin("{smaple}", err.msg) && occursin("gather", err.msg)
    # outputs must agree on wildcards
    @test_throws ArgumentError @rule bad2("in/{a}" => ["out/{a}", "out/{a}_{b}"]) = "x"
    @test_throws ArgumentError @rule bad3("in/{input}" => "out/{input}") = "x"
    @test_throws ArgumentError @rule bad4([] => []) = "x"
    @test_throws SP.TemplateError @rule bad5("in/{s}" => "out/{s}") = "awk '{print}' {input}"
    @test_throws SP.TemplateError @rule bad6("in/{s}" => "out/{s}") = "tool {input[2]}"
    @test_throws ArgumentError @rule bad7("in/{s}" => "out/{s}") = 42
    # gather may introduce wildcards that the outputs lack
    m = @rule merge(gather("bam/{s}.bam") => "all.vcf") = "cat {input} > {output}"
    @test m.inputs[1].gather
end

@testset "Workflow: automatic targets from source files" begin
    in_tempdir() do _
        mkpath("raw")
        for s in ["A", "B", "C"]
            write("raw/$(s)_R1.fq", "reads $s\n")
        end
        write("ref.fa", ">ref\n")
        trim = @rule trim("raw/{s}_R1.fq" => "trim/{s}.fq") = "tr a-z A-Z < {input} > {output}"
        align = @rule align(["trim/{s}.fq", "ref.fa"] => "bam/{s}.bam"; threads=2) =
            "cat {input[2]} {input[1]} > {output}"
        index = @rule index("bam/{s}.bam" => "bam/{s}.bam.bai") = "wc -c < {input} > {output}"
        merge = @rule merge(gather("bam/{s}.bam") => "all.txt") = "cat {input} > {output}"
        wf = trim >> align >> index >> merge          # rules merge into one Workflow
        @test wf isa Workflow && length(wf.rules) == 4 && isempty(wf.targets)
        r = quiet(wf; jobs=4)
        @test r.success && nstatus(r, :done) == 10
        @test r.value == ["bam/A.bam.bai", "bam/B.bam.bai", "bam/C.bam.bai", "all.txt"]
        @test read("all.txt", String) == ">ref\nREADS A\n>ref\nREADS B\n>ref\nREADS C\n"
        @test "align[s=B]" in labels(r)
        # everything is up to date on a second run
        @test all(==(:skipped), statuses(quiet(wf)))
        # a new sample is picked up, and the gather step reruns because its inputs changed
        write("raw/D_R1.fq", "reads D\n")
        r = quiet(wf)
        @test sort(labels(filter(s -> s.status === :done, r.steps))) ==
              ["align[s=D]", "index[s=D]", "merge", "trim[s=D]"]
        # order of the rules does not matter
        @test quiet(merge >> index >> align >> trim).success
        # run(rules...) and Workflow(...) are the same thing
        @test quiet(Workflow(trim, align, index, merge)).value == quiet(wf).value
        @test run(trim, align; targets="bam/A.bam", verbose=false).value == ["bam/A.bam"]
    end
end

@testset "Workflow: explicit and pattern targets" begin
    in_tempdir() do _
        mkpath("raw")
        for s in ["A", "B"], r in [1, 2]
            write("raw/$(s)_$(r).txt", "$s$r")
        end
        pair = @rule pair(["raw/{s}_1.txt", "raw/{s}_2.txt"] => "pairs/{s}.txt") = "cat {input} > {output}"
        r = quiet(Workflow(pair; targets="pairs/B.txt"))
        @test r.value == ["pairs/B.txt"] && labels(r) == ["pair[s=B]"]
        r = quiet(Workflow(pair; targets="pairs/{s}.txt"))
        @test r.value == ["pairs/A.txt", "pairs/B.txt"]
        # a sample with an incomplete pair is not a target
        write("raw/C_1.txt", "C1")
        @test quiet(Workflow(pair; targets="pairs/{s}.txt")).value == ["pairs/A.txt", "pairs/B.txt"]
        # filter on discovered wildcard values
        r = quiet(Workflow(pair; targets="pairs/{s}.txt", filter=w -> w.s != "A"); force=true)
        @test r.value == ["pairs/B.txt"]
        @test quiet(pair; targets=expand("pairs/{s}.txt"; s=["A", "B"])).success
        # run(workflow; targets=...) overrides the stored targets
        wf = Workflow(pair; targets="pairs/A.txt")
        @test quiet(wf; targets="pairs/B.txt").value == ["pairs/B.txt"]
    end
end

@testset "Workflow: unresolvable targets explain why" begin
    in_tempdir() do _
        mkpath("raw"); write("raw/A.fq", "")
        trim = @rule trim("raw/{s}.fq" => "trim/{s}.fq") = "cp {input} {output}"
        align = @rule align("trim/{s}.fq" => "bam/{s}.bam") = "cp {input} {output}"
        r = quiet(trim >> align; targets="bam/Z.bam")
        @test !r.success && r[1].result.kind === :unresolved
        msg = r[1].result.message
        @test occursin("cannot build `bam/Z.bam`", msg) && occursin("raw/Z.fq", msg)
        @test occursin("rule `align[s=Z]` needs `trim/Z.fq`", msg)
        r = quiet(trim >> align; targets="bam/{s}.bam", filter=w -> false)
        @test !r.success && occursin("matched nothing", r[1].result.message)
        r = quiet(Workflow(@rule x("none/{s}.fq" => "out/{s}.txt") = "cp {input} {output}"))
        @test occursin("none/{s}.fq (0 matches)", r[1].result.message)
        @test_throws ErrorException plan(trim, align; targets="bam/Z.bam")
        # a dependency cycle is reported, not an infinite loop
        a = @rule a("x/{s}.b" => "x/{s}.a") = "cp {input} {output}"
        b = @rule b("x/{s}.a" => "x/{s}.b") = "cp {input} {output}"
        r = quiet(Workflow(a, b; targets="x/q.a"))
        @test !r.success && occursin("cycle", r[1].result.message)
    end
end

@testset "Workflow: rule choice and existing files" begin
    in_tempdir() do _
        # the most specific matching rule wins, whatever the declaration order
        generic = @rule generic("{x}.sam" => "{x}.bam") = "echo generic > {output}"
        sorted = @rule sorted("{x}.bam" => "{x}.sorted.bam") = "echo sorted > {output}"
        write("A.sam", "")
        r = quiet(Workflow(generic, sorted; targets="A.sorted.bam"))
        @test labels(r) == ["generic[x=A]", "sorted[x=A]"]
        # an existing file that no applicable rule can rebuild is used as is (like Make)
        gunzip = @rule gunzip("{x}.fq.gz" => "{x}.fq") = "gunzip -c {input} > {output}"
        count = @rule count("{x}.fq" => "{x}.count") = "wc -l < {input} > {output}"
        write("B.fq", "line\n")
        r = quiet(Workflow(gunzip, count; targets="B.count"))
        @test r.success && labels(r) == ["count[x=B]"]
        # a newer input rebuilds an existing target (regression: existing targets were never rebuilt)
        copyrule = @rule cp("src/{x}.txt" => "dst/{x}.txt") = "cp {input} {output}"
        mkpath("src"); write("src/A.txt", "old")
        quiet(copyrule; targets="dst/A.txt")
        sleep(1.1); write("src/A.txt", "new")
        r = quiet(copyrule; targets="dst/A.txt")
        @test statuses(r) == [:done] && read("dst/A.txt", String) == "new"
    end
end

@testset "Workflow: concrete steps, sinks and gather" begin
    in_tempdir() do _
        mkpath("raw")
        foreach(s -> write("raw/$s.txt", s), ["A", "B"])
        ref = @step ref([] => "ref/index.txt") = "echo index > {output}"
        use = @rule use(["raw/{s}.txt", "ref/index.txt"] => "out/{s}.txt") = "cat {input} > {output}"
        r = quiet(Workflow(ref, use))
        @test r.success && nstatus(r, :done) == 3                 # the reference is built once
        @test read("out/A.txt", String) == "Aindex\n"
        # a rule without outputs runs once per matching input
        seen = String[]
        lock_ = ReentrantLock()
        report = @rule report("out/{s}.txt") = path -> lock(() -> push!(seen, path), lock_)
        r = quiet(Workflow(ref, use, report))
        @test r.success && sort(seen) == ["out/A.txt", "out/B.txt"]
        # gather: function rules get the list; per-group gathers work too
        foreach(((s, r),) -> write("raw/$(s)_$(r).dat", "$s$r"), [("A", 1), ("A", 2), ("B", 1)])
        pergroup = @rule pergroup(gather("raw/{s}_{rep}.dat") => "groups/{s}.txt") =
            (files, out) -> write(out, join(basename.(files), ","))
        r = quiet(Workflow(pergroup))
        @test r.value == ["groups/A.txt", "groups/B.txt"]
        @test read("groups/A.txt", String) == "A_1.dat,A_2.dat"
        # gather traces through rules: files that do not exist yet are found from their sources
        upper = @rule upper("raw/{s}.txt" => "up/{s}.txt") = "tr a-z A-Z < {input} > {output}"
        allup = @rule allup(gather("up/{s}.txt") => "all_up.txt") = "cat {input} > {output}"
        r = quiet(upper >> allup)
        @test read("all_up.txt", String) == "AB"
        # wildcard values with spaces are quoted in commands
        mkpath("sp"); write("sp/my sample.txt", "x")
        q = @rule q("sp/{s}.txt" => "spo/{s}.txt") = "cp {input} {output} && echo {s} > /dev/null"
        @test quiet(q).success && isfile("spo/my sample.txt")
    end
end

@testset "Workflow composition and @workflow" begin
    in_tempdir() do _
        mkpath("raw"); write("raw/A.txt", "a")
        up = @rule up("raw/{s}.txt" => "up/{s}.txt") = "tr a-z A-Z < {input} > {output}"
        rev = @rule rev("up/{s}.txt" => "rev/{s}.txt") = "rev < {input} > {output}"
        # a workflow is a node: it composes with steps, and its value is its target list
        r = quiet((up >> rev) >> (@step n = paths -> length(paths)))
        @test r.value == 1
        # a workflow sees files that an earlier step created
        mk = @step mk([] => "raw/B.txt") = "printf b > {output}"
        r = quiet(mk >> Workflow(up, rev))
        @test r.value == ["rev/A.txt", "rev/B.txt"]
        wf = @workflow "demo" begin
            samples = ["A"]
            @rule up2("raw/{s}.txt" => "up2/{s}.txt") = "cp {input} {output}"
            expand("up2/{s}.txt"; s=samples)
        end
        @test wf.name == "demo" && wf.targets == ["up2/A.txt"] && length(wf.rules) == 1
        @test quiet(wf).value == ["up2/A.txt"]
        @test_throws ArgumentError @workflow "bad" begin
            42
        end
        # explicit targets survive a merge; automatic ones become patterns
        merged = Workflow(up; targets="up/A.txt") >> rev
        @test Set(merged.targets) == Set(["up/A.txt", "rev/{s}.txt"])
        @test_throws ArgumentError Workflow(Step(:x, sh"true"))   # steps need outputs
        @test_throws ArgumentError Workflow(42)
    end
end

@testset "plan and check" begin
    in_tempdir() do _
        mkpath("raw"); foreach(s -> write("raw/$s.txt", s), ["A", "B"])
        up = @rule up("raw/{s}.txt" => "up/{s}.txt") = "tr a-z A-Z < {input} > {output}"
        rev = @rule rev("up/{s}.txt" => "rev/{s}.txt") = "rev < {input} > {output}"
        p = plan(up, rev; targets="rev/A.txt")
        @test p isa SP.Job && SP.step_label(p.step) == "rev[s=A]"
        @test [SP.step_label(s) for s in steps(p)] == ["up[s=A]", "rev[s=A]"]
        @test quiet(p).success
        c = check(up >> rev; verbose=false)
        @test c isa RunResult && c.planned
        @test statuses(c) == [:skipped, :skipped, :planned, :planned]   # A is built; B is not
        reasons = Dict(s.label => s.result for s in c if s.status === :planned)
        @test reasons["up[s=B]"] == "missing output up/B.txt"
        @test reasons["rev[s=B]"] == "missing output rev/B.txt"
        @test !isfile("up/B.txt")                                        # nothing ran
        # check finds inputs that nothing produces
        c = check(Step(:needs, sh"true", ["absent.txt"], ["o.txt"]); verbose=false)
        @test !c.success && c[1].result.kind === :missing_input
        # ... but knows about files earlier steps will create
        mk = @step mk([] => "raw/C.txt") = "echo c > {output}"
        c = check(mk >> Workflow(up, rev); verbose=false)
        @test c.success && "rev[s=C]" in labels(c)
    end
end
