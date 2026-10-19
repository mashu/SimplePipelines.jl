plain(x) = sprint(show, MIME("text/plain"), x)
dag_text(x; expand=true) = sprint(io -> print_dag(io, x; expand))

@testset "show" begin
    a = @step a = sh"echo a"
    @test sprint(show, a) == "Step(:a)"
    @test sprint(show, sh"echo") == "sh\"echo\""
    @test occursin(">>", sprint(show, a >> a))
    @test occursin("&", sprint(show, a & Step(:b, sh"true")))
    @test sprint(show, Pipeline(a; name="p")) == "Pipeline(\"p\")"
    @test occursin("Wildcards(s=\"A\")", sprint(show, SP.Wildcards(Dict("s" => "A"))))
    s = @step bam(["r1.fq", "r2.fq"] => "a.bam"; threads=4) = "bwa mem -t {threads} {input} > {output}"
    text = plain(s)
    @test occursin("← r1.fq, r2.fq", text) && occursin("→ a.bam", text)
    @test occursin("\$ bwa mem -t {threads} r1.fq r2.fq > a.bam", text) && occursin("threads=4", text)
    r = @rule align("raw/{s}.fq" => "bam/{s}.bam") = "bwa mem {input} > {output}"
    @test occursin("Rule align", plain(r)) && occursin("raw/{s}.fq", plain(r))
    wf = Workflow(r; name="w")
    @test occursin("Workflow w", plain(wf)) && occursin("automatic", plain(wf))
end

@testset "print_dag" begin
    in_tempdir() do _
        a = Step(:a, sh"echo a", ["in.txt"], ["out.txt"])
        b, c = Step(:b, sh"true"), Step(:c, sh"true")
        text = dag_text(a >> (b & c) | c)
        @test occursin("Fallback", text) && occursin("Sequence", text) && occursin("Parallel", text)
        @test occursin("← in.txt", text) && occursin("→ out.txt", text) && occursin("├─", text)
        @test occursin("Retry ×3", dag_text(b^3)) && occursin("Timeout 5.0s", dag_text(Timeout(b, 5)))
        @test occursin("Branch", dag_text(Branch(() -> true, b, c)))
        @test occursin("threads=2", dag_text(with_resources(b; threads=2)))
        # ForEach and Workflow are expanded against the files on disk
        mkpath("raw"); foreach(s -> write("raw/$s.txt", s), ["A", "B"])
        fe = ForEach("raw/{s}.txt") do s; @step cat = sh("cat raw/$s.txt"); end
        @test occursin("cat raw/B.txt", dag_text(fe)) && !occursin("cat raw/B.txt", dag_text(fe; expand=false))
        up = @rule up("raw/{s}.txt" => "up/{s}.txt") = "tr a-z A-Z < {input} > {output}"
        merge = @rule merge(gather("up/{s}.txt") => "all.txt") = "cat {input} > {output}"
        rev = @rule rev("up/{s}.txt" => "rev/{s}.txt") = "rev {input} > {output}"
        text = dag_text(up >> merge >> rev)
        @test occursin("up[s=A]", text) && occursin("\$ cat up/A.txt up/B.txt > all.txt", text)
        @test occursin("(shown above)", text)                 # shared jobs are printed once
        @test occursin("cannot expand yet", dag_text(Workflow(@rule x("none/{s}") = "true")))
        @test occursin("… ", dag_text(reduce(&, [Step(Symbol("s$i"), sh"true") for i in 1:30])))
        # REPL view does not touch the filesystem
        @test occursin("ForEach(\"raw/{s}.txt\")", plain(fe))
    end
end

@testset "results" begin
    in_tempdir() do _
        r = quiet(Step(:ok, sh"echo ok") & Step(:bad, sh"echo boom >&2; exit 3"))
        text = plain(r)
        @test occursin("1 done, 0 up to date, 1 failed", text)
        @test occursin("✗ bad", text) && occursin("exited with code 3", text)
        bad = only(filter(s -> !s.success, r.steps))
        @test occursin("boom", plain(bad)) && occursin("error:", plain(bad))
        @test occursin("StepResult(\"bad\", :failed)", sprint(show, bad))
        @test occursin("exited with code 3", sprint(showerror, bad.result))
        c = check(Step(:x, sh"true", String[], ["x.txt"]); verbose=false)
        @test occursin("Check pipeline: 1 to run", plain(c)) && occursin("missing output x.txt", plain(c))
        # verbose output
        out = mktemp() do path, io
            redirect_stdout(io) do
                run(Step(:shown, sh"echo hi") >> Step(:boom, sh"echo err >&2; exit 2"))
            end
            flush(io)
            read(path, String)
        end
        @test occursin("═══ Pipeline: pipeline ═══", out) && occursin("▶ shown", out)
        @test occursin("\$ echo hi", out) && occursin("✓ shown", out) && occursin("✗ boom", out)
        @test occursin("err", out) && occursin("1 done, 0 up to date, 1 failed", out)
    end
end
