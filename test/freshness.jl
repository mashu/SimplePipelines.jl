@testset "up-to-date steps are skipped" begin
    in_tempdir() do _
        write("in.txt", "x")
        s = @step copy("in.txt" => "out.txt") = "cp {input} {output}"
        @test !is_fresh(s)
        @test statuses(quiet(s)) == [:done]
        @test is_fresh(s)
        r = quiet(s)
        @test statuses(r) == [:skipped] && r[1].result === nothing && r.value == ["out.txt"]
        # a newer input makes it stale
        sleep(1.1); write("in.txt", "y")
        @test !is_fresh(s)
        @test statuses(quiet(s)) == [:done] && read("out.txt", String) == "y"
        # a missing output makes it stale
        rm("out.txt")
        @test statuses(quiet(s)) == [:done]
        # an output only step (e.g. a download) stays done while its output exists
        dl = @step dl([] => "data.csv") = "echo a,b > {output}"
        quiet(dl)
        @test statuses(quiet(dl)) == [:skipped]
    end
end

@testset "steps without outputs always run" begin
    in_tempdir() do _
        # a step without outputs runs every time and passes its fresh value on
        a = @step a = () -> 41
        b = @step b = x -> x + 1
        quiet(a >> b)
        r = quiet(a >> b)
        @test statuses(r) == [:done, :done] && r.value == 42
        @test !is_fresh(a)
        write("in.txt", "x")
        lint = @step lint("in.txt") = sh"test -s in.txt"
        @test statuses(quiet(lint)) == [:done] && statuses(quiet(lint)) == [:done]
    end
end

@testset "a changed recipe reruns the step" begin
    in_tempdir() do _
        write("in.txt", "x")
        v1 = @step make("in.txt" => "out.txt") = "echo v1 > {output}"
        v2 = @step make("in.txt" => "out.txt") = "echo v2 > {output}"
        quiet(v1)
        @test is_fresh(v1) && !is_fresh(v2)
        r = quiet(v2)
        @test statuses(r) == [:done] && strip(read("out.txt", String)) == "v2"
        @test statuses(quiet(v2)) == [:skipped]
        # changing the inputs list also counts
        write("extra.txt", "e")
        v3 = Step(:make, "echo v2 > {output}", ["in.txt", "extra.txt"], ["out.txt"])
        @test !is_fresh(v3)
        # renaming a step does not
        renamed = @step other("in.txt" => "out.txt") = "echo v2 > {output}"
        @test is_fresh(renamed)
        # changing threads does not (only the template counts for rules)
        mkpath("raw"); write("raw/A.txt", "a")
        t1 = @rule t("raw/{s}.txt" => "t/{s}.txt"; threads=1) = "cp {input} {output} # {threads}"
        t8 = @rule t("raw/{s}.txt" => "t/{s}.txt"; threads=8) = "cp {input} {output} # {threads}"
        quiet(t1)
        @test statuses(quiet(t8)) == [:skipped]
    end
end

@testset "state file" begin
    in_tempdir() do dir
        write("in.txt", "x")
        s = @step copy("in.txt" => "out.txt") = "cp {input} {output}"
        quiet(s)
        text = read(".pipeline_state", String)
        @test startswith(text, "# SimplePipelines state v2") && occursin("\tout.txt", text)
        @test filesize(".pipeline_state") < 200                 # one short line per output
        # outputs made without a record (first run, deleted state) are judged by timestamps
        clear_state!()
        @test !isfile(".pipeline_state") && is_fresh(s)
        @test statuses(quiet(s)) == [:skipped]                    # and get adopted
        @test occursin("\tout.txt", read(".pipeline_state", String))
        # a corrupt or old-format state file is ignored
        write(".pipeline_state", "SPstate\0" * repeat("\0", 64))
        @test is_fresh(s)
        # state_path keeps several pipelines apart
        quiet(s; state_path=joinpath("meta", "state.tsv"), force=true)
        @test isfile(joinpath("meta", "state.tsv"))
        @test is_fresh(s; state_path=joinpath("meta", "state.tsv"))
        # failed steps forget their record
        bad = @step copy("in.txt" => "out.txt") = "exit 1"
        quiet(bad; force=true)
        @test !occursin("\tout.txt", read(".pipeline_state", String))
        # check does not write state
        clear_state!()
        check(s; verbose=false)
        @test !isfile(".pipeline_state")
    end
end
