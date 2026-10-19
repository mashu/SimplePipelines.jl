"""
    SimplePipelines

Incremental, parallel pipelines for shell commands and Julia functions, with
Snakemake-style wildcard rules.

```julia
using SimplePipelines

# explicit steps
prep  = @step prep([] => "data.txt") = "seq 1 100 > {output}"
total = @step total("data.txt" => "total.txt") = "paste -sd+ {input} | bc > {output}"
run(prep >> total)

# wildcard rules: one step per sample found on disk
trim  = @rule trim("raw/{s}.fq.gz" => "trim/{s}.fq.gz") = "fastp -i {input} -o {output}"
align = @rule align("trim/{s}.fq.gz" => "bam/{s}.bam"; threads=8) =
    "bwa mem -t {threads} ref.fa {input} | samtools sort -o {output}"
run(trim >> align)
```

See [`@step`](@ref), [`@rule`](@ref), [`Workflow`](@ref), [`run`](@ref) and [`check`](@ref).
"""
module SimplePipelines

export AbstractNode, Step, Sequence, Parallel, Retry, Fallback, Branch, Timeout, Force, ForEach, Pipeline
export @step, @branch, @sh_str, sh, with_resources
export Rule, @rule, gather, Workflow, @workflow, expand, discover, plan
export check, StepResult, StepFailure, RunResult
export print_dag, steps, count_steps, is_fresh, clear_state!

include("shell.jl")
include("patterns.jl")
include("nodes.jl")
include("rules.jl")
include("operators.jl")
include("macros.jl")
include("results.jl")
include("resolve.jl")
include("state.jl")
include("scheduler.jl")
include("execute.jl")
include("runtime.jl")
include("logging.jl")
include("run.jl")
include("display.jl")

using PrecompileTools: @setup_workload, @compile_workload

@setup_workload begin
    @compile_workload begin
        mktempdir() do dir
            cd(dir) do
                state = joinpath(dir, "state")
                mkpath("raw")
                write("raw/A.txt", "a\n")
                write("raw/B.txt", "b\n")
                make = @step make([] => "data.txt") = "echo data > {output}"
                copy = @step copy("data.txt" => "copy.txt") = (src, dst) -> cp(src, dst; force=true)
                size = @step size = paths -> filesize(only(paths))
                run(make >> copy >> size; verbose=false, state_path=state)
                run(make >> copy >> size; verbose=false)
                redirect_stdout(devnull) do
                    run(make >> copy >> size; state_path=state)
                    run(make >> copy >> size; force=true, state_path=state)
                end
                upper = @rule upper("raw/{s}.txt" => "up/{s}.txt") = "tr a-z A-Z < {input} > {output}"
                merge = @rule merge(gather("up/{s}.txt") => "all.txt") = "cat {input} > {output}"
                wf = upper >> merge
                run(wf; verbose=false)
                redirect_stdout(devnull) do
                    check(wf)
                    check(wf; state_path=state)
                    run(wf; state_path=state)
                    print_dag(wf)
                    show(stdout, MIME"text/plain"(), run(wf; state_path=state))
                end
                fan = ForEach(["x", "y"]) do x
                    @step echo = sh("echo $x")
                end
                run((fan & Retry(sh"true", 2)) >> Branch(v -> length(v) == 2, sh"true", sh"false") |
                    Timeout(sh"true", 10); verbose=false, state_path=state)
                discover("raw/{s}.txt")
                expand("out/{s}.txt"; s=["A", "B"])
            end
        end
    end
end

end # module
