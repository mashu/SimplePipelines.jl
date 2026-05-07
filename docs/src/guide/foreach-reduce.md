# Fan-out and reduce

## `ForEach` (pattern or collection)

Two modes:

- **Collection** — apply the block to each item (like map).
- **Pattern** — discover files by wildcard pattern and run the block per match.

```julia
samples = ["sample_A", "sample_B", "sample_C"]
merge_results = @step merge_results = sh"echo merge done"
pipeline = ForEach(samples) do s
    Step(Symbol("process_", s), sh("analyze $s.fastq"))
end >> merge_results
```

**Pattern-based discovery** scans the filesystem; matching files must exist.

```julia
cd(mktempdir()) do
    write("s1.fastq", ""); write("s2.fastq", "")
    mkpath("fastq"); write("fastq/s1_R1.fq.gz", ""); write("fastq/s2_R1.fq.gz", "")
    mkpath("data/p1"); write("data/p1/s1.csv", "")

    ForEach("{sample}.fastq") do sample
        sh("process $(sample).fastq")
    end

    ForEach("fastq/{sample}_R1.fq.gz") do sample
        sh("pear $(sample)_R1 $(sample)_R2") >> sh("analyze $(sample)")
    end

    ForEach("data/{project}/{sample}.csv") do project, sample
        sh("process $(project)/$(sample).csv")
    end

    ForEach("{id}.fastq") do id
        sh("align $(id).fastq")
    end >> @step merge = sh"merge *.bam"
end
```

## `Reduce` (combine parallel outputs)

Collect outputs from parallel steps and combine them:

```julia
analyze_a = @step a = sh"echo result_a"
analyze_b = @step b = sh"echo result_b"
pipeline = Reduce(analyze_a & analyze_b) do outputs
    join(outputs, "\n")
end

combine_results(outputs) = join(outputs, "\n")
step_a = @step a = sh"echo result_a"
step_b = @step b = sh"echo result_b"
step_c = @step c = sh"echo result_c"
pipeline = Reduce(combine_results, step_a & step_b & step_c)

merge_outputs(outputs) = join(outputs, "\n")
fetch = @step fetch = sh"echo data"
analyze_a = @step a = sh"wc -c"
analyze_b = @step b = sh"wc -l"
report = @step report = sh"echo done"
pipeline = fetch >> Reduce(merge_outputs, analyze_a & analyze_b) >> report
```

The reducer receives a vector of outputs from successful parallel branches (element types depend on upstream steps).

### Low-memory / large data

The runtime keeps every step's `result` alive in the per-run memo (so DAG sharing
works correctly). To keep memory bounded:

1. Have each step write its result to a file and return a [`FilePath`](@ref) — only
   the path travels between steps; the underlying value is freed.
2. Let the reducer open or stream paths one at a time and return a combined path.
3. The returned vector contains every step's result; if you only need the final
   value, take `last(results)`.

**Next:** [Running and inspecting](running-and-results.md) — `run`, results, utilities, shell + Julia together.
