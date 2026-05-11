# Fan-out and reduce

Use `ForEach` when the same subgraph should run many times. Use `Reduce` when
parallel branch outputs need to become one value.

This is different from rules: rules start from target file names and resolve
dependencies. `ForEach` starts from a collection or discovered files and builds a
branch for each item.

## Fan Out Over A Collection

The simplest `ForEach` is like `map`, but each item becomes a pipeline branch.

```julia
samples = ["sample_A", "sample_B", "sample_C"]

pipeline = ForEach(samples) do s
    @step process = sh("analyze $(s).fastq > $(s).txt")
end
```

The block returns a step or node. It is evaluated when the pipeline runs, so the
fan-out can reflect the current collection or filesystem.

## Discover Existing Files

Pattern mode scans the filesystem and captures wildcard values from matching
files.

```julia
cd(mktempdir()) do
    mkpath("fastq")
    write("fastq/A_R1.fq.gz", "")
    write("fastq/B_R1.fq.gz", "")

    pipeline = ForEach("fastq/{sample}_R1.fq.gz") do sample
        @step align = sh("align fastq/$(sample)_R1.fq.gz > $(sample).bam")
    end

    run(pipeline)
end
```

Multiple wildcards become multiple arguments:

```julia
ForEach("data/{project}/{sample}.csv") do project, sample
    @step process = sh("process data/$(project)/$(sample).csv")
end
```

Use pattern mode when the input files already exist and should drive the work.
Use rules when the requested output target should drive dependency resolution.

## Resource Limits

Fan-out can create many runnable branches. By default, `run` limits live branches
with `jobs=default_jobs()`, so a large `ForEach` queues work instead of starting
everything at once.

If each branch calls a heavy tool, declare that too:

```julia
pipeline = ForEach(samples) do sample
    step = @step align = sh("bwa mem -t 4 ref.fa $(sample).fq > $(sample).bam")
    with_resources(step; mem_mb=8_000, threads=4)
end

run(pipeline)
```

The default memory and thread budgets then keep declared-heavy branches from
running together when they would exceed the host-aware caps.

## Reduce Branch Outputs

`Reduce` runs a node and gives successful branch results to one reducer
function.

```julia
analyze_a = @step a = sh"echo result_a"
analyze_b = @step b = sh"echo result_b"

pipeline = Reduce(analyze_a & analyze_b) do outputs
    join(outputs, "\n")
end
```

The reducer receives a vector. The element type depends on upstream step
results, so keep reducers small and explicit.

## Large Data

For large branch results, do not return big objects from every branch if the
reducer can stream files instead. Return paths:

```julia
branches = ForEach(["A", "B"]) do sample
    @step write_result = () -> begin
        path = "$(sample).txt"
        write(path, "result for $sample")
        FilePath(path)
    end
end
```

Then the reducer can open each path one at a time.

## What To Remember

`ForEach` repeats a branch. `Reduce` combines branch results. If the work is
file-target driven, consider rules and `@workflow` first; if it is item-driven,
`ForEach` is usually clearer.

**Next:** [Running and inspecting](running-and-results.md) explains execution,
results, and memory behavior.
