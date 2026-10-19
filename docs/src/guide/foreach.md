# Fan-out with ForEach

[`ForEach`](@ref) builds one branch per item when the pipeline runs. Use it when
the work is driven by a list or a set of files and you want ordinary Julia code in
each branch. Use [rules](rules.md) when output file names should drive the work and
dependencies between files matter.

## Over a collection

```julia
samples = ["A", "B", "C"]

per_sample = ForEach(samples) do s
    @step count = sh("zcat raw/$(s).fq.gz | wc -l")
end

results = run(per_sample)
results.value              # one stdout per sample, in order
```

The block returns a step or node; a shell command or function is lifted to a step.
Log lines are tagged with the item: `count[A]`. Any iterable works:
`ForEach(1:10)`, `ForEach(eachrow(df))`, `ForEach(readlines("samples.txt"))`.

## Over files

```julia
ForEach("fastq/{sample}_R1.fq.gz") do sample
    @step trim = sh("fastp -i fastq/$(sample)_R1.fq.gz -o trim/$(sample).fq.gz")
end
```

The pattern is matched when the pipeline runs, so it sees files that earlier steps
created. Several wildcards arrive as several arguments, in order:

```julia
ForEach("data/{project}/{sample}.csv") do project, sample
    @step load = () -> process("data/$project/$sample.csv")
end
```

A pattern that matches nothing is reported as a failure (`:no_matches`), because
it almost always means a wrong path. An empty collection is not a failure.

## Reducing branch values

The value of a `ForEach` is the vector of branch values, so `>>` into a function
reduces it:

```julia
total = ForEach(samples) do s
    @step reads = () -> countlines("raw/$(s).fq") ÷ 4
end >> sum
```

For large data, return paths rather than big objects, and let the reducer read the
files one at a time.

## Branches and resources

All branches run in parallel within the `jobs` budget of [`run`](@ref). Declare
what heavy steps need, so that they queue instead of overloading the machine:

```julia
ForEach(samples) do s
    @step align(; threads=8, mem_mb=16_000) =
        sh("bwa mem -t 8 ref.fa raw/$(s).fq | samtools sort -o bam/$(s).bam")
end
```

**Next:** [Control flow](control-flow.md).
