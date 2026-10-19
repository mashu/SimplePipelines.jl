# Wildcard rules and workflows

Most preprocessing applies the same steps to many files. A **rule** is a step
template whose paths contain `{wildcards}`; a **workflow** is a set of rules that
SimplePipelines turns into concrete steps by matching file names.

## Rules

```julia
using SimplePipelines

trim = @rule trim("raw/{sample}_R1.fq.gz" => "trim/{sample}.fq.gz"; threads=4) =
    "fastp -w {threads} -i {input} -o {output}"
```

Read this as: "to make `trim/X.fq.gz`, run fastp on `raw/X_R1.fq.gz`". A
wildcard matches one path component, so it never contains `/`. `@step` with a
wildcard path builds the same thing, so you can use whichever reads better.

Rules are checked when they are defined:

- every input wildcard must appear in the outputs, so `{smaple}` vs `{sample}` is
  an error, not a silent mismatch;
- all outputs must use the same wildcards;
- unknown placeholders in the command are errors.

The command can use `{input}`, `{output}`, `{input[i]}`, `{output[i]}`,
`{threads}`, `{mem_mb}` and the wildcards themselves (`{sample}`). A function rule
receives one argument per declared input (a `Vector{String}` for a
[`gather`](@ref)), then the outputs:

```julia
filter_reads = @rule filter_reads("tsv/{sample}.tsv" => "filtered/{sample}.tsv") =
    function (input, output)
        rows = filter(l -> !occursin("\tNA\t", l), readlines(input))
        write(output, join(rows, "\n"))
    end
```

## Running rules

Running one rule builds its outputs for every input that exists:

```julia
run(trim)          # one step per raw/*_R1.fq.gz
```

Chain rules with `>>` (or `&`; for rules they mean the same thing). The result is a
[`Workflow`](@ref): a single node that is resolved into concrete steps when it runs.

```julia
align = @rule align(["trim/{sample}.fq.gz", "ref.fa"] => "bam/{sample}.bam"; threads=8) =
    "bwa mem -t {threads} ref.fa {input[1]} | samtools sort -o {output}"
index = @rule index("bam/{sample}.bam" => "bam/{sample}.bam.bai") = "samtools index {input}"

wf = trim >> align >> index
run(wf)
```

Nothing lists the samples. The workflow builds the outputs of every rule that no
other rule consumes (here `bam/{sample}.bam.bai`), and finds the samples by tracing
back through `align` and `trim` to the files that exist under `raw/`. Order does not
matter: dependencies come from the file names, so `index >> align >> trim` is the
same workflow.

Each sample becomes its own chain of steps, and samples run in parallel within your
`jobs` budget. The scheduler prefers to finish a sample before starting the next,
so results arrive early and intermediate files do not pile up.

## Targets

Pass `targets` to choose what to build. A target is a path or a pattern:

```julia
run(wf; targets="bam/A.bam.bai")                       # one sample
run(wf; targets="bam/{sample}.bam")                    # every sample, stop after align
run(wf; targets=expand("bam/{s}.bam.bai"; s=["A", "B"]))
run(trim, align; targets="bam/A.bam")                  # rules given directly
```

A pattern target gets its wildcard values from the source files, like the automatic
targets. A sample whose sources are incomplete (an `R1` without its `R2`, say) is
not a target. To drop values, pass a `filter`:

```julia
run(wf; filter = w -> w.sample != "control")
run(wf; filter = w -> parse(Int, w.rep) < 100)
```

The filter receives a `Wildcards` value: read fields as `w.sample` or `w["sample"]`.

## Collecting all samples: `gather`

A merge step needs every sample. Wrap its input in [`gather`](@ref): wildcards that
do not appear in the outputs are collected over every value.

```julia
calls = @rule calls(gather("bam/{sample}.bam") => "calls.vcf") =
    "bcftools mpileup -f ref.fa {input} | bcftools call -mv -o {output}"

run(trim >> align >> calls)
```

The BAM files do not have to exist yet. The workflow traces them back to the raw
files, so `calls` depends on the `align` step of every sample. When a new sample
appears, `calls` reruns, because its list of inputs changed. Wildcards that do
appear in the outputs still split the work, which gives one merge per group:

```julia
@rule per_donor(gather("bam/{donor}_{rep}.bam") => "merged/{donor}.bam") =
    "samtools merge {output} {input}"
```

## Concrete steps and output-less rules

A workflow can contain concrete steps with outputs, for example a reference index
that every sample needs. It is built once, before the samples that need it:

```julia
index_ref = @step index_ref("ref.fa" => "ref.fa.bwt") = "bwa index {input}"
align = @rule align(["trim/{s}.fq.gz", "ref.fa.bwt"] => "bam/{s}.bam") =
    "bwa mem ref.fa {input[1]} > {output}"
run(Workflow(index_ref, trim, align))
```

A rule without outputs, such as a validation or upload step, runs once for every
match of its inputs:

```julia
validate = @rule validate("bam/{s}.bam") = "samtools quickcheck {input}"
run(trim >> align >> validate)
```

## How a target is resolved

When several rules can produce a path, the most specific one wins (the one whose
wildcards match the fewest characters), as long as its inputs can be satisfied. For
`A.sorted.bam`, a rule for `{x}.sorted.bam` beats one for `{x}.bam`, regardless of
the order you wrote them in.

As in Make, an existing file that no applicable rule can rebuild is used as it is.
When a target cannot be built, the error names the whole chain:

```text
cannot build `bam/Z.bam`:
  rule `align[sample=Z]` needs `trim/Z.fq.gz`
  rule `trim[sample=Z]` needs `raw/Z_R1.fq.gz`
  `raw/Z_R1.fq.gz` does not exist and no rule produces it
```

## Inspecting a workflow

```julia
discover("raw/{sample}_R1.fq.gz")   # what does the pattern match on disk?
print_dag(wf)                        # the concrete steps, with paths and commands
check(wf)                            # what would run, and why
plan(wf; targets="bam/A.bam")       # the concrete DAG as a node
```

Steps in logs and results are labelled with their wildcard values:
`align[sample=A]`.

## Workflows inside pipelines

A workflow is a node, so it composes with steps. It is resolved when it runs, so it
sees files that earlier steps created:

```julia
fetch = @step fetch([] => "raw/manifest.txt") = "download_run.sh raw/"
report = @step report = bams -> write("report.txt", join(bams, "\n"))

run(fetch >> (trim >> align) >> report)    # report receives the BAM paths
```

For a workflow in one block, use [`@workflow`](@ref):

```julia
wf = @workflow "rnaseq" begin
    @rule trim("raw/{s}.fq.gz" => "trim/{s}.fq.gz") = "fastp -i {input} -o {output}"
    @rule quant("trim/{s}.fq.gz" => "quant/{s}/abundance.tsv"; threads=4) =
        "kallisto quant -t {threads} -i idx -o quant/{s} --single -l 200 -s 20 {input}"
end
```

**Next:** [Fan-out with ForEach](foreach.md).
