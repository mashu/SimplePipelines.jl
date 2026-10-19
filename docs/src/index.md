# SimplePipelines.jl

*Incremental, parallel pipelines for shell commands and Julia functions, with
Snakemake-style wildcard rules.*

SimplePipelines takes the plumbing out of preprocessing pipelines. Declare what
each step reads and writes; the package works out the order, runs independent work
in parallel within your CPU and memory budget, skips what is already up to date, and
tells you exactly what failed and why.

```julia
using SimplePipelines

trim  = @rule trim("raw/{sample}_R1.fq.gz" => "trim/{sample}.fq.gz"; threads=4) =
    "fastp -w {threads} -i {input} -o {output}"
align = @rule align(["trim/{sample}.fq.gz", "ref.fa"] => "bam/{sample}.bam"; threads=8) =
    "bwa mem -t {threads} ref.fa {input[1]} | samtools sort -o {output}"
calls = @rule calls(gather("bam/{sample}.bam") => "calls.vcf") =
    "bcftools mpileup -f ref.fa {input} | bcftools call -mv -o {output}"

check(trim >> align >> calls)     # what would run, and why
run(trim >> align >> calls)       # one trim and align per sample on disk, then one variant call
```

There is no sample list to maintain: the samples are whatever `raw/*_R1.fq.gz`
files exist. Add a sample and rerun, and only the new sample plus the variant call
run again.

## What it does for you

- **Wildcards end to end.** `{sample}` in a path is a wildcard. Targets can be
  patterns (`"bam/{sample}.bam"`): values are discovered by tracing the rules back
  to the source files. `gather(...)` collects every sample into one step.
- **Incremental builds that are correct.** A step is skipped only when its outputs
  exist, are newer than its inputs, *and* were made by the same command. Edit a
  command and the affected steps rerun. Outputs of a failed step are removed, so a
  half-written file never looks finished.
- **Safe on your machine.** At most `jobs` CPU threads are busy, however deeply the
  pipeline is nested; steps declare `threads` and `mem_mb`. Command output streams
  through bounded buffers. Timeouts and Ctrl-C stop running commands, including
  their child processes.
- **Errors you can act on.** Shell commands run with `bash -e -o pipefail`, so a
  failure inside a pipe fails the step. Typos in wildcards and unknown placeholders
  are caught when the rule is defined. When a target cannot be built, you get the
  chain of missing files.
- **Low latency.** Loading takes about 0.2 s. The runtime is compiled once, so your
  own functions add almost no compile time on the first run.

## Where to go next

1. [Getting started](guide/getting-started.md): a first pipeline in five minutes.
2. [Steps](guide/steps.md) and [Composing pipelines](guide/composition.md): the
   building blocks.
3. [Wildcard rules and workflows](guide/rules.md): the part that saves the most code
   in bioinformatics.
4. [Running, freshness and resources](guide/running.md): what `run` and `check` do.

The [quick reference](reference/quickref.md) fits the whole API on one page. If you
are upgrading from 0.3, read the [migration notes](reference/migration.md).
