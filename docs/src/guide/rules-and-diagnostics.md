# Rules and diagnostics

Writing every sample by hand gets old quickly. Rules are for workflows where the
same transformation applies to many files:

```text
raw/A.fq -> out/A.bam
raw/B.fq -> out/B.bam
raw/C.fq -> out/C.bam
```

Instead of listing every step, write one pattern. The wildcard in the output
path is inferred from the target you ask for.

```julia
align = @rule align("raw/{sample}.fq" => "out/{sample}.bam") =
    "bwa mem ref.fa {input} > {output}"
```

Read this as: "to make `out/{sample}.bam`, use `raw/{sample}.fq` and run this
command."

The command is a template string. Unlike `@step sort = sh"sort data.txt"`, a
rule does not know its concrete filenames yet. `{input}`, `{output}`, and
`{sample}` are filled in when you ask for a target such as `out/A.bam`.

## Check One Rule First

Before composing a workflow, check the rule by itself:

```julia
check(align)
```

This answers: what wildcards does the rule know about, and which placeholders
does the command use?

Then try one concrete target:

```julia
check(align, "out/A.bam")
```

Now the abstract pattern becomes concrete:

```text
sample  => A
input   => raw/A.fq
output  => out/A.bam
command => bwa mem ref.fa raw/A.fq > out/A.bam
```

Nothing runs here. This is a safe way to catch spelling mistakes like
`{smaple}` before building the whole DAG.

## Workflow block

Once individual rules look right, collect them into a workflow block. A workflow
is just a named set of rules plus the targets you want to build.

```julia
wf = @workflow "rnaseq" begin
    @rule source([] => "raw/{sample}.fq") =
        "mkdir -p raw && echo {sample} > {output}"

    @rule align("raw/{sample}.fq" => "out/{sample}.bam") =
        "mkdir -p out && bwa mem ref.fa {input} > {output}"

    @targets "out/{sample}.bam" sample=["A", "B"]
end
```

`@targets` expands the target pattern:

```julia
@targets "out/{sample}.bam" sample=["A", "B"]
# ["out/A.bam", "out/B.bam"]
```

The workflow block keeps the beginner-facing syntax compact. Underneath, it is
the same `Workflow` object used by `plan(wf)` and `run(wf)`.

## Explain the DAG

Use [`explain`](@ref) when rules depend on other rules and you want to see the
chain for one target.

```julia
explain(wf; target="out/A.bam")
```

For the workflow above, `explain` shows that `out/A.bam` is made by `align`, and
that `align` depends on `raw/A.fq`, which is made by `source`.

After the explanation makes sense, inspect or run the DAG:

```julia
print_dag(plan(wf))
run(wf)
```

## What To Remember

Use `check` while writing one rule. Use `explain` after composing rules into a
workflow. That keeps debugging local: first prove one pattern, then prove the
dependency chain.

**Next:** [Fan-out and reduce](foreach-reduce.md) covers item-driven fan-out
when a collection or discovered files should drive the branches.
