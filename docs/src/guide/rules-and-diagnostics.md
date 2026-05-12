# Wildcard steps and diagnostics

Writing every sample by hand gets old quickly. When the same transformation
applies to many files, write one step template with a wildcard:

```text
raw/A.fq -> out/A.bam
raw/B.fq -> out/B.bam
raw/C.fq -> out/C.bam
```

The beginner-facing syntax is still `@step`. The only difference is that the
file paths contain `{sample}`:

```julia
align = @step align("raw/{sample}.fq" => "out/{sample}.bam") =
    "bwa mem ref.fa {input} > {output}"
```

Read this as: "for every sample, use `raw/<sample>.fq` to make
`out/<sample>.bam`."

Underneath, this is a [`Rule`](@ref): a pattern-backed step that becomes concrete
only after wildcard values are known. You can still write `@rule` explicitly, but
most users can start with wildcard `@step`.

## Julia Function Templates

Wildcard steps are not only for shell commands. A single-input/single-output
function can receive the concrete input and output paths:

```julia
using CSV, DataFrames

vj_error_filter = @step vj_error_filter("tsv/{sample}.tsv" => "out/{sample}.tsv") =
    function(input_file, output_file)
        df = CSV.read(input_file, DataFrame)
        filtered_df = filter(row -> row.V_errors <= 1, df)
        CSV.write(output_file, filtered_df, delim='\t')
    end
```

If matching inputs such as `tsv/A.tsv` and `tsv/B.tsv` already exist, this can run
directly:

```julia
run(vj_error_filter)
```

The template discovers matching inputs, infers `sample`, substitutes outputs, and
runs one concrete step per sample.

## Check One Template First

Before running a wildcard template, check it:

```julia
check(vj_error_filter)
```

When your template has wildcard **input** paths (like `tsv/{sample}.tsv`), this
also scans the filesystem the same way `run` would and lists concrete previews:
resolved inputs, outputs, and (for string work) the rendered command. You do not
need to invent `out/SOME.tsv` first unless you want that single-target view.

If many files match, only the first 20 previews print by default:

```julia
check(vj_error_filter; limit=50)
```

If the template has **no** wildcard inputs (for example only `[] => "out/{x}"`),
there is nothing to discover on disk: read the `discovery` line in the output and
use a concrete output path:

```julia
check(vj_error_filter, "out/A.tsv")
```

Now the abstract pattern becomes concrete:

```text
sample  => A
input   => tsv/A.tsv
output  => out/A.tsv
```

Nothing runs here. This is a safe way to catch spelling mistakes like
`{smaple}` before building the whole DAG.

## Workflow block

Direct `run(template)` is input-driven: existing files decide which samples run.
When outputs should drive the workflow, collect templates into a workflow block
and declare targets.

```julia
wf = @workflow "rnaseq" begin
    @step source([] => "raw/{sample}.fq") =
        "mkdir -p raw && echo {sample} > {output}"

    @step align("raw/{sample}.fq" => "out/{sample}.bam") =
        "mkdir -p out && bwa mem ref.fa {input} > {output}"

    @targets "out/{sample}.bam" sample=["A", "B"]
end
```

`@targets` expands the target pattern:

```julia
@targets "out/{sample}.bam" sample=["A", "B"]
# ["out/A.bam", "out/B.bam"]
```

The workflow block keeps the beginner-facing syntax compact. Underneath, it is a
`Workflow` object used by `plan(wf)` and `run(wf)`.

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

Use `check` while writing one wildcard step. Use direct `run(template)` when
existing inputs should drive the work. Use `@workflow` and `@targets` when
requested outputs should drive dependency resolution.

**Next:** [Fan-out and reduce](foreach-reduce.md) covers item-driven fan-out
when a collection or discovered files should drive the branches.
