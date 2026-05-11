# Rules and diagnostics

Rules are easiest to learn one at a time. A rule says how output paths are made
from input paths, using `{wildcard}` names that are inferred from the requested
target.

```julia
align = @rule align("raw/{sample}.fq" => "out/{sample}.bam") =
    "bwa mem ref.fa {input} > {output}"
```

Before composing a workflow, inspect the rule shape:

```julia
check(align)
```

Then test one concrete target:

```julia
check(align, "out/A.bam")
```

This shows the wildcard values, concrete inputs and outputs, and rendered shell
command without running anything.

## Workflow block

When the rules look right, collect them into a workflow:

```julia
wf = @workflow "rnaseq" begin
    @rule source([] => "raw/{sample}.fq") =
        "mkdir -p raw && echo {sample} > {output}"

    @rule align("raw/{sample}.fq" => "out/{sample}.bam") =
        "mkdir -p out && bwa mem ref.fa {input} > {output}"

    @targets "out/{sample}.bam" sample=["A", "B"]
end
```

`@targets` is the beginner-friendly form of [`expand`](@ref). It returns the
same vector of concrete target strings, so you can also use it with `push!(wf, ...)`.

## Explain the DAG

Use [`explain`](@ref) when multiple rules are connected and you want to see why
a target resolves the way it does:

```julia
explain(wf; target="out/A.bam")
print_dag(plan(wf))
run(wf)
```

`check` is local to one rule. `explain` follows dependencies across the workflow.
