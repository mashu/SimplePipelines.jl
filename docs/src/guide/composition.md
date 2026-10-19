# Composing pipelines

Four operators draw the graph. Any shell command (`sh"..."`, a `String` or a `Cmd`)
or function next to a node is lifted to a step automatically.

| operator | meaning | node |
|:---------|:--------|:-----|
| `a >> b` | run `b` after `a` succeeds | [`Sequence`](@ref) |
| `a & b` | run `a` and `b` at the same time | [`Parallel`](@ref) |
| `a \| b` | run `b` only if `a` fails | [`Fallback`](@ref) |
| `a^n` | retry `a` up to `n` times | [`Retry`](@ref) |

```julia
fetch >> (qc & align) >> report
download^3 | use_cached_copy
```

!!! warning "Precedence"
    Julia gives `>>` higher precedence than `&`, and `&` higher than `|`. So
    `a & b >> c` means `a & (b >> c)`: write `(a & b) >> c` when `c` should wait
    for both.

Composition is cheap and has no side effects: nothing runs until
[`run`](@ref) is called. A node can be reused in several places; a node that
appears in several branches of one pipeline runs once, and every branch sees its
result.

## Data flow

Every node has a *value*, and `>>` passes it to the next node:

| node | value |
|:-----|:------|
| step with declared outputs | the output paths (`Vector{String}`) |
| other shell step | its stdout (`String`) |
| other function step | its return value |
| `a & b`, [`ForEach`](@ref) | the vector of branch values, in order |
| `a >> b` | the value of `b` |
| [`Workflow`](@ref) | the paths of its targets |

A function step without declared paths receives the upstream value when it takes
one argument, and is called with none otherwise:

```julia
half = @step half = () -> 21
double = @step double = x -> 2x
announce = @step announce = () -> println("done")

run(half >> double >> announce)        # announce takes no argument: fine
run(half >> double).value              # 42
```

Parallel branches produce a vector, which makes reductions direct:

```julia
sizes = ForEach(["a.txt", "b.txt"]) do f
    @step size = () -> filesize(f)
end
run(sizes >> sum).value                # total size
```

For file-based pipelines the paths are usually all you need: a step after another
one simply reads the files it declares.

## Nodes as values

All composition results are ordinary Julia values, so pipelines can be built with
ordinary code:

```julia
per_chrom = [@step call(["sample.bam"] => "calls/chr$c.vcf") =
                 "bcftools mpileup -r chr$c -f ref.fa {input} | bcftools call -mv -o {output}"
             for c in 1:22]

pipeline = align >> Parallel(per_chrom) >> merge_vcfs
```

`Sequence(nodes...)` and `Parallel(nodes...)` (or a vector of nodes) are the
function forms of `>>` and `&`.

**Next:** [Wildcard rules and workflows](rules.md).
