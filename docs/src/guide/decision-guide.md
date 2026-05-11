# Choosing the right shape

SimplePipelines gives you two complementary ways to build a DAG. You can write
the graph directly with operators, or you can describe file patterns with rules.
Both end up as the same kind of runnable pipeline.

## Start With The Shape Of Your Problem

If you have a handful of concrete steps, write them directly:

```julia
prepare >> sort_file >> (count_lines & count_bytes)
```

This is the clearest form when the filenames are fixed and the graph is small.

If you have repeated file patterns, use rules:

```julia
@rule align("raw/{sample}.fq" => "out/{sample}.bam") =
    "bwa mem ref.fa {input} > {output}"
```

Rules are better when the interesting question is "what target do I want?", and
the package should work backward to find dependencies.

## Use Diagnostics Before Running

For one rule, use `check`:

```julia
check(align, "out/A.bam")
```

For a composed workflow, use `explain`:

```julia
explain(wf; target="out/A.bam")
```

Those commands are there so you can inspect wildcard values and dependency
chains without executing shell commands.

## Choose Operators Only When Needed

Most explicit DAGs start with `>>` and `&`. Add the value-passing variants only
when a function step needs a specific branch result:

- use `|>` when one function should receive all branch outputs;
- use `.>>` when the next step should run once per branch;
- use `>>>` when two steps inside a branch should receive the same original input.

The [Composing pipelines](composition.md) page shows these in context. If you
find yourself checking that table often, the graph may be clearer as a rule-based
workflow or a `ForEach`.

## Resource Hints Are Optional

The default run is conservative: fan-out is bounded, large results spill to disk,
and declared resource hints can limit concurrent memory or thread usage. You
only need [`with_resources`](@ref) for steps that you know are heavy:

```julia
heavy = with_resources(step; mem_mb=4_000, threads=4)
run(heavy & other)
```

By default, `run` already caps live parallel branches with `jobs`, caps declared
memory at half of system RAM, and caps declared threads at the same host-aware
limit as `jobs`. Pass explicit budgets only when you want stricter or looser
limits.

For details on results and spill behavior, see [Running and inspecting](running-and-results.md).
