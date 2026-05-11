# SimplePipelines.jl

*Minimal, type-stable DAG pipelines for Julia*

SimplePipelines is for small-to-medium workflows where the shape of the work
should be visible in Julia code. A step can be a shell command or a Julia
function. Operators connect steps into a DAG. `run` executes that DAG and returns
one result per step.

The central idea is deliberately small:

```julia
step_a >> step_b        # run in order
step_a & step_b         # run branches in parallel
run(pipeline)           # execute and collect StepResult values
```

For a complete first example, start with [Getting started](guide/getting-started.md).

## Learning Paths

If you are new, read the first path in order. If you already know the basics,
jump to the shape that matches your workflow.

### First Pipeline

[Getting started](guide/getting-started.md) -> [Steps and shell](guide/steps-and-shell.md) -> [Composing pipelines](guide/composition.md) -> [Running and inspecting](guide/running-and-results.md)

This path teaches one concrete DAG before introducing advanced features.

### Pattern/Rule Workflow

[Rules and diagnostics](guide/rules-and-diagnostics.md) is the entry point when
your files look like `raw/{sample}.fq` -> `out/{sample}.bam`. It starts with
`check(rule)` so you can see wildcard values before running anything.

### Cookbook And Reference

Use [Examples](examples/basics.md) for runnable recipes, and the
[quick reference](reference/quickref.md) for compact syntax lookup.

Full generated docstrings live on the [API page](api.md).
