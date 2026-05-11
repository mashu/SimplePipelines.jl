# SimplePipelines.jl

*Minimal, type-stable DAG pipelines for Julia*

A **step** is a shell command or Julia function. **Operators** connect steps into a graph: `>>` (order), `&` (parallel), `|>` (pipe), and others. **[`run`](@ref)** executes the graph once; each step yields a [`StepResult`](@ref) (success, duration, outputs).

| Ad-hoc Julia/shell glue | Same thing as a pipeline |
|---------------------------|----------------------------|
| Run A, then B, then C | `a >> b >> c` |
| Two branches, then merge | `(a & b) >> merge` |
| Retry or alternate on failure | `step^3`, `x \| y`, `Branch(…)` |
| Shell + Julia in one workflow | Same operators for `sh"…"` and function steps |

```julia
using SimplePipelines

fetch     = @step fetch = sh"echo data > file.txt"
analyze_a = @step a = sh"wc -c file.txt"
analyze_b = @step b = sh"wc -l file.txt"
report    = @step r = sh"echo done"

fetch >> (analyze_a & analyze_b) >> report |> run

run(sh"echo one" >> sh"echo two")
run((sh"A" & sh"B") >> sh"merge")
```

**[Steps and shell](guide/steps-and-shell.md)** · **[Composing pipelines](guide/composition.md)** · **[Rules and diagnostics](guide/rules-and-diagnostics.md)** · **[Choosing operators & Workflow](guide/decision-guide.md)** · **[Control flow](guide/control-flow.md)** · **[Fan-out and reduce](guide/foreach-reduce.md)** · **[Running and inspecting](guide/running-and-results.md)** · **[Examples](examples/basics.md)** · **[Quick reference](reference/quickref.md)** · **[Public API policy](reference/public-api.md)** · **[API](api.md)**
