# Choosing operators, Workflow, and resources

Short map from “what you need” to the DSL piece to use.

## Data flow between steps

| Goal | Use |
|:-----|:----|
| Run A then B; B gets A’s **single** output (or last branch only if A had many) | `>>` (sequence; see [Composing pipelines](composition.md)) |
| Run A then B; B is a **function step** receiving **all** branch outputs as one vector | `\|>` (pipe) |
| Run A then B **once per branch** of A (each B sees one branch’s output) | `.>>` (broadcast `>>`) |
| Run A then B; B gets the **same context** as A (e.g. same ForEach wildcard), not A’s stdout | `>>>` (same-input pipe) |

When the left side has **one** successful path, `>>`, `|>`, `.>>`, and `>>>` behave the same for “what B receives” until you introduce `Parallel` / `ForEach` with multiple branches — then the table in [Composing pipelines](composition.md) applies.

## Rules vs hand-built DAG

| Situation | Prefer |
|:----------|:-------|
| Many targets built from patterns (`data/{id}.fq` → `out/{id}.bam`), shared deps, Make-like resolve | [`Workflow`](@ref) + [`@rule`](@ref) + [`resolve`](@ref) / [`plan`](@ref) |
| One-off script, few steps, explicit graph in code | `>>` / `&` on [`Step`](@ref)s directly |
| Inspect what would run without executing | `run(..., dry_run=true)` or [`print_dag`](@ref) on [`plan(wf)`](@ref) |

[`Workflow`](@ref) is a registry of rules and default target strings; `run(wf)` calls [`plan`](@ref) then the same [`run`](@ref) as a [`Pipeline`](@ref).

## Memory and parallelism

| Situation | Use |
|:----------|:----|
| Steps that reserve a lot of RAM or threads; cap **concurrent** use | [`with_resources`](@ref) on those nodes + `run(..., memory_budget_mb=..., thread_budget=...)` |
| Default laptop / workstation run | Defaults (`jobs`, `memory_budget_mb`, `auto_spill`) are usually enough |

See [Running and inspecting](running-and-results.md) for spill behaviour and the optional `report` callback on [`run`](@ref).
