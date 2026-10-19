# Quick reference

## Steps

| syntax | meaning |
|:-------|:--------|
| `@step name = sh"cmd"` / `= "cmd"` | shell command (`bash -e -o pipefail`) |
| `@step name = sh("cmd $(x)")` | with Julia interpolation |
| `@step name = () -> expr` | Julia function |
| `@step name = f(args)` | call deferred until the step runs |
| `@step name(in => out) = "tool {input} > {output}"` | declared paths and a template |
| `@step name([a, b] => [c]; threads=4, mem_mb=8000, timeout=600) = ...` | several paths, limits (MiB, seconds) |
| `@step name(in => out) = julia("script.jl {input} {output}")` | Julia script in a separate, limited process |
| `@step name(in => out) = (i, o) -> ...` | function receiving the paths |
| `@step name(in) = ...` | inputs only |
| `Step(name, work, inputs, outputs; threads, mem_mb, timeout)` | constructor form |

Placeholders: `{input}`, `{output}`, `{input[i]}`, `{output[i]}`, `{threads}`,
`{mem_mb}`, `{wildcard}`; `{{word}}` is a literal `{word}`. `{threads}` and `{mem_mb}`
are filled in when the step starts, with what it was granted.

## Composition

| syntax | meaning | value |
|:-------|:--------|:------|
| `a >> b` | then | value of `b` (receives value of `a`) |
| `a & b` | in parallel; write `(a & b) >> c` | `[va, vb]` |
| `a \| b` | fallback | value of the one that succeeded |
| `a^n`, `Retry(a, n; delay)` | retry | |
| `Branch(cond, a, b)`, `@branch cond a b` | conditional | |
| `Timeout(a, seconds)` | deadline; kills commands | |
| `Force(a)` | ignore freshness | |
| `with_resources(a; threads, mem_mb, timeout)` | default limits | |
| `ForEach(items) do x ... end` | one branch per item | vector |
| `ForEach("dir/{w}.ext") do w ... end` | one branch per file | vector |
| `Sequence(nodes...)`, `Parallel(nodes...)` | function forms | |

## Rules and workflows

| syntax | meaning |
|:-------|:--------|
| `@rule name("in/{s}.x" => "out/{s}.y"; threads) = "cmd {input} > {output}"` | rule |
| `@step name("in/{s}.x" => "out/{s}.y") = ...` | the same (wildcards make a rule) |
| `gather("bam/{s}.bam")` | input collecting every value of `{s}` |
| `r1 >> r2 >> r3` | merge rules into a [`Workflow`](@ref) |
| `Workflow(rules_and_steps...; targets, filter, name)` | explicit workflow |
| `@workflow "name" begin ... end` | workflow from a block |
| `run(wf; targets="out/{s}.y")` | pattern target: every `s` with sources |
| `run(wf; filter = w -> w.s != "ctl")` | drop wildcard values |
| `expand("out/{s}.y"; s=["A", "B"])` | concrete paths |
| `discover("raw/{s}_R1.fq.gz")` | wildcard values on disk |
| `plan(wf; targets)` | concrete DAG, without running |

## Running and inspecting

| call | meaning |
|:-----|:--------|
| `run(x; jobs, mem_mb, min_free_mb, nice, guard, force, verbose, state_path)` | run; returns a [`RunResult`](@ref) |
| `check(x; ...)` | what would run and why; runs nothing |
| `print_dag(x)` | tree with paths and commands |
| `success(r)`, `r.value`, `r[i]` | overall success, pipeline value, step results |
| `r[i].label`, `.status`, `.result`, `.duration`, `.peak_mb` | per-step fields |
| `steps(x)`, `count_steps(x)` | steps of a node |
| `is_fresh(step)`, `clear_state!()` | freshness state |
