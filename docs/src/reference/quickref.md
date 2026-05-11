# Quick reference

One-page lookup for syntax and APIs. Narrative explanations live in the [user guide](../guide/steps-and-shell.md).

## Steps

| Syntax | Meaning |
|--------|---------|
| `@step name = sh"cmd"` | Shell command |
| `@step name = sh("\$(var)")` | Shell with Julia interpolation |
| `@step name = sh(cmd_func)` | Run-time command, e.g. `sh(() -> "tool " * ref)` |
| `shell_raw"..."`, `shell_raw"""..."""` | Literal shell (`\$VAR` not expanded by Julia) |
| `@step name = () -> expr` | Julia function step |

## Operators

| Op | Meaning | Example |
|----|---------|---------|
| `>>` | Sequence; passes output into the next **function** step | `a >> b >> c` |
| `&` | Parallel | `a & b & c` |
| `\|` | Fallback if primary fails | `a \| b` |
| `^n` | Retry up to *n* times | `a^3` |
| `\|>` | Pipe outputs into RHS | `a \|> b` |
| `>>>` | Same input to both sides | `a >>> b` |
| `.>>` | Broadcast: attach RHS to each branch | `fe .>> step` |

## Sequencing and piping

When the left side has **multiple** outputs (`ForEach`, `Parallel`), `>>`, `|>`, and `.>>` differ:

| Left | `>>` | `|>` | `.>>` |
|:-----|:-----|:-----|:------|
| Single | one value → next | one value | one value |
| Multi | **last** only | **vector** of all | **per branch** |

## Control-flow helpers

| API | Role |
|-----|------|
| `Timeout(a, secs)` | Fail if overrun |
| `Branch(cond, a, b)` | Conditional subgraph (`cond()` or `cond(upstream)` after `>>`) |
| `@branch flag[] a b` | Sugar for `Branch(() -> flag[], a, b)` |
| `ForEach(xs) do ... end` | Fan-out over collection |
| `ForEach("pat/{x}.ext") do ... end` | Fan-out from glob pattern |
| `Reduce(f, a & b)` | Merge parallel outputs |
| `Retry(a, n; delay=d)` | Retries with optional delay |

## Execution

| Call / field | Role |
|--------------|------|
| `run(p)` / `p \|> run` | Execute; returns result vector |
| `run(..., verbose=false)` | Quiet (no command echo) |
| `run(..., dry_run=true)` | Preview only |
| `run(..., report=(res; pipeline) -> …)` | Hook after run, before saving state |
| `.success`, `.duration`, `.result`, `.inputs`, `.outputs` | Per-step outcome fields |
| `materialize_table(FilePath(p))` | After `using CSV, DataFrames`: CSV → `DataFrame` |

## Utilities

| API | Role |
|-----|------|
| `print_dag(node)` | Print structure |
| `count_steps(node)` | Step count |
| `steps(node)` | Flat list of steps |

## Full detail

Narrative walkthroughs: [User guide](../guide/steps-and-shell.md) and [Examples](../examples/basics.md). Generated docstrings: [API](../api.md).
