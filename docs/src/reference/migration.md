# Migrating from 0.3

Version 0.4 is a redesign around one data model (values flow with `>>`, files are
declared on steps) and one resolution model for wildcards. Most pipelines need small
edits.

## Removed, and what to use instead

| 0.3 | 0.4 |
|:----|:----|
| `a \|> f`, `Reduce(f, a)` | `a >> f`: a parallel node's value is the vector of branch values |
| `a .>> b` | put `b` inside the `ForEach` block: `ForEach(xs) do x; step(x) >> b end` |
| `a >>> b` | `ForEach` branches no longer receive the item as input; use the block argument |
| `RuleChain` (`r1 \|> r2`), `RuleValuePipe` | `r1 >> r2` (a [`Workflow`](@ref)), then `>> f` |
| `resolve(rules, targets)` | `plan(rules...; targets)` or `run(rules...; targets)` |
| `check(rule, target)`, `explain(wf; target)` | `plan(rule; targets=target)` and `print_dag`, or `check(x)` for a dry run |
| `run(x; dry_run=true)` | `check(x)` |
| `@targets "p/{s}" s=[...]` | `expand("p/{s}"; s=[...])`, or a pattern target |
| `wildcards_filter=` | `filter=` |
| `sh(() -> cmd)` | `() -> sh(cmd)`: a function returning a command runs it |
| `sh_pipe(a, b)` | `pipeline(a, b)`, or one `sh"a \| b"` |
| `shell_raw"..."` | `raw"..."`; `sh"..."` already leaves `$` alone |
| `fe(...)` | `ForEach(...)` |
| `FilePath`, `materialize`, `materialize_table`, auto-spill | values are passed as they are; write large data to files and pass paths |
| `memory_budget_mb`, `thread_budget` | `mem_mb`, and `jobs` counts threads |
| `report=` keyword | inspect the returned `RunResult` |
| `NoWork`, `Pipe`, `SameInputPipe`, `BroadcastPipe`, `Resources`, `Resourced` | internal or gone |
| `Step{F}`, `StepResult{...}` type parameters | plain `Step`, `StepResult` |

## Behaviour changes

- **Steps without declared outputs always run.** In 0.3 they were skipped on later
  runs and passed the string `"up to date (not re-run)"` downstream.
- **A changed command reruns the step**, even when the outputs are newer than the
  inputs. The state file is now a small text file; the old 8 MB binary file is
  ignored and replaced.
- **Shell commands use `bash -e -o pipefail`.** A failure inside a pipe now fails the
  step. Commands like `zcat x.gz | head` need `set +o pipefail;` in front.
- **Outputs of a failed step are deleted**, and output directories are created
  automatically, so `mkdir -p` in commands is no longer needed.
- **Workflows rebuild stale targets.** In 0.3 an existing target was treated as done
  even when its inputs had changed.
- **Function steps adapt to their arity.** A zero-argument function after another
  step is called without arguments instead of failing.
- **A function rule receives one argument per declared input**, then the outputs
  (`f(input, output)` as before, `f(r1, r2, output)` for two inputs). The
  `(inputs, outputs, wildcards)` builder form is gone; use a string template with
  `{wildcard}` placeholders, or derive values from the paths.
- **`jobs` is a global cap** on busy CPU threads (default: all CPU threads), counting
  each step's `threads`. In 0.3 it limited branches per parallel node, so nested
  parallelism multiplied.
- **`run` returns a `RunResult`.** It still behaves as a vector of `StepResult`s,
  and adds `success`, `value` and `duration`. `StepResult` gained `label` and
  `status`.
- **Errors in user code are reported as failed steps** instead of being thrown, for
  example a `ForEach` block that returns `nothing`, or a `Branch` condition that
  throws.
- **The operators do not change Base behaviour any more.** `` `a` & `b` `` is Base's
  `AndCmds` again. Use `sh"a" & sh"b"`, or wrap commands in steps.
- **Julia 1.10** is the minimum version.
