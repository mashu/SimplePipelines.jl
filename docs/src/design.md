# Design

This page is for contributors. It explains how the package is put together and why.

## Concepts

There are three kinds of objects:

- **Nodes** (`AbstractNode`) are what runs: `Step` (one command or function),
  composites built by operators (`Sequence`, `Parallel`, `Fallback`, `Retry`,
  `Branch`, `Timeout`, `Force`, `Resourced`), and two *lazy* nodes that are expanded
  when they run: `ForEach` (fan-out) and `Workflow` (rules resolved against the
  filesystem).
- **Rules** (`Rule`) are templates. They are not nodes: they become `Step`s inside a
  `Workflow`. Using a rule where a node is expected wraps it in a `Workflow`.
- **Results**: `StepResult` per step and `RunResult` per run.

Everything a user writes reduces to these, so there is one execution path, one
freshness model and one way to report errors.

## Source layout

| file | contents |
|:-----|:---------|
| `shell.jl` | `Shell`, `sh`, the bash strict-mode invocation, shell quoting |
| `patterns.jl` | wildcard `Pattern`s, globbing, `discover`, `expand`, template rendering |
| `nodes.jl` | `Step` and composite nodes; constructors validate their input |
| `rules.jl` | `Rule`, `gather`, `Workflow` |
| `operators.jl` | `>>`, `&`, `\|`, `^` |
| `macros.jl` | `@step`, `@rule`, `@branch`, `@workflow` |
| `results.jl` | `StepFailure`, `StepResult`, `RunResult`, `Outcome`, `run_safely` |
| `resolve.jl` | rule resolution and pattern tracing; `plan` |
| `state.jl` | freshness and the recipe file |
| `guard.jl`, `procfs.jl`, `policy.jl` | the resource guard process: `/proc` and cgroup readers, `decide`, `guard_main` |
| `scheduler.jl` | `Need`, `Grant`, admission control (`Pool`), `Watchdog` (Timeout deadlines) |
| `link.jl` | the driver's side of the guard: starting it, the protocol, interrupts, guard loss |
| `runtime.jl` | `Runtime`, run lifecycle, memoization, recording |
| `commands.jl` | running one step's work: rendering at launch, spawning, outcomes |
| `exec_step.jl` | `exec(::Step)`: freshness, admission, eviction and requeue, results |
| `exec_nodes.jl` | `exec` for composite and lazy nodes |
| `logging.jl`, `display.jl` | progress output, `show`, `print_dag` |
| `run.jl` | `run` and `check` |

## Execution

`run` builds a `Runtime` (budgets, memo, results, state) and calls
`run_node(root, runtime, scope, input)`. `run_node` memoizes; `exec` dispatches on
the node type. A `Scope` carries what is inherited down the tree: the force flag,
default resources, the active `Watchdog`, the scheduling priority and the log tag.

Each node returns an `Outcome(ok, value)`; the value is what `>>` passes on.
`Parallel`, `ForEach` and `Job` spawn one task per branch. Tasks are cheap, and
concurrency is limited at the leaves instead: a step must `acquire!` its threads and
memory from the `Pool` before it runs. Limiting at the leaves keeps the limit global
however the graph is nested, and it cannot deadlock, because waiting branches hold
nothing.

The pool admits waiters in priority order. A step's priority is its position in the
tree (the vector of branch indices on the path to it), so earlier samples go first.
The head of the queue blocks smaller requests behind it, which prevents a large step
from starving.

### Memoization

`run_node` memoizes on `(node, input)`: the first visitor executes, concurrent
visitors wait on a channel, and later visitors get the stored outcome. A step with
declared outputs is keyed by its outputs instead, so two distinct `Step` objects
that write the same file run once. Memoizing composites, not just steps, keeps a
shared sub-DAG linear in its size. `Retry` clears failed entries before each new
attempt.

### Steps

`exec(::Step)`:

1. `staleness` decides whether the step must run, under the runtime lock.
2. `need` computes the step's `Need`: threads (at most `jobs`), the memory limit
   (declared or inherited `mem_mb`, lowered to the run's budget), the admission
   charge (the limit, or 256 MiB) and the timeout.
3. In `check` mode, the step's template is rendered to catch errors, and the step
   is recorded as planned and its outputs added to the pending set.
4. Otherwise the step acquires a `Grant`; `perform` renders the template from the
   grant, creates the output directories, checks the inputs, runs the work and
   checks the outputs, and the step releases the grant.
5. A command the guard stopped to free memory comes back as `Evicted`: the step is
   queued again with a charge based on what it used, and its third attempt runs
   alone. Otherwise `finish!` records the result, updates the recipe state, and
   removes partial outputs when the step failed.

Commands run with `run(...; wait=false)` and stream stdout and stderr through
pipes. Readers keep at most 32 MiB of stdout and the last 16 KiB of stderr. Every
leaf of a command starts in its own session (`setsid`) with the thread variables set.
Its leader pids are read while holding the iolock, so a command that exits at once
cannot be reaped before it is registered, and Ctrl-C is deferred meanwhile.

## The resource guard

Enforcement cannot live in the Julia process that runs the pipeline: a busy function
step, the garbage collector or a single thread would delay it, and processes would
outlive Julia when it is killed. The guard is a separate Julia process (one thread),
one per Julia session, started by the first `run`. It runs this package's precompiled
code (`using SimplePipelines; guard_main()`, about 0.4 s); while the package is being
precompiled or its cache is stale it compiles `guard.jl` itself at `-O0` instead. It
shares the session's process group, so Ctrl-C reaches it; it ignores SIGHUP and (on
Linux) SIGTERM, and kills everything when its stdin closes, which is how it notices
that Julia died. All its tasks defer Ctrl-C, which it handles between ticks.

The driver writes one line per event (`run`, `step`, `sess`, `done`, `term`,
`endrun`) and a reader task handles the guard's lines (`ready`, `sample`, `gone`,
`warn`, `bye`). Each tick (0.5 s, 0.1 s near a limit) the guard scans `/proc`,
attributes processes to steps by session (and setsid'd children by parent), sums
`RssAnon + RssShmem + VmSwap` per step, and reads available memory through every
cgroup level. The pure function `decide` turns that view into terminations:
per-step limits and deadlines, eviction under pressure (two samples, never a lone
step, victims that exceed their charge first), immediate kills at the critical
level, and leftover processes once a step's leaders have exited. A step is reported
`gone` only when none of its processes is left.

Samples feed the `Pool`: admission requires free CPU slots, the budget, and
`charge ≤ available − 2·reserve − unused charges of running steps`. A stale sample or
a paused guard stops admission; waiting longer than 120 s with nothing running
refuses the waiters instead of hanging. The guard fails closed: if it cannot start
or exits, the driver kills the commands itself and stops the run.

### Errors

User code runs inside `run_safely`, the package's single `try`/`catch`. It turns
exceptions into failed outcomes carrying a `StepFailure`, and on Ctrl-C it calls
`interrupt_all!` (refuse waiters, wake memo waiters, save state, kill every command)
before rethrowing. The guard has no exception boundary at all: an error ends it, and
the run stops. Locked sections defer Ctrl-C (`with_lock`) instead of using `finally`. Every other function reports
problems through return values: `Unresolved` in resolution, `Outcome(false, …)`
during execution.

## Resolution

`plan_workflow` turns a `Workflow` into a DAG of `Job`s (a step plus the jobs that
produce its inputs).

- **Concrete paths** resolve backward. Candidate rules are sorted by specificity
  (characters captured by wildcards), and the first whose inputs resolve wins. A
  path no rule can build is a `Leaf` if it exists, and `Unresolved` with a reason
  otherwise. Results are cached per path, so shared inputs give shared jobs.
- **Patterns** (pattern targets, `gather`, output-less rules) are *traced*. A
  pattern is unified with every rule output that can produce it: its wildcards are
  replaced by probe tokens made of control characters, which cannot occur in rule
  literals. The rule's inputs are then rewritten in terms of the pattern's
  wildcards, traced recursively, and the resulting assignments natural-joined.
  Patterns no rule produces are globbed on disk. Globbing walks only directories
  the pattern can match.

## Latency

The package loads in about 0.2 s. A first `run` adds about 0.5 s, most of it starting
the guard; later runs add about 20 ms.

- Node, result and outcome types are not parametric, and the runtime files are
  compiled with `@nospecialize`. User functions and values come in endless types;
  specializing the runtime on them would recompile it for every new closure. The
  traversal uses dynamic dispatch either way, so this costs nothing at run time.
- Locks are taken through `with_lock(f, lock, args...)` with named functions rather
  than capturing closures, whose types would differ for every value.
- A PrecompileTools workload runs the common paths (steps, rules, gather, check,
  ForEach, control flow, display) during precompilation.

Per-step overhead is about 30 µs for a Julia function. A shell step costs what
starting bash costs (a few ms), plus about 1 ms for the guard's round trip.

## Conventions

The code follows the repository's standards (`.cursor/rules/julia-standards.mdc`):
multiple dispatch instead of `isa` branching, no leading underscores, no `try` beyond
`run_safely`, and small named functions. Type parameters are used where something
dispatches on them (`ForEach{S}` on its source, `Candidate{E}`). Fields that hold
arbitrary user values (`StepResult.result`, `Outcome.value`) are deliberately
untyped, for the latency reason above.
