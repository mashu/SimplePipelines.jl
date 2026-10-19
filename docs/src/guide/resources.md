# Resource limits and safety

A pipeline must never freeze your computer. SimplePipelines therefore runs every
command under a **resource guard**: a small separate process, started by the first
`run`, that measures each command the pipeline starts and stops the ones that use too
much memory or time. It works even while Julia itself is busy, and it kills everything
the pipeline started if Julia is interrupted or dies. There is nothing to set up.

## Declaring what a step needs

```julia
@rule sort_bam("bam/{s}.bam" => "sorted/{s}.bam"; threads=4, mem_mb=8000, timeout=3600) =
    "samtools sort -@ {threads} -m 1500M -o {output} {input}"
```

- `threads`: CPU slots the step takes (default 1). `{threads}` in the template is
  this number, and its commands see `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`,
  `MKL_NUM_THREADS`, `RAYON_NUM_THREADS`, `JULIA_NUM_THREADS`, `GOMAXPROCS` and
  similar variables set to it, so multithreaded libraries do not start one thread
  per core in every step. These variables are advice that a tool may ignore; pass
  `{threads}` to tools that take a thread flag.
- `mem_mb`: the step's memory limit in MiB. The step starts only when that much
  memory is free, and it is terminated if its processes use more (`:memory_limit`).
  `{mem_mb}` in the template is the same number. Give the tool about 80% of it,
  because tools need memory beyond their own setting, and per-thread settings
  multiply: `samtools sort -@ 4 -m 1500M` may use 4 × 1500 MiB.
- `timeout`: seconds. A step still running after that long is terminated
  (`:timed_out`).

[`with_resources`](@ref)`(node; threads, mem_mb, timeout)` sets defaults for every
step inside `node` that does not declare its own.

## What the guard does

| situation | what happens |
|:----------|:-------------|
| a command uses more memory than its `mem_mb` | terminated within a fraction of a second; the step fails with `:memory_limit` |
| a command runs longer than its `timeout` | SIGTERM, then SIGKILL 5 s later; the step fails with `:timed_out` |
| a [`Timeout`](@ref) node expires | its commands are terminated; its steps that have not started never start |
| free memory gets low | new steps wait (`⏸ waiting for memory`) |
| free memory keeps falling | the pipeline's own commands are stopped, those using more than they declared first, and queued again when memory is free (`↺`); a step that does not fit even when it runs alone fails with `:out_of_memory` |
| other programs use the memory | nothing is stopped; the run waits and says so |
| a step leaves background processes running | they are terminated and the step fails |
| Ctrl-C, the terminal is closed, Julia is killed | every command the pipeline started is killed |

Memory is measured per step, summed over every process the step starts, as memory
the system cannot reclaim: anonymous memory, shared memory and swap. Page cache from
reading large files does not count. Free memory takes container and cgroup limits
into account (Docker, Slurm, Kubernetes, systemd), so the pipeline stays inside the
allocation of a cluster job.

Steps that declare no `mem_mb` have no per-step limit, but they are measured and
protected like the others: they are charged 256 MiB when they start, and they are
the first to be stopped when memory runs out because they use more than they
declared. Declare `mem_mb` for memory-hungry tools so they start only when there is
room for them.

`StepResult.peak_mb` holds each command's peak memory, and the log shows it:
`✓ align[s=A] (12.3s, peak 3.2 GiB)`. Use it to choose `mem_mb`.

## Run settings

```julia
run(pipeline; jobs=16, mem_mb=64_000, min_free_mb=0, nice=10, guard=true)
```

| keyword | default | meaning |
|:--------|:--------|:--------|
| `jobs` | CPUs this process may use | CPU slots; a step takes as many as its `threads` |
| `mem_mb` | 80% of the memory this process may use | budget for the steps running at once, in MiB (0: none); a step's `mem_mb` above it is lowered to it |
| `min_free_mb` | automatic: 5% of memory, between 256 and 2048 MiB | memory to keep free: below twice this new steps wait, below it commands are stopped |
| `nice` | `10` | priority of commands (0–19), so the computer stays responsive |
| `guard` | `true` | `false` runs without the guard: no memory limits, timeouts or cleanup |

The CPU and memory defaults follow the process's CPU affinity and cgroup limits, so
`jobs` and `mem_mb` are right inside containers and cluster jobs.

## Julia code

Julia function steps run inside your Julia session. Their `mem_mb` is reserved when
they start but cannot be enforced, and they cannot be stopped. Put heavy Julia work
in a script and run it with [`julia`](@ref):

```julia
@step counts("reads.tsv" => "counts.tsv"; threads=4, mem_mb=8000) =
    julia("--project=scripts scripts/count.jl {input} {output}")
```

The script runs in a new Julia process: this Julia, the active project,
`--threads={threads}` and a `--heap-size-hint` below `mem_mb`, followed by your
arguments (your flags win). It is limited, measured and stopped like any command.
The paths are in `ARGS`.

Commands started with `run(...)` inside a Julia function belong to your Julia
session and are not tracked; return the command from the function instead, and the
step runs it.

## Rules for commands

- A step ends when **all** its processes have exited. If the main command finishes
  and leaves processes behind, they are terminated and the step fails: end scripts
  that start background jobs with `wait`.
- Commands have no terminal: `stdin` is `/dev/null`, so a tool waiting for input
  fails instead of hanging.
- Commands run at `nice` 10 and are the system's preferred victims if it runs out of
  memory despite everything, so Julia and your desktop survive.

## Limits

- **Linux** has every guarantee above. On **macOS** timeouts, cleanup and Ctrl-C
  work, but memory is not measured. On **Windows** only `guard=false` is supported.
- Memory is sampled every 0.5 s (0.1 s near a limit), so a command that allocates
  very fast can overshoot briefly. If the system runs out of memory in that moment,
  its out-of-memory killer picks the command, which fails with `:out_of_memory`.
- A program that turns itself into a daemon (a new session and a double fork in less
  than a sampling interval) escapes the guard.
- Files written to memory-backed filesystems such as `/dev/shm` count as used memory
  but belong to no step.
- Several `run`s at the same time in one Julia session share the guard but not
  their budgets.
- The guard uses about 110 MiB and starts in about 0.3 s, once per Julia session. It
  stops with Julia.

## Failures

| kind | meaning |
|:-----|:--------|
| `:memory_limit` | the step used more than its `mem_mb`; raise it, or lower the tool's own memory setting |
| `:out_of_memory` | the computer ran out of memory for this step, or the system's out-of-memory killer stopped it |
| `:timed_out` | the step's `timeout` or an enclosing `Timeout` expired |
| `:process_failed` | the command failed, left background processes running, or was killed from outside |
| `:stopped` | the run was interrupted, or the guard exited unexpectedly |
| `:template` | a placeholder cannot be filled, such as `{mem_mb}` without a declared `mem_mb` |

If the guard cannot start or exits unexpectedly, the run stops and kills its
commands, because steps must not run without protection.

**Next:** [Examples](../examples/basics.md).
