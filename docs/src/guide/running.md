# Running and freshness

## `run`

```julia
results = run(pipeline; jobs=16, mem_mb=64_000, force=false, verbose=true)
```

| keyword | default | meaning |
|:--------|:--------|:--------|
| `jobs` | CPUs this process may use | CPU slots; a step uses as many as its `threads` |
| `mem_mb` | 80% of the memory this process may use | memory budget in MiB for the steps running at once |
| `min_free_mb`, `nice`, `guard` | | see [Resource limits and safety](resources.md) |
| `force` | `false` | run every step, even if up to date |
| `verbose` | `true` | print progress |
| `state_path` | `".pipeline_state"` | where recipes of built files are remembered |

For rules and workflows, `run` also takes `targets` and `filter` (see
[rules](rules.md#Targets)).

When a step fails, its branch stops; independent branches keep going, so one bad
sample does not cost you the others. Rerun after fixing it: everything that
succeeded is skipped.

## Results

`run` returns a [`RunResult`](@ref), a vector of [`StepResult`](@ref)s in
completion order, with:

- `success`, also `success(results)`: whether the pipeline as a whole succeeded;
- `value`: the value of the pipeline (for example, what a final function returned);
- `duration` in seconds.

Each `StepResult` has `label` (such as `align[sample=A]`), `status` (`:done`,
`:skipped`, `:failed`), `success`, `duration`, `peak_mb` (peak memory of its
command), `inputs`, `outputs` and `result`: stdout for a shell step, the return
value for a function, or a [`StepFailure`](@ref) when the step failed.

```julia
failed = filter(r -> !r.success, results)
foreach(r -> println(r.label, ": ", r.result.message), failed)
```

A `StepFailure` has a `kind` (`:process_failed`, `:memory_limit`, `:timed_out`,
`:exception`, `:missing_input`, `:missing_output`, …), a `message` that says what to
change, and a `detail` with the end of stderr or the top of the Julia stack trace.

## `check`

```julia
check(pipeline)
```

`check` takes the same arguments as `run` but runs nothing. It expands
[`ForEach`](@ref) nodes, resolves workflows, and reports each step as `:planned`
with the reason it would run, `:skipped` if up to date, or `:failed` when something
is wrong, such as an input that does not exist and that no earlier step produces.

```text
═══ Check: pipeline ═══
▶ align[sample=C]  (missing output bam/C.bam)
▶ calls  (input bam/C.bam will be rebuilt)
═══ 2 to run, 4 up to date, 0 failed in 0.01s ═══
```

[`print_dag`](@ref) shows the structure, with ForEach nodes expanded and workflows
resolved into their concrete steps.

## Freshness

A step with declared outputs is **up to date**, and skipped, when:

1. every output exists,
2. no input is newer than the oldest output, and
3. the outputs were last made by the same recipe: the same command template, the
   same inputs and, for a rule, the same wildcard values.

The third rule is what makes editing a command rerun the affected steps, and what
makes a merge rerun when a sample is added. Recipes are remembered in
`.pipeline_state`, a small text file with one line per output. If a file has no
record, because it was made before the first run or the state file was deleted,
the first two rules decide, and the record is added. Julia functions are tracked
by their inputs and outputs only; after changing a function, use `force=true` or
[`Force`](@ref).

Steps **without** declared outputs always run. Their value, such as the result of
a function, is therefore always fresh when passed downstream.

When a step fails, the output files it wrote are deleted, so a partial file never
passes as finished. [`clear_state!`](@ref) forgets the recorded recipes and
[`is_fresh`](@ref) tells you whether a step would be skipped.

## Scheduling

The scheduler admits a step only when its resources fit:

- `jobs` is the number of CPU slots. A step takes `threads` slots (default 1), so
  at most `jobs` threads are busy, however deeply parallel nodes are nested. A step
  asking for more than `jobs` gets `jobs`.
- `mem_mb` caps the memory charged to running steps: their `mem_mb`, or 256 MiB for
  a step that declares none. A step also waits until that much memory is actually
  free. A step that does not fit runs alone rather than waiting forever.

```julia
@rule sort_bam("bam/{s}.bam" => "sorted/{s}.bam"; threads=4, mem_mb=8000) =
    "samtools sort -@ {threads} -m 1500M -o {output} {input}"
```

`mem_mb` is also the step's memory limit, enforced while it runs; see
[Resource limits and safety](resources.md).

When several steps are waiting, the one earliest in the pipeline (for example, the
next step of the first sample) goes first. Samples are finished a few at a time
rather than all first steps being run first.

Shell commands run concurrently even when Julia has a single thread. Julia function
steps run on Julia threads, so start Julia with `-t auto` if they do heavy work, or
better, run heavy Julia code with [`julia`](@ref) so it can be limited.

## Output

- A command's stdout is captured up to 32 MiB. A command that prints more is
  stopped and fails with `:stdout_limit`: redirect large output to a file and
  declare it as an output. Only the last 16 KiB of stderr is kept.
- Values passed between steps stay in memory until `run` returns, as part of the
  results. For large data, write files and pass paths.

**Next:** [Resource limits and safety](resources.md).
