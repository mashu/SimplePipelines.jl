# Getting started

This page builds a small pipeline: make a data file, sort it, then compute two
summaries in parallel.

```text
prepare ── sort_file ──┬── count_lines
                       └── count_bytes
```

## Steps declare what they read and write

```julia
using SimplePipelines

prepare = @step prepare([] => "data.txt") = "seq 3 -1 1 > {output}"

sort_file = @step sort_file("data.txt" => "sorted.txt") =
    "sort {input} > {output}"

count_lines = @step count_lines("sorted.txt" => "lines.txt") =
    "wc -l < {input} > {output}"

count_bytes = @step count_bytes("sorted.txt" => "bytes.txt") =
    "wc -c < {input} > {output}"
```

`name(inputs => outputs) = command` declares the files. In the command, `{input}`
and `{output}` are replaced by those paths. Declaring files is what makes the
pipeline incremental and safe: the step is skipped when its outputs are up to date,
its output directory is created for it, and it fails clearly if an input is missing
or an output was not produced.

## Compose with `>>` and `&`

```julia
pipeline = prepare >> sort_file >> (count_lines & count_bytes)
```

`>>` means "then" and `&` means "at the same time". Keep the parentheses: in Julia,
`>>` binds tighter than `&`.

## Look before you run

```julia
print_dag(pipeline)      # the structure, with paths and commands
check(pipeline)          # what would run, and why
```

`check` runs nothing. It reports each step as *planned* (with the reason, such as
`missing output sorted.txt`), *up to date*, or as a problem, such as an input that
no step produces.

## Run

```julia
results = run(pipeline)
```

```text
═══ Pipeline: pipeline ═══
▶ prepare
    $ seq 3 -1 1 > data.txt
✓ prepare (0.01s)
▶ sort_file
    $ sort data.txt > sorted.txt
✓ sort_file (0.01s)
...
═══ 4 done, 0 up to date, 0 failed in 0.05s ═══
```

`run` returns a [`RunResult`](@ref): a vector with one [`StepResult`](@ref) per
step, plus the overall `success` and the pipeline's `value`.

```julia
success(results)                   # true
results[2].label, results[2].status
```

Run it again and every step is skipped: the outputs exist and nothing changed.
Touch `data.txt`, or edit a command, and exactly the affected steps run again.

## Julia functions are steps too

A function can be a step. With declared paths it receives them as arguments; the
last step below receives the value of the previous one.

```julia
total = @step total("lines.txt" => "report.txt") = (lines, report) ->
    write(report, "lines: " * strip(read(lines, String)))

show_report = @step show_report = paths -> read(only(paths), String)

run(pipeline >> total >> show_report).value     # "lines: 3"
```

## Many samples: rules

Real pipelines repeat the same steps for many files. Instead of writing one step
per file, write a rule with a `{wildcard}`:

```julia
align = @rule align("reads/{sample}.fq" => "bam/{sample}.bam") =
    "bwa mem ref.fa {input} | samtools sort -o {output}"

run(align)        # once for every reads/*.fq on disk
```

That is the subject of [Wildcard rules and workflows](rules.md).

**Next:** [Steps](steps.md).
