# Getting started

This page builds one small pipeline from scratch. It is intentionally simple:
create a file, sort it, then compute two summaries in parallel.

The point is not the shell commands. The point is the shape:

```text
prepare -> sort_file -> count_lines
                    \-> count_bytes
```

## Define The Work

A pipeline is made from steps. Each step has a name and some work to run.
When a step reads or writes files, declare those files so the runtime can check
inputs, verify outputs, and skip fresh work later.

```julia
using SimplePipelines

prepare = @step prepare([] => ["data.txt"]) =
    sh"(echo line3; echo line1; echo line2) > data.txt"

sort_file = @step sort_file(["data.txt"] => ["sorted.txt"]) =
    sh"sort data.txt > sorted.txt"
```

The first step has no input files and promises `data.txt`. The second step needs
`data.txt` and promises `sorted.txt`.

## Add Parallel Work

After the file is sorted, two summaries can run independently.

```julia
count_lines = @step count_lines(["sorted.txt"] => ["lines.txt"]) =
    sh"wc -l sorted.txt > lines.txt"

count_bytes = @step count_bytes(["sorted.txt"] => ["bytes.txt"]) =
    sh"wc -c sorted.txt > bytes.txt"
```

## Compose The DAG

Use `>>` for "then" and `&` for "at the same time".

```julia
pipeline = prepare >> sort_file >> (count_lines & count_bytes)
```

This is the whole DAG. You can inspect it before running:

```julia
print_dag(pipeline)
```

## Run It

```julia
results = run(pipeline)
```

`run` returns one [`StepResult`](@ref) per step that ran. The printed log is for
you; the result vector is for code.

```julia
all(r -> r.success, results)
last(results).outputs
```

If you run the same pipeline again, steps with fresh declared outputs can be
skipped.

## Where This Goes Next

This example used fixed filenames. When filenames follow a pattern such as
`raw/{sample}.fq` -> `out/{sample}.bam`, use rules and check them before running:

```julia
align = @step align("raw/{sample}.fq" => "out/{sample}.bam") =
    "bwa mem ref.fa {input} > {output}"

check(align, "out/A.bam")
```

For now, keep the mental model:

- steps are units of work;
- `>>` and `&` draw the DAG;
- `run` executes the DAG and returns structured results.

**Next:** [Steps and shell](steps-and-shell.md) explains how to write steps well.
