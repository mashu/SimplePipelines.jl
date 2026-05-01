# Steps and shell

A **step** is a shell command or a Julia function. Optionally attach **file inputs/outputs** to a step.

## Shell commands

```julia
using SimplePipelines

# Direct command (anonymous step)
step = @step sh"samtools sort input.bam"

# Named step
step = @step sort = sh"samtools sort input.bam"

# Shell features (>, |, &&) — use sh"..."
step = @step sort = sh"sort data.txt | uniq > sorted.txt"

# Shell variables (\$VAR) — use shell_raw so Julia does not interpolate
donors = ["A", "B"]
step = @step process = sh(() -> shell_raw"for d in " * join(donors, " ") * shell_raw"; do echo \$d; done")
# Multiline: shell_raw"""...""" for readability
```

## Julia functions

```julia
# Anonymous function step
step = @step () -> process_data()

# Named function step
step = @step analyze = () -> run_analysis("data.csv")
```

## File dependencies (optional)

Track input/output files for validation:

```julia
@step align("reads.fq" => "aligned.bam") = sh"bwa mem ref.fa reads.fq > aligned.bam"
```

**Next:** [Composing pipelines](composition.md) — chaining steps with `>>`, `|>`, parallelism, and DAG shapes.
