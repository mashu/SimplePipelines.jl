# Steps and shell

Everything starts with a step. A step is one piece of work: either a shell
command or a Julia function. Later pages show how to connect steps, but first it
helps to be clear about what a step owns.

A step has:

- a name, used in output and state;
- work, such as `sh"sort data.txt"` or `() -> compute()`;
- optional input and output file paths.

The file paths are not just documentation. They let SimplePipelines check that
inputs exist, verify outputs after a command runs, and skip fresh work on later
runs.

## Shell commands

Use `sh"..."` when the command is known while constructing the pipeline.

```julia
using SimplePipelines

sort_file = @step sort_file = sh"sort data.txt > sorted.txt"
```

Shell syntax such as redirects and pipes belongs inside the string:

```julia
unique_lines = @step unique_lines =
    sh"sort data.txt | uniq > unique.txt"
```

If the command depends on a Julia value, build it with `sh(...)`:

```julia
sample = "A"
align = @step align = sh("bwa mem ref.fa reads_$(sample).fq > $(sample).sam")
```

For commands that should be generated at run time, pass a function to `sh`:

```julia
sample_ref = Ref("A")
align = @step align = sh(() -> "bwa mem ref.fa reads_$(sample_ref[]).fq > out.sam")
```

Use `shell_raw"..."` when the shell script itself contains `$VAR` and Julia
should not try to interpolate it.

Wildcard file paths create a pattern-backed step template:

```julia
@step align("raw/{sample}.fq" => "out/{sample}.bam") =
    "bwa mem ref.fa {input} > {output}"
```

That string is a template, not a `Cmd`. It is rendered later, when concrete
wildcard values are known. Wildcard steps are covered in
[Rules and diagnostics](rules-and-diagnostics.md).

## Julia functions

Function steps are useful for small transformations, summaries, or glue logic
that is clearer in Julia than in shell.

```julia
make_report = @step report = () -> begin
    n = countlines("sorted.txt")
    "sorted.txt has $n lines"
end
```

When a function step follows another step with `>>` or `|>`, it can receive the
previous result. The next page explains those operators in context.

```julia
emit_path = @step emit_path = () -> "data.txt"
read_file = @step read_file = path -> read(path, String)

pipeline = emit_path >> read_file
```

## File dependencies (optional)

A step that writes files should usually declare them:

```julia
align = @step align("reads.fq" => "aligned.bam") =
    sh"bwa mem ref.fa reads.fq > aligned.bam"
```

This reads as: "`align` needs `reads.fq` and promises `aligned.bam`."

Inputs and outputs can be vectors when there are several files:

```julia
merge = @step merge(["a.bam", "b.bam"] => ["merged.bam"]) =
    sh"samtools merge merged.bam a.bam b.bam"
```

## What To Remember

Write the smallest honest step. If a command creates a file, declare that file.
If a short piece of logic is easier in Julia, make it a function step. Once each
unit is clear, the DAG becomes easy to read.

**Next:** [Composing pipelines](composition.md) shows how `>>` and `&` turn these
steps into a workflow.
