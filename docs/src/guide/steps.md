# Steps

A [`Step`](@ref) is one unit of work: a shell command or a Julia function, with
optional input and output paths. Build steps with [`@step`](@ref):

```julia
@step name = work
@step name(inputs => outputs) = work
@step name(inputs => outputs; threads=4, mem_mb=8000) = work
@step name(inputs) = work            # inputs only, e.g. a validation step
@step work                           # anonymous
```

`inputs` and `outputs` are a path or a vector of paths. If any path contains a
`{wildcard}`, `@step` returns a [`Rule`](@ref) instead (see
[rules](rules.md)).

## Shell commands

A `String` or `sh"..."` is a shell command:

```julia
@step unique_lines = sh"sort data.txt | uniq -c > counts.txt"
@step unique_lines = "sort data.txt | uniq -c > counts.txt"     # the same
```

Commands run with `bash -e -o pipefail -c` (`sh -e -c` if bash is missing). Strict
mode means a failing command anywhere in the script, including the middle of a
pipe, fails the step: `bwa mem ... | samtools sort ...` fails if `bwa` fails,
instead of silently producing a truncated BAM.

!!! note "Pipes into `head`"
    Under `pipefail`, `zcat big.gz | head -n 4000` fails because `zcat` is killed
    by SIGPIPE when `head` stops reading. Write
    `set +o pipefail; zcat big.gz | head -n 4000 > sub.fq` for such commands.

`sh"..."` does not interpolate `$`, so shell variables pass through. Use
`sh("...$(x)...")` or a string template when the command needs Julia values.
Backtick `Cmd`s work as well and run without a shell.

## Templates and placeholders

When a step declares paths, its command is a template:

| placeholder | value |
|:------------|:------|
| `{input}`, `{output}` | all inputs / outputs, space-separated |
| `{input[i]}`, `{output[i]}` | the `i`-th declared input / output |
| `{threads}`, `{mem_mb}` | the step's declared resources |
| `{sample}` (any name) | a wildcard value (rules only) |

```julia
@step bam(["R1.fq", "R2.fq"] => "aln.bam"; threads=8) =
    "bwa mem -t {threads} ref.fa {input} | samtools sort -@ {threads} -o {output}"
```

Paths and values containing spaces or other special characters are shell-quoted
automatically. An unknown placeholder is an error when the step is defined, so a
typo cannot reach the shell. For a literal brace word, double the braces:
`awk '{{print}}'` becomes `awk '{print}'`. `${VAR}` is left to the shell. Braces
around anything that is not a plain word, such as awk's `{print $1}`, need no
escaping.

## Julia functions

```julia
@step summarize = () -> sum(parse.(Int, readlines("numbers.txt")))
```

How a function is called:

- **With declared paths**, it receives them: `f(inputs..., outputs...)`.

  ```julia
  @step filter_tsv("in.tsv" => "out.tsv") = function (input, output)
      lines = filter(l -> !startswith(l, "#"), readlines(input))
      write(output, join(lines, "\n"))
  end
  ```

- **Without paths**, it receives the upstream value if it takes one argument, and
  is called with no arguments otherwise (see [data flow](composition.md#Data-flow)).

A function that returns a command has the command run as part of the step, which
is handy when the command depends on something computed at run time:

```julia
@step align_best = () -> sh("bwa mem $(pick_reference()) reads.fq > out.sam")
```

A function that returns a node, for example a [`ForEach`](@ref), runs that node
next. This is how to build part of a pipeline from data only known at run time.

### Deferred calls

On the right-hand side of `@step`, a function *call* runs when the step runs, not
when the pipeline is built:

```julia
@step load = read_big_table("x.csv")     # read_big_table runs inside the pipeline
```

Command builders are the exception and run immediately, so the command is known
for display and change tracking: `sh(...)`, `string(...)`, `*`, `Cmd(...)` and
`pipeline(...)`.

## What declaring paths buys you

For a step with declared outputs:

- it is **skipped** when every output exists, no input is newer than the oldest
  output, and the outputs were made by the same command and inputs;
- the **output directories** are created before it runs;
- it **fails clearly** when an input is missing, or when it finishes without
  creating an output;
- output files it wrote are **removed if it fails**, so a partial file never looks
  finished;
- two different steps that declare the same outputs **run only once** in a run.

Steps without outputs always run: there is nothing on disk to be up to date.

## Resources

`threads`, `mem_mb` (MiB) and `timeout` (seconds) tell the scheduler what a step
needs, and they are limits while it runs:

```julia
@step sort_bam("aln.bam" => "sorted.bam"; threads=4, mem_mb=8000, timeout=3600) =
    "samtools sort -@ {threads} -m 1500M -o {output} {input}"
```

A step occupies `threads` of the `jobs` CPU slots given to [`run`](@ref) and starts
only when its `mem_mb` is free. A command that uses more memory than its `mem_mb`, or
runs longer than its `timeout`, is terminated. `{threads}` and `{mem_mb}` are filled
in when the step starts. [`with_resources`](@ref) sets defaults for every step inside
a node that does not declare its own. See
[Resource limits and safety](resources.md).

## Julia code in a separate process

Julia functions run inside your Julia session, where they cannot be limited or
stopped. For heavy Julia work, run a script with [`julia`](@ref): it gets its own
process, limited like any command.

```julia
@step counts("reads.tsv" => "counts.tsv"; threads=4, mem_mb=8000) =
    julia("scripts/count.jl {input} {output}")
```

**Next:** [Composing pipelines](composition.md).
