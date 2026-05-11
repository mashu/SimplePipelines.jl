# Composing pipelines

Once steps are clear, composition should read like the workflow diagram you have
in mind. SimplePipelines uses a few operators for this. Most pipelines only need
two at first:

- `>>` for "then";
- `&` for "at the same time".

The other operators are for data-flow details once your graph has branches.

## Run One Step After Another

Use `>>` when a later step should wait for an earlier step.

```julia
prepare = @step prepare([] => ["data.txt"]) =
    sh"(echo line3; echo line1; echo line2) > data.txt"

sort_file = @step sort_file(["data.txt"] => ["sorted.txt"]) =
    sh"sort data.txt > sorted.txt"

pipeline = prepare >> sort_file
run(pipeline)
```

This shape is useful even when the shell commands already mention file names,
because the DAG now knows ordering, freshness, and failure boundaries.

## Fork, Then Join

Use `&` when independent branches can run concurrently.

```text
prepare -> sort_file -> count_lines
                    \-> count_bytes
```

```julia
count_lines = @step count_lines(["sorted.txt"] => ["lines.txt"]) =
    sh"wc -l sorted.txt > lines.txt"

count_bytes = @step count_bytes(["sorted.txt"] => ["bytes.txt"]) =
    sh"wc -c sorted.txt > bytes.txt"

pipeline = prepare >> sort_file >> (count_lines & count_bytes)
```

Parentheses matter: `(a & b) >> c` means both `a` and `b` must finish before
`c` runs.

## Passing Values, Not Just Files

File paths are often enough. When you use function steps, step results can also
flow directly into the next function.

```julia
emit_path = @step emit_path = () -> "sorted.txt"
read_text = @step read_text = path -> read(path, String)

pipeline = emit_path >> read_text
```

For a single upstream result, `>>` and `|>` both pass that value to a function
step. The difference matters when the left side has many branch results.

## Advanced: Many Branch Outputs

When a parallel node produces multiple results, choose the operator based on what
the next function step should receive:

- `a >> b`: pass the last branch result to `b`;
- `a |> b`: pass a vector of all branch outputs to one function step;
- `a .>> b`: run `b` once per branch;
- `a >>> b`: run the second step with the same input as the first.

```julia
left = (@step a = `echo A`) & (@step b = `echo B`)

collect_all = @step collect_all = xs -> join(strip.(String.(xs)), ",")
per_branch = @step per_branch = x -> "saw " * strip(String(x))

pipeline_a = left |> collect_all
pipeline_b = left .>> per_branch
```

Use these only when you need this value-passing behavior. For file-oriented
workflows, plain `>>` and `&` usually keep the graph easier to read. For repeated
branches over samples or files, continue to [Fan-out and reduce](foreach-reduce.md).

## What To Remember

Write the DAG the way you would draw it. Start with `>>` and `&`; reach for the
pipe variants only when a function step needs a particular value from branch
outputs.

**Next:** [Rules and diagnostics](rules-and-diagnostics.md) shows how to describe
many file targets from patterns and inspect the result before running.
