# Composing pipelines

Build workflows by combining steps with operators. Start with **`>>`** (sequence) and **`&`** (parallel), then use pipe variants when you need finer control over how values flow between branches.

## Sequential execution: `>>`

The `>>` operator chains steps: each waits for the previous to complete. When the next node is a **function step**, it receives the previous step's output (or the current branch context inside `ForEach`):

```julia
# Chain commands directly (anonymous steps)
pipeline = sh"download data.txt" >> sh"process data.txt" >> sh"upload results.txt"

# Data passing: function steps receive previous output
download(id) = "data_$(id).csv"
process(path) = read(path, String)
pipeline = @step dl = download >> @step proc = process

# Named steps
step_a = @step step_a = sh"download data.txt"
step_b = @step step_b = sh"process data.txt"
step_c = @step step_c = sh"upload results.txt"
pipeline = step_a >> step_b >> step_c
```

## Pipe (`|>`), same input (`>>>`), and broadcast (`.>>`)

When the left has **one** output, `>>`, `|>`, and `.>>` all pass that value to the next (function) step. When the left has **multiple** outputs (e.g. `ForEach`, `Parallel`), they differ:

| Left side     | `>>`                | `|>`                     | `.>>`                      |
|:--------------|:--------------------|:-------------------------|:---------------------------|
| Single output | step(one value)     | step(one value)          | step(one value)            |
| Multi output  | step(**last** only) | step(**vector** of all)  | step **per branch**        |

- **`a |> b`** — Run `a`, then run `b` with `a`'s output(s). RHS must be a function step. Multi-branch → one call with a vector.
- **`a >>> b`** — Run `a` then `b` with the **same** input (e.g. inside `ForEach`, both get the branch id).
- **`a .>> b`** — Attach `b` to **each branch** of `a`. Each branch runs as `branch >> b`; you do not wait for all of `a` to finish before starting `b` on completed branches.

```julia
fetch = @step fetch = `echo "content"`
process(x) = uppercase(String(x))
pipeline = fetch |> @step process = process

ForEach([1, 2]) do id
    @step first = (x -> "a_$(x)") >>> @step second = (x -> "b_$(x)")
end

ForEach(["a", "b"]) do x
    Step(Symbol("echo_", x), `echo $x`)
end .>> @step process = (s -> "got_" * strip(String(s)))
```

See also the [quick reference](../reference/quickref.md#sequencing-and-piping) tables for a compact summary.

## Parallel execution: `&`

The `&` operator groups steps to run concurrently:

```julia
parallel = sh"task_a" & sh"task_b" & sh"task_c"

sample_1 = @step s1 = sh"process sample1"
sample_2 = @step s2 = sh"process sample2"
sample_3 = @step s3 = sh"process sample3"
merge_results = @step merge = sh"merge outputs"
pipeline = (sample_1 & sample_2 & sample_3) >> merge_results
```

## Complex DAGs

Combine `>>` and `&` for fork–join graphs.

### Diamond pattern

```
       ┌── analyze_a ──┐
 fetch─┤               ├── report
       └── analyze_b ──┘
```

```julia
fetch = @step fetch = sh"echo 'a,b\n1,2' > data.csv"
analyze_a = @step a = sh"wc -l data.csv"
analyze_b = @step b = sh"wc -c data.csv"
report = @step report = () -> "done"

pipeline = fetch >> (analyze_a & analyze_b) >> report
run(pipeline)
```

### Multi-stage parallel

```
     ┌─ b ─┐     ┌─ e ─┐
  a ─┤     ├─ d ─┤     ├─ g
     └─ c ─┘     └─ f ─┘
```

```julia
a = @step a = sh"step_a"
b = @step b = sh"step_b"
c = @step c = sh"step_c"
d = @step d = sh"step_d"
e = @step e = sh"step_e"
f = @step f = sh"step_f"
g = @step g = sh"step_g"

pipeline = a >> (b & c) >> d >> (e & f) >> g
```

### Independent branches

```
  ┌─ fetch_a >> process_a ─┐
  ├─ fetch_b >> process_b ─┼── merge
  └─ fetch_c >> process_c ─┘
```

```julia
fetch_a = @step fetch_a = sh"fetch sample_a"
process_a = @step process_a = sh"process sample_a"
# ... similarly for b, c
merge = @step merge = sh"merge results"

branch_a = fetch_a >> process_a
branch_b = fetch_b >> process_b
branch_c = fetch_c >> process_c
pipeline = (branch_a & branch_b & branch_c) >> merge
```

This pattern is common for processing multiple samples independently before combining results.

**Next:** [Control flow](control-flow.md) — fallback, retry, branching, and timeouts.
