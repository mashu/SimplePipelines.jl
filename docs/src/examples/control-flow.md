# Examples: control flow

Cookbook examples for retry, fallback, and conditional branches. Read
[Control flow](../guide/control-flow.md) first if you want the concepts before
the recipes.

**More examples:** [Basics](basics.md) · [Complex DAGs](complex-dags.md) · [Bioinformatics](bioinformatics.md)

## 2.1 Retry a flaky step

**Flow:** Run a step up to N times, then continue.

```
  [fetch^3]  ──►  process
```

**Goal:** Retry a fetch before processing its output.

```julia
using SimplePipelines

fetch = @step fetch = sh"echo '{\"x\":1}' > data.json"
process = @step process = sh"wc -c data.json"
pipeline = Retry(fetch, 3, delay=0.1) >> process
run(pipeline)
```

## 2.2 Fallback to a backup method

**Flow:** If the primary method fails, run the backup.

```
   primary  ──►  (success) result
      │
      └──►  fallback ──►  result
```

**Goal:** Produce `sorted.csv` even when the fast path fails.

```julia
using SimplePipelines

run(@step setup = sh"(echo 'a,b'; echo '1,2') > data.csv", verbose=false)
fast = @step fast = sh"false"
slow = @step slow = sh"cat data.csv > sorted.csv"
pipeline = fast | slow
run(pipeline)
```

## 2.3 Retry, then fallback

**Goal:** Retry the primary method, then use the backup if every attempt fails.

```julia
using SimplePipelines

run(@step setup = sh"(echo 'a,b'; echo '1,2') > data.csv", verbose=false)
fast = @step fast = sh"false"
slow = @step slow = sh"cat data.csv > sorted.csv"
pipeline = Retry(fast, 3) | slow
run(pipeline)
```

## 2.4 Conditional branching

**Flow:** One of two branches runs based on a condition.

```
            ┌── if true  ──►  branch_a
  condition ─┤
            └── if false ──►  branch_b
```

**Goal:** Choose processing by file size or environment.

```julia
using SimplePipelines

run(@step setup = sh"(echo 'a,b'; echo '1,2'; echo '3,4') > data.csv", verbose=false)

small_pipeline = @step small = sh"head -n 1000 data.csv > sample.csv"
large_pipeline = @step decompress = sh"gunzip -c data.csv.gz > data.csv" >> @step process = sh"split -l 10000 data.csv chunk_"
pipeline = Branch(
    () -> filesize("data.csv") < 100_000_000,
    small_pipeline,
    large_pipeline,
)
run(pipeline)

debug_steps = @step debug = sh"echo 'debug mode'"
prod_steps = @step prod = sh"echo 'production'"
pipeline = Branch(() -> get(ENV, "DEBUG", "0") == "1", debug_steps, prod_steps)
run(pipeline)
```
