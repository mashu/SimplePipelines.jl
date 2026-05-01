# Examples: control flow

Retry, fallback, and conditional branches.

**More examples:** [Basics](basics.md) · [Complex DAGs](complex-dags.md) · [Bioinformatics](bioinformatics.md)

## 2.1 Retry and fallback

**Flow (retry):** Run a step up to N times, then continue.

```
  [fetch^3]  ──►  process
```

**Flow (fallback):** If primary fails, run fallback.

```
   primary  ──►  (success) result
      │
      └──►  fallback ──►  result
```

**Goal:** Retry a flaky fetch; fall back when the primary fails; combine both.

```julia
using SimplePipelines

fetch = @step fetch = sh"echo '{\"x\":1}' > data.json"
process = @step process = sh"wc -c data.json"
pipeline = Retry(fetch, 3, delay=0.1) >> process

run(@step setup = sh"(echo 'a,b'; echo '1,2') > data.csv", verbose=false)
fast = @step fast = sh"sort data.csv > sorted.csv"
slow = @step slow = sh"cat data.csv > sorted.csv"
pipeline = fast | slow

pipeline = Retry(fast, 3) | slow
run(pipeline)
```

## 2.2 Conditional branching

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
