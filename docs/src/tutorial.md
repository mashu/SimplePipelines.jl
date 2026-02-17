# Tutorial

## Steps

A **Step** is the basic unit of work—either a shell command or Julia function.

### Shell Commands

```julia
using SimplePipelines

# Direct command (anonymous step)
step = @step sh"samtools sort input.bam"

# Named step
step = @step sort = sh"samtools sort input.bam"

# Shell features (>, |, &&) - use sh"..."
step = @step sort = sh"sort data.txt | uniq > sorted.txt"
```

### Julia Functions

```julia
# Anonymous function step
step = @step () -> process_data()

# Named function step
step = @step analyze = () -> run_analysis("data.csv")
```

### File Dependencies (Optional)

Track input/output files for validation:

```julia
@step align("reads.fq" => "aligned.bam") = sh"bwa mem ref.fa reads.fq > aligned.bam"  # sh"..." for redirection
```

## Sequential Execution: `>>`

The `>>` operator chains steps—each waits for the previous to complete. When the next node is a **function step**, it receives the previous step's output (or the current branch context inside `ForEach`):

```julia
# Chain commands directly (anonymous steps)
pipeline = sh"download data.txt" >> sh"process data.txt" >> sh"upload results.txt"

# Data passing: function steps receive previous output
download(id) = "data_$(id).csv"
process(path) = read(path, String)  # receives path from download
pipeline = @step dl = download >> @step proc = process

# Or define named steps, then chain
step_a = @step step_a = sh"download data.txt"
step_b = @step step_b = sh"process data.txt"
step_c = @step step_c = sh"upload results.txt"
pipeline = step_a >> step_b >> step_c
```

## Pipe (`|>`), same input (`>>>`), and broadcast (`.>>`)

When the left has **one** output, `>>`, `|>`, and `.>>` all pass that value to the next (function) step. When the left has **multiple** outputs (e.g. ForEach, Parallel), they differ:

| Left side     | `a >> step`         | `a |> step`            | `a .>> step`                 |
| ------------- | ------------------- | ---------------------- | ---------------------------- |
| Single output | step(one value)     | step(one value)        | step(one value)              |
| Multi output  | step(**last** only)  | step(**vector** of all) | step **per branch** (one call each) |

- **`a |> b`** — Run `a`, then run `b` with `a`'s output(s). RHS must be a function step. Multi-branch → one call with a vector.
- **`a >>> b`** — Run `a` then `b` with the **same** input (e.g. inside ForEach, both get the branch id). Use when the next step should not receive `a`'s output.
- **`a .>> b`** — Attach `b` to **each branch** of `a`. Each branch runs as `branch >> b`; you don't wait for all of `a` to finish before starting `b` on completed branches.

```julia
# Pipe: pass download output to process
fetch = @step fetch = `echo "content"`
process(x) = uppercase(String(x))
pipeline = fetch |> @step process = process

# Same input (e.g. in ForEach): both steps get the id
ForEach([1, 2]) do id
    @step first = (x -> "a_$(x)") >>> @step second = (x -> "b_$(x)")
end

# Broadcast: process each branch output immediately
ForEach(["a", "b"]) do x
    Step(Symbol("echo_", x), `echo $x`)
end .>> @step process = (s -> "got_" * strip(String(s)))
```

## Parallel Execution: `&`

The `&` operator groups steps to run concurrently:

```julia
# Parallel steps (anonymous)
parallel = sh"task_a" & sh"task_b" & sh"task_c"

# Named steps in parallel, then merge
sample_1 = @step s1 = sh"process sample1"
sample_2 = @step s2 = sh"process sample2"
sample_3 = @step s3 = sh"process sample3"
merge_results = @step merge = sh"merge outputs"
pipeline = (sample_1 & sample_2 & sample_3) >> merge_results
```

## Complex DAGs

Combine `>>` and `&` for arbitrary graphs.

### Diamond Pattern

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

### Multi-Stage Parallel

For graphs with multiple fork-join points, compose in stages:

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

### Independent Branches

Process independent pipelines in parallel, then merge:

```
  ┌─ fetch_a >> process_a ─┐
  │                        │
  ├─ fetch_b >> process_b ─┼── merge
  │                        │
  └─ fetch_c >> process_c ─┘
```

```julia
fetch_a = @step fetch_a = sh"fetch sample_a"
process_a = @step process_a = sh"process sample_a"
fetch_b = @step fetch_b = sh"fetch sample_b"
process_b = @step process_b = sh"process sample_b"
fetch_c = @step fetch_c = sh"fetch sample_c"
process_c = @step process_c = sh"process sample_c"
merge = @step merge = sh"merge results"

branch_a = fetch_a >> process_a
branch_b = fetch_b >> process_b
branch_c = fetch_c >> process_c
pipeline = (branch_a & branch_b & branch_c) >> merge
```

This pattern is common for processing multiple samples/files independently before combining results.

## Fallback: `|`

The `|` operator provides fallback behavior—if the primary fails, run the fallback:

```julia
# If fast method fails, use slow method
fast_method = @step fast = sh"fast_tool input.txt"
slow_method = @step slow = sh"slow_tool input.txt"
pipeline = fast_method | slow_method

# Chain multiple fallbacks
method_a = @step a = sh"method_a input"
method_b = @step b = sh"method_b input"
method_c = @step c = sh"method_c input"
pipeline = method_a | method_b | method_c
```

## Retry: `^` or `Retry()`

Retry a node up to N times on failure:

```julia
# Using ^ operator (concise) – retry flaky step up to 3 times
flaky_api_call = @step api = sh"echo 'mock response'"
pipeline = flaky_api_call^3

# Using Retry() with delay between attempts
network_request = @step fetch = sh"echo 'data'"
pipeline = Retry(network_request, 5, delay=2.0)

# Combine with fallback
primary = @step primary = sh"echo primary"
fallback = @step fallback = sh"echo fallback"
pipeline = primary^3 | fallback
```

## Branch (Conditional)

Execute different branches based on a runtime condition:

```julia
# Branch based on file size
large_file_pipeline = @step large = sh"process_large data.txt"
small_file_pipeline = @step small = sh"process_small data.txt"
pipeline = Branch(
    () -> filesize("data.txt") > 1_000_000,
    large_file_pipeline,
    small_file_pipeline
)

# Branch based on environment
debug_steps = @step debug = sh"run with verbose logging"
normal_steps = @step normal = sh"run quietly"
pipeline = Branch(
    () -> haskey(ENV, "DEBUG"),
    debug_steps,
    normal_steps
)
```

## Timeout

Fail if a node exceeds a time limit:

```julia
# 30 second timeout
long_running_step = @step long = sh"sleep 1"
pipeline = Timeout(long_running_step, 30.0)

# Combine with retry and fallback
api_call = @step api = sh"echo result"
backup = @step backup = sh"echo fallback"
pipeline = Timeout(api_call, 5.0)^3 | backup
```

## ForEach (pattern or collection)

ForEach has two modes (dispatch on second argument). **Collection:** apply the block to each item (like Map). **Pattern:** discover files by pattern and run the block per match.

```julia
# Over a collection (list supplied in code)
samples = ["sample_A", "sample_B", "sample_C"]
pipeline = ForEach(samples) do s
    Step(Symbol("process_", s), sh("analyze $s.fastq"))
end >> merge_results
```

**Pattern-based discovery:** ForEach(pattern) scans the filesystem; matching files must exist. Example with a temp dir:

```julia
# Create dummy files so ForEach can find matches (run in a temp dir)
cd(mktempdir()) do
    write("s1.fastq", ""); write("s2.fastq", "")
    mkpath("fastq"); write("fastq/s1_R1.fq.gz", ""); write("fastq/s2_R1.fq.gz", "")
    mkpath("data/p1"); write("data/p1/s1.csv", "")

    # Single step per file - use sh("...") for interpolation
    ForEach("{sample}.fastq") do sample
        sh("process $(sample).fastq")
    end

    # Multi-step per file - chain with >>
    ForEach("fastq/{sample}_R1.fq.gz") do sample
        sh("pear $(sample)_R1 $(sample)_R2") >> sh("analyze $(sample)")
    end

    # Multiple wildcards
    ForEach("data/{project}/{sample}.csv") do project, sample
        sh("process $(project)/$(sample).csv")
    end

    # Chain with downstream merge
    ForEach("{id}.fastq") do id
        sh("align $(id).fastq")
    end >> @step merge = sh"merge *.bam"
end
```

## Reduce (Combine)

Collect outputs from parallel steps and combine them:

```julia
# Combine parallel outputs with a function (define steps first)
analyze_a = @step a = sh"echo result_a"
analyze_b = @step b = sh"echo result_b"
pipeline = Reduce(analyze_a & analyze_b) do outputs
    join(outputs, "\n")
end

# Using a named function (define reducer and steps first)
combine_results(outputs) = join(outputs, "\n")
step_a = @step a = sh"echo result_a"
step_b = @step b = sh"echo result_b"
step_c = @step c = sh"echo result_c"
pipeline = Reduce(combine_results, step_a & step_b & step_c)

# In a pipeline: fetch -> parallel analysis -> reduce -> report
merge_outputs(outputs) = join(outputs, "\n")
fetch = @step fetch = sh"echo data"
analyze_a = @step a = sh"wc -c"
analyze_b = @step b = sh"wc -l"
report = @step report = sh"echo done"
pipeline = fetch >> Reduce(merge_outputs, analyze_a & analyze_b) >> report
```

The reducer function receives a vector of outputs from all successful parallel steps (type depends on what the upstream steps return).

### Low-memory / large data

To avoid holding many large outputs in memory: (1) Have each step write its result to a file and **return the path** (e.g. `String`). (2) The reducer then receives a vector of paths and can open/process one at a time (or stream), write the combined result to a file, and return that path. (3) Use `run(pipeline; keep_outputs=:last)` so the returned `results` vector only retains the final step's `.output`; other steps get `.output === nothing`. You still get success, duration, and inputs per step; only the large values are dropped. Use `keep_outputs=:none` to drop all outputs.

## Running Pipelines

Use `run(pipeline)` or `pipeline |> run`:

```julia
# Basic execution
results = run(pipeline)

# Silent (no progress output)
results = run(pipeline, verbose=false)

# Dry run (preview structure)
run(pipeline, dry_run=true)

# Named pipeline
step_a = @step a = sh"first"
step_b = @step b = sh"second"
p = Pipeline(step_a >> step_b, name="My Workflow")
run(p)
```

## Checking Results

```julia
results = run(pipeline)

for r in results
    if r.success
        println("$(r.step.name): completed in $(r.duration)s")
    else
        println("$(r.step.name): FAILED - $(r.output)")
    end
end

# Check overall success
all_ok = all(r -> r.success, results)
```

## Mixing Shell and Julia

Shell commands and Julia functions compose seamlessly:

```julia
# Julia: prepare data (e.g. filter non-empty lines)
prep = @step prep = () -> begin
    raw = read("raw.csv", String)
    cleaned = filter(line -> !isempty(strip(line)), split(raw, '\n'))
    write("clean.csv", join(cleaned, '\n'))
    return "Wrote $(length(cleaned)) lines"
end

# Shell: run external tool
external = @step tool = sh"wc -l clean.csv > result.txt"  # sh"..." for redirection

# Julia: postprocess
post = @step post = () -> begin
    n = parse(Int, split(read("result.txt", String))[1])
    return "Line count: $n"
end

pipeline = prep >> external >> post
run(pipeline)
```

## Utilities

```julia
# Count steps in a pipeline
n = count_steps(pipeline)

# Get all steps as a vector
all_steps = steps(pipeline)

# Print DAG structure
print_dag(pipeline)
```
