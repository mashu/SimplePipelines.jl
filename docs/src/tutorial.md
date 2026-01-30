# Tutorial

## Steps

A **Step** is the basic unit of work—either a shell command or Julia function.

### Shell Commands

```julia
# Direct command (anonymous step)
step = @step `samtools sort input.bam`

# Named step
step = @step sort = `samtools sort input.bam`
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
@step align("reads.fq" => "aligned.bam") = `bwa mem ref.fa reads.fq > aligned.bam`
```

## Sequential Execution: `>>`

The `>>` operator chains steps—each waits for the previous to complete:

```julia
# Three steps in order
pipeline = step_a >> step_b >> step_c

# Chain commands directly
pipeline = `download data.txt` >> `process data.txt` >> `upload results.txt`
```

## Parallel Execution: `&`

The `&` operator groups steps to run concurrently:

```julia
# Three steps in parallel
parallel = step_a & step_b & step_c

# Process samples in parallel, then merge
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
fetch = @step fetch = `curl -o data.csv https://example.com/data`
analyze_a = @step a = `tool_a data.csv`
analyze_b = @step b = `tool_b data.csv`
report = @step report = () -> combine_results()

pipeline = fetch >> (analyze_a & analyze_b) >> report
```

### Multi-Stage Parallel

For graphs with multiple fork-join points, compose in stages:

```
     ┌─ b ─┐     ┌─ e ─┐
  a ─┤     ├─ d ─┤     ├─ g
     └─ c ─┘     └─ f ─┘
```

```julia
a = @step a = `step_a`
b = @step b = `step_b`
c = @step c = `step_c`
d = @step d = `step_d`
e = @step e = `step_e`
f = @step f = `step_f`
g = @step g = `step_g`

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
pipeline = fast_method | slow_method

# Chain multiple fallbacks
pipeline = method_a | method_b | method_c
```

## Retry: `^` or `Retry()`

Retry a node up to N times on failure:

```julia
# Using ^ operator (concise)
pipeline = flaky_api_call^3

# Using Retry() with delay between attempts
pipeline = Retry(network_request, 5, delay=2.0)

# Combine with fallback
pipeline = primary^3 | fallback
```

## Branch (Conditional)

Execute different branches based on a runtime condition:

```julia
# Branch based on file size
pipeline = Branch(
    () -> filesize("data.txt") > 1_000_000,
    large_file_pipeline,
    small_file_pipeline
)

# Branch based on environment
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
pipeline = Timeout(long_running_step, 30.0)

# Combine with retry and fallback
pipeline = Timeout(api_call, 5.0)^3 | backup
```

## Map (Fan-out)

Apply a function to each item, creating parallel steps:

```julia
# Process files in parallel
samples = ["sample_A", "sample_B", "sample_C"]
pipeline = Map(samples) do s
    Step(Symbol("process_", s), `analyze $s.fastq`)
end >> merge_results

# Equivalent to:
# (process_sample_A & process_sample_B & process_sample_C) >> merge_results
```

## Running Pipelines

```julia
# Basic execution
results = run_pipeline(pipeline)

# Silent (no progress output)
results = run_pipeline(pipeline, verbose=false)

# Dry run (preview structure)
run_pipeline(pipeline, dry_run=true)

# Named pipeline
p = Pipeline(step_a >> step_b, name="My Workflow")
run_pipeline(p)
```

## Checking Results

```julia
results = run_pipeline(pipeline)

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
# Julia: prepare data
prep = @step prep = () -> begin
    data = load("raw.csv")
    cleaned = filter_invalid(data)
    save("clean.csv", cleaned)
end

# Shell: run external tool
external = @step tool = `external_program clean.csv -o result.txt`

# Julia: postprocess
post = @step post = () -> parse_and_summarize("result.txt")

pipeline = prep >> external >> post
run_pipeline(pipeline)
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
