# Running and inspecting

## Running pipelines

Use `run(pipeline)` or `pipeline |> run`:

```julia
results = run(pipeline)

# Default: verbose=true prints each shell command before it runs
results = run(pipeline)

results = run(pipeline, verbose=false)

run(pipeline, dry_run=true)

step_a = @step a = sh"first"
step_b = @step b = sh"second"
p = Pipeline(step_a >> step_b, name="My Workflow")
run(p)
```

## Checking results

```julia
results = run(pipeline)

for r in results
    if r.success
        println("$(r.step.name): completed in $(r.duration)s")
    else
        println("$(r.step.name): FAILED - $(r.result)")
    end
end

all_ok = all(r -> r.success, results)
```

Relevant fields include `.success`, `.duration`, `.result`, `.inputs`, and `.outputs`.

## Mixing shell and Julia

Shell commands and Julia functions compose seamlessly:

```julia
prep = @step prep = () -> begin
    raw = read("raw.csv", String)
    cleaned = filter(line -> !isempty(strip(line)), split(raw, '\n'))
    write("clean.csv", join(cleaned, '\n'))
    return "Wrote $(length(cleaned)) lines"
end

external = @step tool = sh"wc -l clean.csv > result.txt"

post = @step post = () -> begin
    n = parse(Int, split(read("result.txt", String))[1])
    return "Line count: $n"
end

pipeline = prep >> external >> post
run(pipeline)
```

## Utilities

```julia
n = count_steps(pipeline)
all_steps = steps(pipeline)
print_dag(pipeline)
```

## Where to go next

- [Examples](../examples/basics.md) — runnable patterns in order of difficulty.
- [Quick reference](../reference/quickref.md) — one-page operator and API tables.
- [API](../api.md) — full docstrings.
