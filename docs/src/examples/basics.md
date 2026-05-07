# Examples: basics

Sequential pipelines, parallelism, Julia + shell, and data flow (`>>`, `|>`, `.>>`).

**More examples:** [Control flow](control-flow.md) · [Complex DAGs](complex-dags.md) · [Bioinformatics](bioinformatics.md)

## 1.1 Basic pipeline

**Flow:** Three steps in sequence.

```
  download  ──►  process  ──►  upload
```

**Goal:** Create a file, process it, then copy the result (runnable without network).

```julia
using SimplePipelines

download = @step download = sh"(echo line3; echo line1; echo line2) > data.txt"
process = @step process = sh"sort data.txt > sorted.txt"
upload = @step upload = sh"cp sorted.txt uploaded.txt"

pipeline = download >> process >> upload
run(pipeline)
```

## 1.2 Parallel processing

**Flow:** Three steps run in parallel, then one step merges.

```
       ┌── file_a ──┐
       ├── file_b ──┼──►  archive
       └── file_c ──┘
```

**Goal:** Create three files, compress them concurrently, then archive.

```julia
using SimplePipelines

file_a = @step a = sh"echo content_a > file_a.txt && gzip -k file_a.txt"
file_b = @step b = sh"echo content_b > file_b.txt && gzip -k file_b.txt"
file_c = @step c = sh"echo content_c > file_c.txt && gzip -k file_c.txt"
archive = @step archive = sh"tar -cvf archive.tar file_a.txt.gz file_b.txt.gz file_c.txt.gz"

pipeline = (file_a & file_b & file_c) >> archive
run(pipeline)
```

## 1.3 Julia + shell

**Flow:** Julia step → shell step → Julia step.

```
  generate  ──►  process  ──►  report
   (Julia)       (shell)      (Julia)
```

**Goal:** Generate data in Julia, run a shell tool, then summarize in Julia.

```julia
using SimplePipelines
using DelimitedFiles

generate = @step generate = () -> begin
    data = rand(100, 10)
    writedlm("matrix.csv", data, ',')
    return "Generated $(size(data)) matrix"
end

process = @step process = sh"wc -l matrix.csv"

report = @step report = () -> begin
    lines = read("matrix.csv", String)
    nrows = count(==('\n'), lines)
    return "Matrix has $nrows rows"
end

pipeline = generate >> process >> report
run(pipeline)
```

## 1.4 Data passing and pipe

**Flow:** Function steps receive the previous step's output; `|>` pipes output forward; `.>>` attaches the next step to each branch.

```
  download(id)  ──►  process(path)     # >> passes path
  ForEach .>> step                     # each branch: branch >> step
```

**Goal:** Pass output between function steps; use broadcast for per-branch follow-up.

```julia
using SimplePipelines

download() = "data.csv"
process(path) = path * "_done"
pipeline = @step dl = download >> @step proc = process
run(pipeline)

fetch = @step fetch = `echo "hello"`
pipeline = fetch |> @step process = (s -> "got: " * strip(String(s)))
run(pipeline)

fe = ForEach([1, 2]) do x
    Step(Symbol("gen_", x), `echo $x`)
end
pipeline = fe .>> @step process = (s -> "got_" * strip(String(s)))
run(pipeline, force=true)
```
