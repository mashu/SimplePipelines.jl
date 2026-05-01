# Examples: complex DAGs

Multi-stage fork–join and a pipeline that combines several features.

**More examples:** [Basics](basics.md) · [Control flow](control-flow.md) · [Bioinformatics](bioinformatics.md)

## 3.1 Multi-stage parallel

**Flow:** Two parallel fetch+transform branches, then merge, analyze, then report and archive in parallel.

```
  ┌── fetch_db   ──►  transform_db  ──┐
  │                                    ├──►  merge  ──►  analyze  ──►  ┌── report
  └── fetch_files ──►  transform_files ─┘                               └── archive
```

**Goal:** Runnable without network; shows layered `&` and `>>`.

```julia
using SimplePipelines

fetch_db = @step db = sh"echo '{\"db\":1}' > db_data.json"
fetch_files = @step files = sh"echo 'local_data' > local_data.txt"

transform_db = @step transform_db = sh"wc -c db_data.json > db_size.txt"
transform_files = @step transform_files = sh"wc -c local_data.txt > files_size.txt"

merge = @step merge = sh"cat db_size.txt files_size.txt > merged.txt"
analyze = @step analyze = sh"wc -l merged.txt > results.txt"

report = @step report = sh"cat results.txt"
archive = @step archive = sh"gzip -c merged.txt > results.tar.gz"

db_branch = fetch_db >> transform_db
files_branch = fetch_files >> transform_files
pipeline = (db_branch & files_branch) >> merge >> analyze >> (report & archive)

run(pipeline)
```

## 3.2 Robust pipeline (combined features)

**Flow:** Retry+fallback fetch → conditional process → parallel report and notify (with retry on notify).

```
  [primary^3 | backup]  ──►  [small? quick : full]  ──►  report
                                                          └── notify^2
```

**Goal:** Illustrate retries, fallback, branching, and parallel tail in one graph.

```julia
using SimplePipelines

primary_source = @step primary = sh"echo '{\"status\":\"ok\"}' > data.json"
backup_source = @step backup = sh"echo '{\"status\":\"fallback\"}' > data.json"
fetch = Retry(primary_source, 3, delay=0.1) | backup_source

quick_process = @step quick = sh"wc -c data.json > output.txt"
full_process = @step parse = sh"wc -l data.json > output.txt" >> @step validate = sh"wc -c output.txt >> output.txt"

process = Branch(
    () -> filesize("data.json") < 1_000_000,
    quick_process,
    full_process,
)

report = @step report = sh"cat output.txt"
notify = @step notify = sh"echo 'Pipeline done'"

pipeline = fetch >> process >> (report & Retry(notify, 2))
run(Pipeline(pipeline, name="Robust ETL"))
```
