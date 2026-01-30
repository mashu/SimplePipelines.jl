# Examples

This page shows pipeline patterns in order of increasing complexity. Each example has a flow diagram, a short goal, and runnable code.

**Contents**

1. [Basics](#1-basics) — sequential, parallel, Julia + shell
2. [Control flow](#2-control-flow) — retry, fallback, branching
3. [Complex DAGs](#3-complex-dags) — multi-stage parallel, robust pipeline
4. [Bioinformatics](#4-bioinformatics) — immune repertoire (single & multi-donor), variant calling

---

## 1. Basics

### 1.1 Basic Pipeline

**Flow:** Three steps in sequence.

```
  download  ──►  process  ──►  upload
```

**Goal:** Download a file, process it, upload the result.

```julia
using SimplePipelines

download = @step download = sh"curl -o data.txt https://example.com/data"
process = @step process = sh"sort data.txt > sorted.txt"
upload = @step upload = sh"scp sorted.txt server:/data/"

pipeline = download >> process >> upload
run_pipeline(pipeline)
```

---

### 1.2 Parallel Processing

**Flow:** Three steps run in parallel, then one step merges.

```
       ┌── file_a ──┐
       ├── file_b ──┼──►  archive
       └── file_c ──┘
```

**Goal:** Compress three files concurrently, then archive all outputs.

```julia
using SimplePipelines

file_a = @step a = sh"gzip -k file_a.txt"
file_b = @step b = sh"gzip -k file_b.txt"
file_c = @step c = sh"gzip -k file_c.txt"
archive = @step archive = sh"tar -cvf archive.tar *.gz"

pipeline = (file_a & file_b & file_c) >> archive
run_pipeline(pipeline)
```

---

### 1.3 Julia + Shell

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
run_pipeline(pipeline)
```

---

## 2. Control flow

### 2.1 Retry and Fallback

**Flow (retry then continue):** Run a step up to N times, then continue.

```
  [fetch^3]  ──►  process
   (retry)
```

**Flow (fallback):** If primary fails, run fallback.

```
   primary  ──►  (on success)  result
      │
      └──►  (on failure)  fallback  ──►  result
```

**Goal:** Retry flaky fetch; use fallback when primary step fails; combine retry and fallback.

```julia
using SimplePipelines

# Retry then continue
fetch = @step fetch = sh"curl -f https://example.com/data -o data.json"
process = @step process = sh"wc -c data.json"
pipeline = Retry(fetch, 3, delay=1.0) >> process

# Fallback
fast = @step fast = sh"sort data.csv > sorted.csv"
slow = @step slow = sh"cat data.csv > sorted.csv"
pipeline = fast | slow

# Retry then fallback
pipeline = Retry(fast, 3) | slow
```

---

### 2.2 Conditional Branching

**Flow:** One of two branches runs based on a condition.

```
            ┌── if true  ──►  branch_a
  condition ─┤
            └── if false ──►  branch_b
```

**Goal:** Choose processing path by file size or environment (e.g. DEBUG).

```julia
using SimplePipelines

# By file size
small_pipeline = @step small = sh"head -n 1000 data.csv > sample.csv"
large_pipeline = @step decompress = sh"gunzip -c data.csv.gz > data.csv" >> @step process = sh"split -l 10000 data.csv chunk_"

pipeline = Branch(
    () -> filesize("data.csv") < 100_000_000,
    small_pipeline,
    large_pipeline
)

# By environment
debug_steps = @step debug = sh"echo 'debug mode'"
prod_steps = @step prod = sh"echo 'production'"
pipeline = Branch(() -> get(ENV, "DEBUG", "0") == "1", debug_steps, prod_steps)
```

---

## 3. Complex DAGs

### 3.1 Multi-stage Parallel

**Flow:** Two parallel fetch+transform branches, then merge, analyze, then report and archive in parallel.

```
  ┌── fetch_db   ──►  transform_db  ──┐
  │                                    ├──►  merge  ──►  analyze  ──►  ┌── report
  └── fetch_files ──►  transform_files ─┘                               └── archive
```

**Goal:** Fetch from two sources in parallel, process each, merge, analyze, then produce report and archive in parallel.

```julia
using SimplePipelines

fetch_db = @step db = sh"curl -s -o db_data.json https://example.com/export"
fetch_files = @step files = sh"echo \"local_data\" > local_data.txt"

transform_db = @step transform_db = sh"wc -c db_data.json > db_size.txt"
transform_files = @step transform_files = sh"wc -c local_data.txt > files_size.txt"

merge = @step merge = sh"cat db_size.txt files_size.txt > merged.txt"
analyze = @step analyze = sh"wc -l merged.txt > results.txt"

report = @step report = sh"cat results.txt"
archive = @step archive = sh"gzip -c merged.txt > results.tar.gz"

db_branch = fetch_db >> transform_db
files_branch = fetch_files >> transform_files
pipeline = (db_branch & files_branch) >> merge >> analyze >> (report & archive)

run_pipeline(pipeline)
```

---

### 3.2 Robust Pipeline (all features)

**Flow:** Retry+fallback fetch → conditional process (small vs full) → report and notify in parallel (notify with retry).

```
  [primary^3 | backup]  ──►  [small? quick : full]  ──►  report
                                                      └── notify^2
```

**Goal:** Fetch with retry and fallback; branch on file size; run report and notify in parallel.

```julia
using SimplePipelines

primary_source = @step primary = sh"curl -sf https://example.com/data -o data.json"
backup_source = @step backup = sh"echo '{\"status\":\"fallback\"}' > data.json"
fetch = Retry(primary_source, 3, delay=1.0) | backup_source

quick_process = @step quick = sh"wc -c data.json > output.txt"
full_process = @step parse = sh"wc -l data.json > output.txt" >> @step validate = sh"wc -c output.txt >> output.txt"

process = Branch(
    () -> filesize("data.json") < 1_000_000,
    quick_process,
    full_process
)

report = @step report = sh"cat output.txt"
notify = @step notify = sh"echo 'Pipeline done'"

pipeline = fetch >> process >> (report & Retry(notify, 2))
run_pipeline(Pipeline(pipeline, name="Robust ETL"))
```

---

## 4. Bioinformatics

**Multiple donors / samples:** Use `ForEach` with a `{wildcard}` pattern to automatically discover files and create parallel branches. The wildcard values propagate via normal Julia `$()` interpolation.

### 4.1 Immune Repertoire (single donor)

**Flow:** Paired-end FASTQ → merge (PEAR) → FASTQ→FASTA → IgBLAST (V/D/J) → Julia filter by identity.

```
  R1.fastq ──┐
             ├──►  pear  ──►  to_fasta  ──►  igblast  ──►  filter_identity  ──►  igblast_filtered.tsv
  R2.fastq ──┘
```

**Goal:** Merge paired-end reads, run IgBLAST with V/D/J references, filter rows by `v_identity` and `j_identity` > 90% in Julia.

```julia
using SimplePipelines
using DelimitedFiles

pear = @step pear = sh"pear -f R1.fastq -r R2.fastq -o merged"
to_fasta = @step to_fasta = sh"seqtk seq -A merged.assembled.fastq > merged.assembled.fasta"
igblast = @step igblast = sh"igblastn -query merged.assembled.fasta -germline_db_V V.fasta -germline_db_D D.fasta -germline_db_J J.fasta -outfmt 7 -out igblast.tsv"

filter_identity = @step filter_identity = () -> begin
    data, header_row = readdlm("igblast.tsv", '\t', header=true)
    header = vec(header_row)
    v_idx = findfirst(isequal("v_identity"), header)
    j_idx = findfirst(isequal("j_identity"), header)
    to_float(x) = something(tryparse(Float64, string(x)), 0.0)
    filtered = [r for r in eachrow(data) if to_float(r[v_idx]) > 90.0 && to_float(r[j_idx]) > 90.0]
    writedlm("igblast_filtered.tsv", vcat(header', filtered), '\t')
    return "Filtered $(length(filtered)) sequences"
end

pipeline = pear >> to_fasta >> igblast >> filter_identity
run_pipeline(Pipeline(pipeline, name="Immune Repertoire"))
```

---

### 4.2 Immune Repertoire (multiple donors)

**Flow:** `ForEach` discovers files matching pattern → one parallel branch per donor.

```
  ForEach("fastq/{donor}_R1.fq.gz")
       │
       ├── donor1: pear → fasta → igblast  ──┐
       ├── donor2: pear → fasta → igblast  ──┼── (parallel)
       └── donorN: pear → fasta → igblast  ──┘
```

**Goal:** Run the same pipeline for every donor; files discovered automatically.

```julia
using SimplePipelines

# ForEach: pattern discovery + parallel branches in one construct
pipeline = ForEach("fastq/{donor}_R1.fq.gz") do donor
    Cmd(["sh", "-c", "pear -f fastq/$(donor)_R1.fq.gz -r fastq/$(donor)_R2.fq.gz -o $(donor)_merged"]) >>
    Cmd(["sh", "-c", "seqtk seq -A $(donor)_merged.assembled.fastq > $(donor).fasta"]) >>
    Cmd(["sh", "-c", "igblastn -query $(donor).fasta -germline_db_V V.fasta -germline_db_D D.fasta -germline_db_J J.fasta -outfmt 7 -out $(donor)_igblast.tsv"])
end

run_pipeline(pipeline)
```

---

### 4.3 Variant Calling

**Flow:** Paired-end reads → FastQC → trim → BWA (GRCh38) → sort/index → bcftools call → index → filter.

```
  R1.fq.gz ──┐
             ├──►  fastqc  ──►  trim  ──►  align  ──►  index  ──►  call  ──►  index_vcf  ──►  filter_vcf
  R2.fq.gz ──┘
```

**Goal:** QC, trim, align to GRCh38, call variants with bcftools, filter by quality.

```julia
using SimplePipelines

fastqc = @step fastqc = sh"fastqc -o qc/ R1.fq.gz R2.fq.gz"
trim = @step trim = sh"trimmomatic PE R1.fq.gz R2.fq.gz R1_trimmed.fq.gz R1_unpaired.fq.gz R2_trimmed.fq.gz R2_unpaired.fq.gz ILLUMINACLIP:adapters.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:15 MINLEN:36"

align = @step align = sh"bwa mem -t 8 GRCh38.fa R1_trimmed.fq.gz R2_trimmed.fq.gz | samtools sort -@ 4 -o aligned.bam -"
index = @step index = sh"samtools index aligned.bam"

call = @step call = sh"bcftools mpileup -f GRCh38.fa aligned.bam | bcftools call -mv -Oz -o variants.vcf.gz"
index_vcf = @step index_vcf = sh"bcftools index variants.vcf.gz"
filter_vcf = @step filter_vcf = sh"bcftools filter -i 'QUAL>=20' variants.vcf.gz -Oz -o filtered.vcf.gz"

pipeline = fastqc >> trim >> align >> index >> call >> index_vcf >> filter_vcf
run_pipeline(Pipeline(pipeline, name="Variant Calling"))
```
