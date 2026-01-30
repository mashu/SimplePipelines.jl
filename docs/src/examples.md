# Examples

## Basic Pipeline

A simple three-step workflow:

```julia
using SimplePipelines

download = @step download = `curl -o data.txt https://example.com/data`
process = @step process = `sort data.txt > sorted.txt`
upload = @step upload = `scp sorted.txt server:/data/`

pipeline = download >> process >> upload
run_pipeline(pipeline)
```

## Parallel Processing

Process multiple files concurrently:

```julia
using SimplePipelines

# Process each file independently
file_a = @step a = `gzip -k file_a.txt`
file_b = @step b = `gzip -k file_b.txt`
file_c = @step c = `gzip -k file_c.txt`

# Archive all compressed files
archive = @step archive = `tar -cvf archive.tar *.gz`

# Files compress in parallel, then archive
pipeline = (file_a & file_b & file_c) >> archive
run_pipeline(pipeline)
```

## Julia Computation

Mix Julia computation with external tools:

```julia
using SimplePipelines

# Generate data in Julia
generate = @step generate = () -> begin
    data = rand(1000, 100)
    writedlm("matrix.csv", data, ',')
    return "Generated $(size(data)) matrix"
end

# Process with external tool
process = @step process = `./analyze matrix.csv -o stats.json`

# Load and report in Julia
report = @step report = () -> begin
    stats = JSON.parsefile("stats.json")
    println("Mean: $(stats["mean"])")
    println("Std:  $(stats["std"])")
end

pipeline = generate >> process >> report
run_pipeline(pipeline)
```

## Bioinformatics: NGS Alignment

A typical next-generation sequencing alignment workflow:

```julia
using SimplePipelines

# Quality control
fastqc = @step fastqc = `fastqc -o qc/ reads.fq.gz`

# Trim adapters
trim = @step trim = `trimmomatic SE reads.fq.gz trimmed.fq.gz ILLUMINACLIP:adapters.fa:2:30:10`

# Align to reference
align = @step align = `bwa mem -t 8 reference.fa trimmed.fq.gz > aligned.sam`

# Convert and sort
sort_bam = @step sort = `samtools sort -@ 4 -o sorted.bam aligned.sam`

# Index
index = @step index = `samtools index sorted.bam`

# Full pipeline
pipeline = fastqc >> trim >> align >> sort_bam >> index
run_pipeline(Pipeline(pipeline, name="NGS Alignment"))
```

## Bioinformatics: Multi-Sample Variant Calling

Process multiple samples in parallel, then joint-call variants:

```julia
using SimplePipelines

# Per-sample processing function
function sample_pipeline(name, fastq)
    trim = Step(Symbol("trim_", name), `trimmomatic SE $fastq $(name)_trimmed.fq.gz ...`)
    align = Step(Symbol("align_", name), `bwa mem ref.fa $(name)_trimmed.fq.gz > $(name).bam`)
    sort = Step(Symbol("sort_", name), `samtools sort -o $(name)_sorted.bam $(name).bam`)
    return trim >> align >> sort
end

# Build per-sample pipelines
sample_a = sample_pipeline("A", "sample_A.fq.gz")
sample_b = sample_pipeline("B", "sample_B.fq.gz")
sample_c = sample_pipeline("C", "sample_C.fq.gz")

# Joint variant calling (after all samples)
call = @step call = `bcftools mpileup -f ref.fa *_sorted.bam | bcftools call -mv -o variants.vcf`

# Filter variants
filter_vcf = @step filter = `bcftools filter -e 'QUAL<20' variants.vcf > filtered.vcf`

# Complete pipeline:
#   (A & B & C) >> call >> filter
pipeline = (sample_a & sample_b & sample_c) >> call >> filter_vcf

run_pipeline(Pipeline(pipeline, name="Multi-Sample Variant Calling"))
```

## Complex DAG

A workflow with multiple parallel stages:

```julia
using SimplePipelines

# Stage 1: Fetch from multiple sources (parallel)
fetch_db = @step db = `curl -o db_data.json https://api.database.com/export`
fetch_files = @step files = `rsync -av server:/data/ local_data/`

# Stage 2: Transform each source (parallel, after fetch)
transform_db = @step transform_db = () -> transform_database("db_data.json")
transform_files = @step transform_files = () -> transform_files("local_data/")

# Stage 3: Merge and analyze (sequential)
merge = @step merge = () -> merge_datasets("transformed_db.csv", "transformed_files.csv")
analyze = @step analyze = `./analysis_tool merged.csv -o results/`

# Stage 4: Generate outputs (parallel)
report = @step report = () -> generate_report("results/")
archive = @step archive = `tar -czvf results.tar.gz results/`

# Build DAG:
#   (fetch_db >> transform_db) & (fetch_files >> transform_files)
#   >> merge >> analyze
#   >> (report & archive)

db_branch = fetch_db >> transform_db
files_branch = fetch_files >> transform_files

pipeline = (db_branch & files_branch) >> merge >> analyze >> (report & archive)

# Preview structure
println("Pipeline structure:")
print_dag(pipeline)

# Execute
run_pipeline(pipeline)
```
