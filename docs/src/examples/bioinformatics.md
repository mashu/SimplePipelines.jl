# Examples: bioinformatics

Sketches of complete workflows. They need the tools installed; the commands are the
usual ones. A runnable version with placeholder commands is in
[`examples/bioinformatics.jl`](https://github.com/mashu/SimplePipelines.jl/blob/main/examples/bioinformatics.jl).

## Variant calling for every sample

```text
raw/{s}_R1.fq.gz ─┐
                  ├─ trim ── align ── markdup ─┬─ index
raw/{s}_R2.fq.gz ─┘                            └─ (all samples) ── call ── filter
```

```julia
using SimplePipelines

const REF = "ref/GRCh38.fa"

index_ref = @step index_ref(REF => REF * ".bwt") = "bwa index {input}"

trim = @rule trim(["raw/{s}_R1.fq.gz", "raw/{s}_R2.fq.gz"] =>
                  ["trim/{s}_R1.fq.gz", "trim/{s}_R2.fq.gz"]; threads=4) =
    "fastp -w {threads} -i {input[1]} -I {input[2]} -o {output[1]} -O {output[2]}"

align = @rule align(["trim/{s}_R1.fq.gz", "trim/{s}_R2.fq.gz", REF * ".bwt"] => "bam/{s}.bam";
                    threads=8, mem_mb=16_000) =
    "bwa mem -t {threads} -R '@RG\\tID:{s}\\tSM:{s}' $REF {input[1]} {input[2]} | " *
    "samtools sort -@ {threads} -o {output}"

markdup = @rule markdup("bam/{s}.bam" => "dedup/{s}.bam"; threads=4) =
    "samtools markdup -@ {threads} {input} {output}"

index = @rule index("dedup/{s}.bam" => "dedup/{s}.bam.bai") = "samtools index {input}"

call = @rule call(gather("dedup/{s}.bam") => "calls/raw.vcf.gz"; threads=8) =
    "bcftools mpileup --threads {threads} -f $REF {input} | " *
    "bcftools call -mv -Oz -o {output}"

filter_calls = @rule filter_calls("calls/raw.vcf.gz" => "calls/filtered.vcf.gz") =
    "bcftools filter -i 'QUAL>=20' -Oz -o {output} {input}"

wf = Workflow(index_ref, trim, align, markdup, index, call, filter_calls; name="variants")

check(wf)                     # what will run for the samples on disk
run(wf; jobs=32, mem_mb=96_000)
```

What this saves you:

- no sample list: samples are the pairs present in `raw/`, and a sample with only
  `R1` is left out;
- the reference is indexed once, before any alignment;
- `call` waits for every sample and reruns when a sample is added;
- at most 32 threads and 96 GB of declared memory are in use at any time, and
  samples are finished a few at a time;
- a failed alignment removes its partial BAM, and `bwa | samtools` fails if either
  tool fails.

Only part of the samples: `run(wf; targets="dedup/{s}.bam.bai", filter = w -> startswith(w.s, "P1_"))`.

## RNA-seq quantification

```julia
quant = @rule quant(["raw/{s}_R1.fq.gz", "raw/{s}_R2.fq.gz"] => "quant/{s}/abundance.tsv"; threads=4) =
    "kallisto quant -t {threads} -i ref/transcripts.idx -o quant/{s} {input}"

matrix = @rule matrix(gather("quant/{s}/abundance.tsv") => "counts.tsv") =
    function (files, output)
        samples = [basename(dirname(f)) for f in files]
        tables = [split.(readlines(f)[2:end], '\t') for f in files]
        open(output, "w") do io
            println(io, join(["target"; samples], '\t'))
            for (i, row) in enumerate(first(tables))
                println(io, join([row[1]; [t[i][4] for t in tables]], '\t'))
            end
        end
    end

run(quant >> matrix)
```

The Julia function receives the gathered abundance files as a vector and the output
path; it runs once all quantifications are done.

## Immune repertoire, one branch per donor

`ForEach` suits a per-donor chain written as ordinary Julia:

```julia
donors = ForEach("fastq/{donor}_R1.fq.gz") do d
    merge = @step merge(["fastq/$(d)_R1.fq.gz", "fastq/$(d)_R2.fq.gz"] => "merged/$d.assembled.fastq"; threads=4) =
        "pear -j {threads} -f {input[1]} -r {input[2]} -o merged/$d"
    fasta = @step fasta("merged/$d.assembled.fastq" => "fasta/$d.fasta") = "seqtk seq -A {input} > {output}"
    igblast = @step igblast("fasta/$d.fasta" => "igblast/$d.tsv"; threads=4) =
        "igblastn -num_threads {threads} -query {input} -germline_db_V db/V -germline_db_D db/D " *
        "-germline_db_J db/J -outfmt 19 -out {output}"
    merge >> fasta >> igblast
end

run(donors)
```

The same workflow as rules (`@rule merge("fastq/{d}_R1.fq.gz" => ...)`) would also
let you build a single donor by its output name.
