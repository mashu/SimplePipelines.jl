# Examples: bioinformatics

Longer domain workflows. Tool names are real; runs require installed software and inputs unless you substitute `echo` placeholders (see [`examples/bioinformatics.jl`](https://github.com/mashu/SimplePipelines.jl/blob/main/examples/bioinformatics.jl) in the repo).

**More examples:** [Basics](basics.md) · [Control flow](control-flow.md) · [Complex DAGs](complex-dags.md)

**Pattern:** Multiple donors/samples — use [`ForEach`](../guide/foreach-reduce.md) with `{wildcard}` patterns so files are discovered and each match becomes a parallel branch.

## 4.1 Immune repertoire (single donor)

**Flow:** Paired-end FASTQ → merge (PEAR) → FASTQ→FASTA → IgBLAST → Julia filter by identity.

```
  R1.fastq ──┐
             ├──►  pear  ──►  to_fasta  ──►  igblast  ──►  filter_identity  ──►  igblast_filtered.tsv
  R2.fastq ──┘
```

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
run(Pipeline(pipeline, name="Immune Repertoire"))
```

## 4.2 Immune repertoire (multiple donors)

**Flow:** `ForEach` discovers `fastq/{donor}_R1.fq.gz` → one branch per donor.

```
  ForEach("fastq/{donor}_R1.fq.gz")
       │
       ├── donor1: pear → fasta → igblast  ──┐
       ├── donor2: ...                      ──┼── (parallel)
       └── donorN: ...                      ──┘
```

```julia
using SimplePipelines

pipeline = ForEach("fastq/{donor}_R1.fq.gz") do donor
    sh("pear -f fastq/$(donor)_R1.fq.gz -r fastq/$(donor)_R2.fq.gz -o $(donor)_merged") >>
    sh("seqtk seq -A $(donor)_merged.assembled.fastq > $(donor).fasta") >>
    sh("igblastn -query $(donor).fasta -germline_db_V V.fasta -germline_db_D D.fasta -germline_db_J J.fasta -outfmt 7 -out $(donor)_igblast.tsv")
end

run(pipeline)
```

## 4.3 Variant calling

**Flow:** QC → trim → align → sort/index → call → index VCF → filter.

```
  R1.fq.gz ──┐
             ├──►  fastqc  ──►  trim  ──►  align  ──►  index  ──►  call  ──►  index_vcf  ──►  filter_vcf
  R2.fq.gz ──┘
```

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
run(Pipeline(pipeline, name="Variant Calling"))
```
