# SimplePipelines.jl - Bioinformatics Examples
# ============================================
# Immune repertoire (PEAR → IgBLAST → Julia filter) and variant calling (FastQC → trim → BWA → bcftools).
# Uses echo to simulate tools; replace with real commands for actual runs.

using SimplePipelines

println("═══ Immune Repertoire Pipeline ═══\n")

# Paired-end FASTQ → PEAR (merge) → FASTQ to FASTA → IgBLAST (V/D/J) → Julia filter
pear       = @step pear       = `echo "[PEAR] Merging paired-end R1.fastq R2.fastq"`
to_fasta   = @step to_fasta   = `echo "[seqtk] Converting merged.assembled.fastq to FASTA"`
igblast    = @step igblast    = `echo "[IgBLAST] V/D/J assignment with V.fasta D.fasta J.fasta"`
filter_id  = @step filter_id  = () -> "Filtered by v_identity and j_identity > 90.0"

immune = pear >> to_fasta >> igblast >> filter_id
println("Pipeline structure:")
print_dag(immune)
println()
run_pipeline(Pipeline(immune, name="Immune Repertoire"))


println("\n═══ Multi-Donor Immune Repertoire (ForEach) ═══\n")

# Create temp files to simulate multiple donors
dir = mktempdir()
for donor in ["donor1", "donor2", "donor3"]
    touch(joinpath(dir, "$(donor)_R1.fq.gz"))
end

# ForEach: discovers files, creates parallel branches - just return Cmd!
cd(dir) do
    pipeline = ForEach("{donor}_R1.fq.gz") do donor
        `echo "[Processing $donor]"`  # Simple: just return a Cmd
    end
    println("Pipeline structure (3 donors discovered):")
    print_dag(pipeline)
    println()
    run_pipeline(Pipeline(pipeline, name="Multi-Donor"))
end

rm(dir; recursive=true)


println("\n═══ Variant Calling Pipeline ═══\n")

# Paired-end → FastQC → Trimmomatic → BWA (GRCh38) → bcftools call → filter
fastqc   = @step fastqc   = `echo "[FastQC] R1.fq.gz R2.fq.gz"`
trim     = @step trim     = `echo "[Trimmomatic PE] Trim adapters and quality"`
align    = @step align    = `echo "[BWA mem] Align to GRCh38.fa"`
index    = @step index    = `echo "[samtools index] aligned.bam"`
call     = @step call     = `echo "[bcftools mpileup/call] variants.vcf.gz"`
filter_v = @step filter_v = `echo "[bcftools filter] QUAL>=20"`

variant = fastqc >> trim >> align >> index >> call >> filter_v
println("Pipeline structure:")
print_dag(variant)
println()
results = run_pipeline(Pipeline(variant, name="Variant Calling"))

println("\nExecution summary:")
for r in results
    status = r.success ? "✓" : "✗"
    println("  $status $(r.step.name): $(round(r.duration * 1000, digits=1))ms")
end
