# SimplePipelines.jl - Bioinformatics Example
# ============================================
# A simplified NGS alignment and variant calling workflow.

using SimplePipelines

println("═══ NGS Alignment Pipeline ═══\n")

# Define alignment workflow steps
# (Using echo to simulate the actual tools)

fastqc   = @step fastqc   = `echo "[FastQC] Quality control"`
trim     = @step trim     = `echo "[Trimmomatic] Trimming adapters"`
align    = @step align    = `echo "[BWA] Aligning to reference"`
sort_bam = @step sort     = `echo "[SAMtools] Sorting BAM"`
index    = @step index    = `echo "[SAMtools] Indexing BAM"`
markdup  = @step markdup  = `echo "[Picard] Marking duplicates"`

# Build sequential pipeline
alignment = fastqc >> trim >> align >> sort_bam >> index >> markdup

# Show structure and run
println("Pipeline structure:")
print_dag(alignment)
println()

run_pipeline(Pipeline(alignment, name="WGS Alignment"))


println("\n\n═══ Multi-Sample Variant Calling ═══\n")

# Process multiple samples in parallel, then joint-call

# Per-sample processing (simplified)
sample_a = @step sample_a = `echo "[Sample A] Processing"`
sample_b = @step sample_b = `echo "[Sample B] Processing"`
sample_c = @step sample_c = `echo "[Sample C] Processing"`

# Joint calling (requires all samples)
joint_call = @step call = `echo "[BCFtools] Joint variant calling"`
filter_vcf = @step filter = `echo "[BCFtools] Filtering variants"`
annotate   = @step annotate = `echo "[SnpEff] Annotating variants"`

# Build pipeline: parallel samples -> sequential calling
pipeline = (sample_a & sample_b & sample_c) >> joint_call >> filter_vcf >> annotate

println("Pipeline structure:")
print_dag(pipeline)
println()

results = run_pipeline(Pipeline(pipeline, name="Variant Calling"))

# Summary
println("\nExecution Summary:")
for r in results
    status = r.success ? "✓" : "✗"
    println("  $status $(r.step.name): $(round(r.duration * 1000, digits=1))ms")
end
