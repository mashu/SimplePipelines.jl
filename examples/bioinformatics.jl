# SimplePipelines.jl — a variant-calling workflow with placeholder commands.
# Run with `julia --project examples/bioinformatics.jl`. Replace the commands by the
# real tools (fastp, bwa, samtools, bcftools) for actual data.

using SimplePipelines

cd(mktempdir())

# Simulated paired-end reads for four samples, one of them without its R2 file.
mkpath("raw")
for s in ["S1", "S2", "S3"], r in ["R1", "R2"]
    write("raw/$(s)_$(r).fq.gz", "reads $s $r\n")
end
write("raw/S4_R1.fq.gz", "reads S4 R1\n")
mkpath("ref")
write("ref/genome.fa", ">chr1\nACGT\n")

index_ref = @step index_ref("ref/genome.fa" => "ref/genome.fa.bwt") =
    "echo '[bwa index] {input}' > {output}"

trim = @rule trim(["raw/{s}_R1.fq.gz", "raw/{s}_R2.fq.gz"] => "trim/{s}.fq.gz"; threads=2) =
    "echo '[fastp -w {threads}]' | cat - {input} > {output}"

align = @rule align(["trim/{s}.fq.gz", "ref/genome.fa.bwt"] => "bam/{s}.bam"; threads=4, mem_mb=500) =
    "sleep 0.2; echo '[bwa mem -t {threads} | samtools sort] {s}' | cat - {input[1]} > {output}"

index = @rule index("bam/{s}.bam" => "bam/{s}.bam.bai") = "echo '[samtools index]' > {output}"

call = @rule call(gather("bam/{s}.bam") => "calls/all.vcf") =
    "echo '[bcftools mpileup | call] {input}' > {output}"

wf = Workflow(index_ref, trim, align, index, call; name="variant calling")

println("Samples on disk: ", [w.s for w in discover("raw/{s}_R1.fq.gz")])
println("S4 has no R2 file, so it is not a target.\n")

print_dag(wf)
println()
check(wf)
println()
result = run(wf; jobs=8)

println("\nTargets: ", result.value)
println("\n", read("calls/all.vcf", String))

println("Adding a sample reruns only that sample and the joint call:")
write("raw/S4_R2.fq.gz", "reads S4 R2\n")
run(wf; jobs=8)
