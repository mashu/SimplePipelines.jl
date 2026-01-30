# SimplePipelines.jl - Basic Examples
# ====================================

using SimplePipelines

println("═══ Example 1: Sequential Execution ═══\n")

# Chain shell commands with >>
pipeline = `echo "Step 1: Downloading"` >> 
           `echo "Step 2: Processing"` >> 
           `echo "Step 3: Uploading"`

run_pipeline(pipeline)


println("\n═══ Example 2: Named Steps ═══\n")

# Use @step macro for named steps
download = @step download = `echo "Downloading data..."`
process  = @step process  = `echo "Processing data..."`
upload   = @step upload   = `echo "Uploading results..."`

pipeline = download >> process >> upload
run_pipeline(pipeline)


println("\n═══ Example 3: Parallel Execution ═══\n")

# Use & for parallel execution
task_a = @step task_a = `sleep 0.2`
task_b = @step task_b = `sleep 0.2`
task_c = @step task_c = `sleep 0.2`
merge  = @step merge  = `echo "Merging results"`

# All tasks run in parallel (~0.2s total, not 0.6s)
pipeline = (task_a & task_b & task_c) >> merge
run_pipeline(pipeline)


println("\n═══ Example 4: Julia Functions ═══\n")

# Mix Julia code with shell commands
data = Ref{Vector{Float64}}()

generate = @step generate = () -> begin
    println("  Generating random data...")
    data[] = rand(100)
    return "Generated $(length(data[])) values"
end

analyze = @step analyze = () -> begin
    println("  Analyzing data...")
    println("    Mean: $(sum(data[]) / length(data[]))")
    println("    Max:  $(maximum(data[]))")
end

pipeline = generate >> analyze
run_pipeline(pipeline)


println("\n═══ Example 5: Complex DAG ═══\n")

# Diamond pattern: fetch -> (branch_a & branch_b) -> merge
fetch    = @step fetch    = `echo "Fetching data"`
branch_a = @step branch_a = `echo "Analysis A"`
branch_b = @step branch_b = `echo "Analysis B"`
report   = @step report   = `echo "Generating report"`

pipeline = fetch >> (branch_a & branch_b) >> report

# Preview structure
println("DAG structure:")
print_dag(pipeline)
println()

run_pipeline(pipeline)


println("\n═══ Example 6: Fallback ═══\n")

# If primary fails, run fallback
primary = @step primary = `false`  # Always fails
fallback = @step fallback = `echo "Fallback succeeded"`

pipeline = primary | fallback
run_pipeline(pipeline)


println("\n═══ Example 7: Retry ═══\n")

# Retry a flaky step (simulated here)
attempt_count = Ref(0)
flaky = @step flaky = () -> begin
    attempt_count[] += 1
    if attempt_count[] < 3
        error("Attempt $(attempt_count[]) failed")
    end
    println("  Success on attempt $(attempt_count[])!")
    return "ok"
end

pipeline = Retry(flaky, 3)
run_pipeline(pipeline)


println("\n═══ Example 8: Branch (Conditional) ═══\n")

# Choose path based on condition
use_fast = Ref(true)

fast_path = @step fast = `echo "Taking fast path"`
slow_path = @step slow = `echo "Taking slow path"`

pipeline = Branch(() -> use_fast[], fast_path, slow_path)

println("With use_fast=true:")
run_pipeline(pipeline)

use_fast[] = false
println("\nWith use_fast=false:")
run_pipeline(pipeline)


println("\n═══ Example 9: Combined Error Handling ═══\n")

# Robust pipeline: retry primary, fallback if all retries fail
primary = @step primary = `false`  # Always fails
backup = @step backup = `echo "Backup method succeeded"`

pipeline = Retry(primary, 2) | backup
run_pipeline(pipeline)


println("\n═══ Example 10: Dry Run ═══\n")

# Preview what would execute without running
complex = (`step 1` >> `step 2`) & (`step 3` >> `step 4`) >> `step 5`

println("Pipeline structure (dry run):")
run_pipeline(complex, dry_run=true)
