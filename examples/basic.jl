# SimplePipelines.jl - Basic Examples
# ====================================

using SimplePipelines

println("═══ Example 1: Sequential Execution ═══\n")

# Chain shell commands with >>
pipeline = sh"echo 'Step 1: Downloading'" >> 
           sh"echo 'Step 2: Processing'" >> 
           sh"echo 'Step 3: Uploading'"

run(pipeline)


println("\n═══ Example 2: Named Steps ═══\n")

# Use @step macro for named steps
download = @step download = sh"echo 'Downloading data...'"
process  = @step process  = sh"echo 'Processing data...'"
upload   = @step upload   = sh"echo 'Uploading results...'"

pipeline = download >> process >> upload
run(pipeline)


println("\n═══ Example 3: Parallel Execution ═══\n")

# Use & for parallel execution
task_a = @step task_a = sh"sleep 0.2"
task_b = @step task_b = sh"sleep 0.2"
task_c = @step task_c = sh"sleep 0.2"
merge  = @step merge  = sh"echo 'Merging results'"

# All tasks run in parallel (~0.2s total, not 0.6s)
pipeline = (task_a & task_b & task_c) >> merge
run(pipeline)


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
run(pipeline)


println("\n═══ Example 5: Complex DAG ═══\n")

# Diamond pattern: fetch -> (branch_a & branch_b) -> merge
fetch    = @step fetch    = sh"echo 'Fetching data'"
branch_a = @step branch_a = sh"echo 'Analysis A'"
branch_b = @step branch_b = sh"echo 'Analysis B'"
report   = @step report   = sh"echo 'Generating report'"

pipeline = fetch >> (branch_a & branch_b) >> report

# Preview structure
println("DAG structure:")
print_dag(pipeline)
println()

run(pipeline)


println("\n═══ Example 6: Fallback ═══\n")

# If primary fails, run fallback
primary = @step primary = sh"false"  # Always fails
fallback = @step fallback = sh"echo 'Fallback succeeded'"

pipeline = primary | fallback
run(pipeline)


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
run(pipeline)


println("\n═══ Example 8: Branch (Conditional) ═══\n")

# Choose path based on condition
use_fast = Ref(true)

fast_path = @step fast = sh"echo 'Taking fast path'"
slow_path = @step slow = sh"echo 'Taking slow path'"

pipeline = Branch(() -> use_fast[], fast_path, slow_path)

println("With use_fast=true:")
run(pipeline)

use_fast[] = false
println("\nWith use_fast=false:")
run(pipeline)


println("\n═══ Example 9: Combined Error Handling ═══\n")

# Robust pipeline: retry primary, fallback if all retries fail
primary = @step primary = sh"false"  # Always fails
backup = @step backup = sh"echo 'Backup method succeeded'"

pipeline = Retry(primary, 2) | backup
run(pipeline)


println("\n═══ Example 10: Dry Run ═══\n")

# Preview what would execute without running
complex = (sh"step 1" >> sh"step 2") & (sh"step 3" >> sh"step 4") >> sh"step 5"

println("Pipeline structure (dry run):")
run(complex, dry_run=true)
