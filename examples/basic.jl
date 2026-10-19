# SimplePipelines.jl — basic examples. Run with `julia --project examples/basic.jl`.

using SimplePipelines

cd(mktempdir())

println("═══ 1. Steps with declared files ═══")
prepare = @step prepare([] => "data.txt") = "seq 10 -1 1 > {output}"
sorted = @step sorted("data.txt" => "sorted.txt") = "sort -n {input} > {output}"
lines = @step lines("sorted.txt" => "lines.txt") = "wc -l < {input} > {output}"
total = @step total("sorted.txt" => "total.txt") = "paste -sd+ {input} | bc > {output}"
pipeline = prepare >> sorted >> (lines & total)
print_dag(pipeline)
run(pipeline)

println("\n═══ 2. A second run skips what is up to date ═══")
run(pipeline)

println("\n═══ 3. Julia functions and values ═══")
stats = @step stats("sorted.txt" => "stats.txt") = function (input, output)
    xs = parse.(Int, readlines(input))
    write(output, "mean=$(sum(xs) / length(xs))\n")
end
show_stats = @step show_stats = paths -> read(only(paths), String)
println(run(sorted >> stats >> show_stats; verbose=false).value)

println("═══ 4. Parallel values ═══")
squares = ForEach(1:5) do i
    @step square = () -> i^2
end
println("sum of squares: ", run(squares >> sum; verbose=false).value)

println("\n═══ 5. Failure handling ═══")
flaky = @step flaky = sh"test $((RANDOM % 2)) -eq 0"
backup = @step backup = sh"echo backup used"
result = run(Retry(flaky, 3) | backup)
println("success: ", success(result))

println("\n═══ 6. Inspecting without running ═══")
check(pipeline >> stats)
