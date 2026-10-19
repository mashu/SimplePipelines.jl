# Examples: basics

Short, runnable recipes. Each one works in an empty directory.

## A diamond

```julia
using SimplePipelines

fetch   = @step fetch([] => "data.csv") = "printf 'id,value\n1,10\n2,20\n' > {output}"
rows    = @step rows("data.csv" => "rows.txt") = "tail -n +2 {input} | wc -l > {output}"
total   = @step total("data.csv" => "total.txt") = "tail -n +2 {input} | cut -d, -f2 | paste -sd+ | bc > {output}"
summary = @step summary(["rows.txt", "total.txt"] => "summary.txt") = "paste {input} > {output}"

run(fetch >> (rows & total) >> summary)
```

## Julia and shell together

```julia
numbers = @step numbers([] => "numbers.txt") = "seq 1 100 > {output}"
stats = @step stats("numbers.txt" => "stats.txt") = function (input, output)
    xs = parse.(Int, readlines(input))
    write(output, "mean=$(sum(xs) / length(xs)) max=$(maximum(xs))\n")
end
show_stats = @step show_stats = paths -> read(only(paths), String)

run(numbers >> stats >> show_stats).value       # "mean=50.5 max=100\n"
```

## Values without files

```julia
a = @step a = () -> 2
b = @step b = () -> 3
product = @step product = xs -> prod(xs)

run((a & b) >> product).value                   # 6
```

## One step per file

```julia
for name in ["a", "b", "c"]
    write("$name.txt", "hello $name\n")
end

upper = @rule upper("{name}.txt" => "upper/{name}.txt") = "tr a-z A-Z < {input} > {output}"
run(upper)                       # three steps: upper[name=a], upper[name=b], upper[name=c]
```

## Retry, fallback and timeout

```julia
flaky   = @step flaky = sh"test $((RANDOM % 3)) -eq 0"
backup  = @step backup = sh"echo using the backup"
pipeline = Timeout(flaky^5 | backup, 60)

run(pipeline)
```

## Inspect first

```julia
print_dag(fetch >> (rows & total) >> summary)
check(fetch >> (rows & total) >> summary)
```
