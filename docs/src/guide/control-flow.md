# Control flow

Control-flow nodes wrap other nodes with a policy. They compose with everything
else.

## Retry

```julia
download = @step download([] => "data.tar") = "curl -fsSL -o {output} https://example.org/data.tar"

download^3                         # up to three attempts
Retry(download, 5; delay=10)       # five attempts, 10 s apart
```

Only the parts that failed run again: a successful upstream step inside the retried
node is not repeated.

## Fallback

```julia
fast = @step fast("in.txt" => "out.txt") = "fast_tool {input} > {output}"
slow = @step slow("in.txt" => "out.txt") = "slow_tool {input} > {output}"

fast | slow
Retry(fast, 2) | slow
```

The fallback runs only if the primary fails. A pipeline that recovers through a
fallback succeeds (`success(result)` is true), while the primary's failure stays
visible in the results.

## Branch

```julia
Branch(() -> filesize("data.txt") > 10^9, big_file_path, small_file_path)
@branch isfile("cache.bin") load_cache rebuild          # condition written inline
```

The condition runs when the branch is reached. It may take the upstream value:

```julia
detect = @step detect = () -> occursin("PE", read("meta.txt", String))
detect >> Branch(paired -> paired, paired_end_steps, single_end_steps)
```

It must return a `Bool`; anything else, or an exception, is reported as a failure.

## Timeout

```julia
Timeout(long_step, 3600)
```

If the node is not done in time, the timeout fails with kind `:timed_out`. Commands
still running are terminated, together with everything they started (a shell, a
pipe of tools, their children). Steps inside the node that have not started yet are
not started. A Julia function that is already running cannot be stopped safely;
the pipeline stops waiting for it and gives its CPU slots and memory charge to other
steps (the function keeps running in the background until it returns).

A step's own `timeout` (`@step s(; timeout=600) = ...`) does the same for its
command, without a `Timeout` node.

## Force

```julia
Force(step)                  # run even if up to date
run(pipeline; force=true)    # the same, for everything
```

**Next:** [Running and freshness](running.md).
