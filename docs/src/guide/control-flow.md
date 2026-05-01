# Control flow

Optional behaviors around failure, choice, and time limits.

## Fallback: `|`

If the primary fails, run the fallback:

```julia
fast_method = @step fast = sh"fast_tool input.txt"
slow_method = @step slow = sh"slow_tool input.txt"
pipeline = fast_method | slow_method

method_a = @step a = sh"method_a input"
method_b = @step b = sh"method_b input"
method_c = @step c = sh"method_c input"
pipeline = method_a | method_b | method_c
```

## Retry: `^` or `Retry()`

Retry a node up to N times on failure:

```julia
flaky_api_call = @step api = sh"echo 'mock response'"
pipeline = flaky_api_call^3

network_request = @step fetch = sh"echo 'data'"
pipeline = Retry(network_request, 5, delay=2.0)

primary = @step primary = sh"echo primary"
fallback = @step fallback = sh"echo fallback"
pipeline = primary^3 | fallback
```

## `Branch` (conditional)

Pick a branch using a runtime condition:

```julia
large_file_pipeline = @step large = sh"process_large data.txt"
small_file_pipeline = @step small = sh"process_small data.txt"
pipeline = Branch(
    () -> filesize("data.txt") > 1_000_000,
    large_file_pipeline,
    small_file_pipeline,
)

debug_steps = @step debug = sh"run with verbose logging"
normal_steps = @step normal = sh"run quietly"
pipeline = Branch(
    () -> haskey(ENV, "DEBUG"),
    debug_steps,
    normal_steps,
)
```

## `Timeout`

Fail if a node exceeds a time limit:

```julia
long_running_step = @step long = sh"sleep 1"
pipeline = Timeout(long_running_step, 30.0)

api_call = @step api = sh"echo result"
backup = @step backup = sh"echo fallback"
pipeline = Timeout(api_call, 5.0)^3 | backup
```

**Next:** [Fan-out and reduce](foreach-reduce.md) — `ForEach` and `Reduce`.
