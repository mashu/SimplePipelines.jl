# Control flow

Control flow wraps an existing node with a policy: try again, use a backup,
choose one branch, or stop waiting after a deadline. Use these when the workflow
needs operational behavior, not as a substitute for clear DAG structure.

## Use A Backup With `|`

Fallback is useful when two methods can produce an acceptable result and the
second should run only if the first fails.

```julia
fast_method = @step fast = sh"fast_tool input.txt"
slow_method = @step slow = sh"slow_tool input.txt"

pipeline = fast_method | slow_method
```

This is different from `fast_method & slow_method`: fallback does not spend time
on the backup unless it is needed.

## Retry A Flaky Step

```julia
flaky_api_call = @step api = sh"echo 'mock response'"
pipeline = flaky_api_call^3
```

Use `Retry` when you want a delay:

```julia
fetch = @step fetch = sh"curl -L -o data.txt https://example.com/data.txt"
pipeline = Retry(fetch, 5; delay=2.0)
```

Retry composes with fallback:

```julia
pipeline = Retry(fetch, 3; delay=1.0) | @step cached = sh"cp cache.txt data.txt"
```

## Choose A Branch

```julia
large_file_pipeline = @step large = sh"process_large data.txt"
small_file_pipeline = @step small = sh"process_small data.txt"

pipeline = Branch(
    () -> filesize("data.txt") > 1_000_000,
    large_file_pipeline,
    small_file_pipeline,
)
```

Use the `@branch` macro for the common zero-argument case:

```julia
use_fast = Ref(true)
pipeline = @branch use_fast[] fast_method slow_method
```

When a branch follows a step that passes a value forward, the predicate can
accept that value:

```julia
emit_path = @step emit_path = () -> "data.csv"
csv_step = @step csv = sh"process_csv data.csv"
txt_step = @step txt = sh"process_txt data.txt"

pipeline = emit_path >> Branch(path -> endswith(path, ".csv"), csv_step, txt_step)
```

## Stop Waiting With `Timeout`

```julia
long_running_step = @step long = sh"sleep 1"
pipeline = Timeout(long_running_step, 30.0)
```

Timeout returns a failed result if the deadline passes. It reports
`:timed_out` for a timeout and `:inner_exception` if the wrapped node throws
before finishing. External shell subprocesses may continue after Julia stops
waiting for them, so treat timeout as a pipeline-level failure signal rather
than a guaranteed process killer.

## What To Remember

Keep the main DAG readable first. Add retry, fallback, branching, or timeout only
where the workflow really needs that behavior.

**Next:** [Fan-out and reduce](foreach-reduce.md) shows how to repeat a branch
over many items and combine results.
