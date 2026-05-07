# Verbosity logging routed through RunContext. All printing is serialized via
# ctx.log_lock so concurrent Parallel/ForEach branches do not interleave output.

with_log(f, ctx::RunContext) = ctx.verbose ? lock(f, ctx.log_lock) : nothing

function log_start(ctx::RunContext, s::Step)
    with_log(ctx) do
        printstyled("▶ Running: ", color=:cyan)
        println(step_label(s))
    end
end

function log_skip(ctx::RunContext, s::Step)
    with_log(ctx) do
        printstyled("⊳ Up to date: ", color=:light_black)
        printstyled(step_label(s), "\n", color=:light_black)
    end
end

function log_result(ctx::RunContext, r::AbstractStepResult)
    with_log(ctx) do
        if r.success
            printstyled("  ✓ ", color=:green)
            println("Completed in $(round(r.duration, digits=2))s")
        else
            printstyled("  ✗ ", color=:red, bold=true)
            println("Completed in $(round(r.duration, digits=2))s")
            printstyled("  Error: ", color=:red)
            println(r.result)
        end
    end
end

function log_parallel(ctx::RunContext, n::Int)
    with_log(ctx) do
        printstyled("⊕ ", color=:magenta)
        println("Running $n branches in parallel...")
    end
end

function log_progress(ctx::RunContext, done::Int, total::Int)
    with_log(ctx) do
        left = total - done
        printstyled("  → ", color=:light_black)
        printstyled("$done/$total", color=:cyan)
        printstyled(" done", color=:light_black)
        println(left > 0 ? " ($left left)" : "")
    end
end

function log_retry(ctx::RunContext, n::Int, max::Int)
    with_log(ctx) do
        printstyled("↻ ", color=:yellow)
        println("Attempt $n/$max")
    end
end

function log_fallback(ctx::RunContext)
    with_log(ctx) do
        printstyled("↯ ", color=:yellow)
        println("Primary failed, trying fallback...")
    end
end

function log_branch(ctx::RunContext, c::Bool)
    with_log(ctx) do
        printstyled("? ", color=:blue)
        println("Condition: $(c ? "true → if_true" : "false → if_false")")
    end
end

function log_timeout(ctx::RunContext, s::Float64)
    with_log(ctx) do
        printstyled("⏱ ", color=:cyan)
        println("Timeout: $(s)s")
    end
end

function log_force(ctx::RunContext)
    with_log(ctx) do
        printstyled("⚡ ", color=:yellow, bold=true)
        println("Forcing execution...")
    end
end

function log_reduce(ctx::RunContext, n::Symbol)
    with_log(ctx) do
        printstyled("⊛ ", color=:magenta)
        println("Reducing: $n")
    end
end

const cmd_log_prefix = shell_raw"  $ "

function log_cmd(ctx::RunContext, cmd::Cmd)
    with_log(ctx) do
        printstyled(cmd_log_prefix, color=:light_black)
        println(join(cmd.exec, " "))
    end
end

# OrCmds / AndCmds / CmdRedirect (the result of `Base.pipeline(...)` and `sh_pipe`)
# render as a human-readable shell pipeline via their `string` method.
function log_cmd(ctx::RunContext, cmd::Base.AbstractCmd)
    with_log(ctx) do
        printstyled(cmd_log_prefix, color=:light_black)
        println(string(cmd))
    end
end

function log_cmd(ctx::RunContext, cmd::AbstractString)
    with_log(ctx) do
        printstyled(cmd_log_prefix, color=:light_black)
        println(cmd)
    end
end
