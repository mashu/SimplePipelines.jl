# Verbosity logging routed through RunContext. All printing is serialized via
# ctx.log_lock so concurrent Parallel/ForEach branches do not interleave output.

with_log(f, ctx::RunContext) = ctx.verbose ? lock(f, ctx.log_lock) : nothing

# Most log_* functions just emit `<colored prefix><message>` under the lock.
log_event(ctx::RunContext, prefix, color::Symbol, msg; bold::Bool=false) =
    with_log(ctx) do
        printstyled(prefix; color=color, bold=bold)
        println(msg)
    end

log_start(ctx::RunContext, s::Step)    = log_event(ctx, "▶ Running: ", :cyan,    step_label(s))
log_parallel(ctx::RunContext, n::Int)  = log_event(ctx, "⊕ ",          :magenta, "Running $n branches in parallel...")
log_retry(ctx::RunContext, n, max)     = log_event(ctx, "↻ ",          :yellow,  "Attempt $n/$max")
log_fallback(ctx::RunContext)          = log_event(ctx, "↯ ",          :yellow,  "Primary failed, trying fallback...")
log_branch(ctx::RunContext, c::Bool)   = log_event(ctx, "? ",          :blue,    "Condition: $(c ? "true → if_true" : "false → if_false")")
log_timeout(ctx::RunContext, s::Float64) = log_event(ctx, "⏱ ",        :cyan,    "Timeout: $(s)s")
log_force(ctx::RunContext)             = log_event(ctx, "⚡ ",          :yellow,  "Forcing execution..."; bold=true)
log_reduce(ctx::RunContext, n::Symbol) = log_event(ctx, "⊛ ",          :magenta, "Reducing: $n")

# log_skip prints the step name in the same dim colour as the prefix; doesn't fit log_event's plain-message shape.
log_skip(ctx::RunContext, s::Step) = with_log(ctx) do
    printstyled("⊳ Up to date: ", color=:light_black)
    printstyled(step_label(s), "\n", color=:light_black)
end

function log_result(ctx::RunContext, r::AbstractStepResult)
    with_log(ctx) do
        marker, color = r.success ? ("  ✓ ", :green) : ("  ✗ ", :red)
        printstyled(marker; color=color, bold=!r.success)
        println("Completed in $(round(r.duration, digits=2))s")
        if !r.success
            printstyled("  Error: ", color=:red)
            println(r.result)
        end
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

const CMD_LOG_PREFIX = shell_raw"  $ "

log_cmd(ctx::RunContext, cmd::Cmd)              = log_event(ctx, CMD_LOG_PREFIX, :light_black, join(cmd.exec, " "))
log_cmd(ctx::RunContext, cmd::Base.AbstractCmd) = log_event(ctx, CMD_LOG_PREFIX, :light_black, string(cmd))
log_cmd(ctx::RunContext, cmd::AbstractString)   = log_event(ctx, CMD_LOG_PREFIX, :light_black, cmd)
