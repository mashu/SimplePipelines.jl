# Verbosity logging (Verbose/Silent dispatch). Requires Types.jl.

log_start(::Silent, ::Step) = nothing
function log_start(::Verbose, s::Step)
    printstyled("▶ Running: ", color=:cyan)
    println(step_label(s))
end

log_skip(::Silent, ::Step) = nothing
function log_skip(::Verbose, s::Step)
    printstyled("⊳ Up to date: ", color=:light_black)
    printstyled(step_label(s), "\n", color=:light_black)
end

log_result(::Silent, ::AbstractStepResult) = nothing
function log_result(::Verbose, r::AbstractStepResult)
    if r.success
        printstyled("  ✓ ", color=:green)
        println("Completed in $(round(r.duration, digits=2))s")
    else
        printstyled("  ✗ ", color=:red, bold=true)
        println("Completed in $(round(r.duration, digits=2))s")
        printstyled("  Error: ", color=:red)
        println(r.output)
    end
end

log_parallel(::Silent, ::Int) = nothing
function log_parallel(::Verbose, n::Int)
    printstyled("⊕ ", color=:magenta)
    println("Running $n branches in parallel...")
end

log_progress(::Silent, ::Int, ::Int) = nothing
function log_progress(::Verbose, done::Int, total::Int)
    left = total - done
    printstyled("  → ", color=:light_black)
    printstyled("$done/$total", color=:cyan)
    printstyled(" done", color=:light_black)
    println(left > 0 ? " ($left left)" : "")
end

log_retry(::Silent, ::Int, ::Int) = nothing
function log_retry(::Verbose, n::Int, max::Int)
    printstyled("↻ ", color=:yellow)
    println("Attempt $n/$max")
end

log_fallback(::Silent) = nothing
function log_fallback(::Verbose)
    printstyled("↯ ", color=:yellow)
    println("Primary failed, trying fallback...")
end

log_branch(::Silent, ::Bool) = nothing
function log_branch(::Verbose, c::Bool)
    printstyled("? ", color=:blue)
    println("Condition: $(c ? "true → if_true" : "false → if_false")")
end

log_timeout(::Silent, ::Float64) = nothing
function log_timeout(::Verbose, s::Float64)
    printstyled("⏱ ", color=:cyan)
    println("Timeout: $(s)s")
end

log_force(::Silent) = nothing
function log_force(::Verbose)
    printstyled("⚡ ", color=:yellow, bold=true)
    println("Forcing execution...")
end

log_reduce(::Silent, ::Symbol) = nothing
function log_reduce(::Verbose, n::Symbol)
    printstyled("⊛ ", color=:magenta)
    println("Reducing: $n")
end
