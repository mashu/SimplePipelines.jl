# Progress output of `run` and `check`. Printing happens under a lock so lines from
# parallel steps never interleave. Up-to-date steps are only counted, not listed.

@nospecialize   # see runtime.jl

with_log(f, rt::Runtime) = rt.verbose ? lock(f, rt.log_lock) : nothing

function log_line(rt::Runtime, marker, color::Symbol, text; detail="")
    with_log(rt) do
        printstyled(marker; color, bold=true)
        println(text)
        isempty(detail) || printstyled(indent(detail, "    "), "\n"; color=:light_black)
    end
end

indent(text::AbstractString, prefix) = join((prefix * l for l in split(rstrip(text), '\n')), "\n")

function log_start(rt::Runtime, label::String, work)
    with_log(rt) do
        printstyled("▶ "; color=:cyan, bold=true)
        println(label)
        cmd = command_text(work)
        isempty(cmd) || printstyled(indent(shorten(cmd, 400), "    \$ "), "\n"; color=:light_black)
    end
end

command_text(s::Shell) = s.script
command_text(t::Template) = preview(t.script, t.ctx)
command_text(j::JuliaScript) = "julia " * preview(j.template, j.ctx)
command_text(c::Cmd) = join(c.exec, " ")
command_text(c::Base.AbstractCmd) = string(c)
command_text(_) = ""

log_result(rt::Runtime, r::StepResult) = log_status(rt, Val(r.status), r)
log_status(rt::Runtime, ::Val{:done}, r) =
    log_line(rt, "✓ ", :green, "$(r.label) ($(seconds(r.duration))$(peak_text(r.peak_mb)))")
log_status(rt::Runtime, ::Val{:skipped}, r) = nothing
log_status(rt::Runtime, ::Val{:planned}, r) = log_line(rt, "▶ ", :cyan, "$(r.label)  ($(r.result))")
log_status(rt::Runtime, ::Val{:failed}, r) =
    log_line(rt, "✗ ", :red, "$(r.label): $(r.result.message)"; detail=tail_lines(r.result.detail, 15))

tail_lines(text::AbstractString, n::Int) = join(last(split(rstrip(text), '\n'), n), "\n")
seconds(t::Real) = t < 10 ? "$(round(t; digits=2))s" : "$(round(Int, t))s"

peak_text(::Nothing) = ""
peak_text(mb::Int) = mb > 0 ? ", peak " * memory_text(mb) : ""
memory_text(mb::Int) = mb < 1024 ? "$mb MiB" : "$(round(mb / 1024; digits=1)) GiB"

log_retry(rt::Runtime, attempt, max, hint::String) = log_line(rt, "↻ ", :yellow, "retry: attempt $attempt of $max$hint")
log_fallback(rt::Runtime) = log_line(rt, "↯ ", :yellow, "failed; running the fallback")
log_branch(rt::Runtime, choice::Bool) = log_line(rt, "? ", :blue, "branch condition is $choice")
log_workflow(rt::Runtime, wf::Workflow, p::WorkflowPlan) =
    log_line(rt, "⊕ ", :magenta, "workflow $(wf.name): $(p.jobs) step$(p.jobs == 1 ? "" : "s") for " *
                                 "$(length(p.paths)) target$(length(p.paths) == 1 ? "" : "s")")

#==============================================================================#
# Resources and the guard
#==============================================================================#

log_evicted(rt::Runtime, label::String, e::Evicted, next::Need, attempt::Int) =
    log_line(rt, "↺ ", :yellow, "$label: stopped to free memory ($(e.available_mb) MiB left, $(e.reserve_mb) MiB " *
        "must stay free); it used $(e.usage_mb) MiB and will run again when $(next.charge_mb) MiB are free " *
        "(attempt $attempt of $MAX_ATTEMPTS)")

log_clamped(rt::Runtime, label::String, n::Need, budget::Int) =
    log_line(rt, "⚠ ", :yellow, "$label declares mem_mb = $(n.declared_mb) but run(mem_mb = $budget) allows " *
        "$budget; it runs with mem_mb = $(n.limit_mb)")

log_paused(rt::Runtime, s::GuardSample) =
    log_line(rt, "⏸ ", :yellow, "waiting for memory: $(s.available_mb) MiB free, steps start again above " *
        "$(3s.reserve_mb) MiB")

log_thread_override(rt::Runtime, label::String, settings::Vector{String}, threads::Int) =
    log_line(rt, "⚠ ", :yellow, "$label: $(join(settings, ", ")) set by the command was replaced by " *
        "threads=$threads; declare threads instead")

log_function_limits(rt::Runtime) =
    log_line(rt, "ℹ ", :blue, "mem_mb and timeout of Julia function steps are not enforced (they run inside " *
        "this Julia process); use julia(\"…\") to limit Julia code, or a Timeout to stop waiting for it")

log_protection(rt::Runtime, ::NoGuard) = log_line(rt, "⚠ ", :yellow,
    "running without the resource guard: memory limits, timeouts and cleanup are not enforced")
log_protection(rt::Runtime, link::GuardLink) = measures(link) ? nothing : log_line(rt, "ℹ ", :blue,
    "memory is not measured on this system; only timeouts and cleanup are enforced")

log_guard_lost(rt::Runtime, pid::Int32, reason::AbstractString) =
    log_line(rt, "✗ ", :red, "the resource guard (pid $pid) exited unexpectedly: $reason; stopping the run " *
        "because steps must not run without memory protection")

"""A warning from the guard (memory taken by other programs), shown by one active run."""
function guard_warning(text::AbstractString)
    for rt in active_runs()
        rt.verbose || continue
        log_line(rt, "⚠ ", :yellow, text)
        return nothing
    end
end

#==============================================================================#
# Header and footer
#==============================================================================#

function log_header(rt::Runtime, name::String)
    with_log(rt) do
        printstyled(rt.planning ? "═══ Check: " : "═══ Pipeline: ", name, " ═══\n"; color=:blue, bold=true)
    end
end

function log_footer(rt::Runtime, r::RunResult)
    with_log(rt) do
        printstyled("═══ ", summary_text(r), " in $(seconds(r.duration)) ═══\n";
                    color=r.success ? :blue : :red, bold=true)
    end
end

function summary_text(r::RunResult)
    parts = r.planned ? ["$(count_status(r, :planned)) to run"] : ["$(count_status(r, :done)) done"]
    failed = count_status(r, :failed)
    push!(parts, "$(count_status(r, :skipped)) up to date",
          r.success && failed > 0 ? "$failed failed and recovered" : "$failed failed")
    join(parts, ", ")
end

@specialize
