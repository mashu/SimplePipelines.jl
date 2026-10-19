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

function log_start(rt::Runtime, step::Step, label::String)
    with_log(rt) do
        printstyled("▶ "; color=:cyan, bold=true)
        println(label)
        cmd = command_text(step.work)
        isempty(cmd) || printstyled(indent(shorten(cmd, 400), "    \$ "), "\n"; color=:light_black)
    end
end

command_text(s::Shell) = s.script
command_text(c::Cmd) = join(c.exec, " ")
command_text(c::Base.AbstractCmd) = string(c)
command_text(_) = ""

log_result(rt::Runtime, r::StepResult) = log_status(rt, Val(r.status), r)
log_status(rt::Runtime, ::Val{:done}, r) = log_line(rt, "✓ ", :green, "$(r.label) ($(seconds(r.duration)))")
log_status(rt::Runtime, ::Val{:skipped}, r) = nothing
log_status(rt::Runtime, ::Val{:planned}, r) = log_line(rt, "▶ ", :cyan, "$(r.label)  ($(r.result))")
log_status(rt::Runtime, ::Val{:failed}, r) =
    log_line(rt, "✗ ", :red, "$(r.label): $(r.result.message)"; detail=tail_lines(r.result.detail, 15))

tail_lines(text::AbstractString, n::Int) = join(last(split(rstrip(text), '\n'), n), "\n")
seconds(t::Real) = t < 10 ? "$(round(t; digits=2))s" : "$(round(Int, t))s"

log_retry(rt::Runtime, attempt, max) = log_line(rt, "↻ ", :yellow, "retry: attempt $attempt of $max")
log_fallback(rt::Runtime) = log_line(rt, "↯ ", :yellow, "failed; running the fallback")
log_branch(rt::Runtime, choice::Bool) = log_line(rt, "? ", :blue, "branch condition is $choice")
log_workflow(rt::Runtime, wf::Workflow, p::WorkflowPlan) =
    log_line(rt, "⊕ ", :magenta, "workflow $(wf.name): $(p.jobs) step$(p.jobs == 1 ? "" : "s") for " *
                                 "$(length(p.paths)) target$(length(p.paths) == 1 ? "" : "s")")

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
