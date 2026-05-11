# print_dag, print_children, Base.show for StepResult, nodes, Pipeline, MIME. Requires Types, RunNodes.
#
# Color vs no-color is handled by dispatch on a Styler trait (`Color`/`Plain`)
# rather than threading a `color::Bool` through every method and branching at
# every printstyled call site.

abstract type Styler end
struct Color <: Styler end
struct Plain <: Styler end
styler(color::Bool) = color ? Color() : Plain()

# Styled print primitives — dispatch instead of the repeated `color ? printstyled : print` ternary.
emit(::Plain, io::IO, s; kwargs...) = print(io, s)
emit(::Color, io::IO, s; kwargs...) = printstyled(io, s; kwargs...)
emitln(s::Styler, io::IO, x; kwargs...) = (emit(s, io, x; kwargs...); println(io))

"""
    print_dag(node [; color=true])
    print_dag(io, node [, indent])

Print a tree visualization of the pipeline DAG. With `color=true` (default when writing to a terminal), uses colors for node types and status.
Steps with no inputs (start nodes) are shown with ◆ in light cyan; steps with inputs use ○ in cyan. See also [`run`](@ref) and `display(pipeline)`.
"""
print_dag(node::AbstractNode; color::Bool=true) = print_dag(stdout, node, "", "", styler(color))
print_dag(io::IO, node::AbstractNode, ::Int=0) = print_dag(io, node, "", "", Plain())

# pre = prefix for first line, cont = continuation prefix for subsequent lines
function print_dag(io::IO, s::Step, pre::String, cont::String, st::Styler)
    print(io, pre)
    is_start = isempty(s.inputs)
    emit(st, io, is_start ? "◆ " : "○ "; color=is_start ? :light_cyan : :cyan)
    println(io, step_label(s))
    isempty(s.inputs) || (print(io, cont, "    "); emit(st, io, "← "; color=:green); println(io, join(s.inputs, ", ")))
    isempty(s.outputs) || (print(io, cont, "    "); emit(st, io, "→ "; color=:yellow); println(io, join(s.outputs, ", ")))
end

function print_dag(io::IO, s::Sequence, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "▸ Sequence"; color=:blue)
    print_children(io, s.nodes, cont, st)
end

function print_dag(io::IO, p::Parallel, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "⊕ Parallel"; color=:magenta)
    print_children(io, p.nodes, cont, st)
end

function print_dag(io::IO, r::Retry, pre::String, cont::String, st::Styler)
    print(io, pre); emit(st, io, "↻ Retry"; color=:yellow)
    println(io, " ×$(r.max_attempts)", r.delay > 0 ? " ($(r.delay)s delay)" : "")
    print_dag(io, r.node, cont * "    ", cont * "    ", st)
end

function print_dag(io::IO, f::Fallback, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "↯ Fallback"; color=:yellow)
    print_dag(io, f.primary,  cont * "  ├─", cont * "  │ ", st)
    print_dag(io, f.fallback, cont * "  └─", cont * "    ", st)
end

function print_dag(io::IO, b::Branch, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "? Branch"; color=:blue)
    print_branch_arm(io, b.if_true,  cont, "  ├─", "  │ ", st, :green, "✓ ")
    print_branch_arm(io, b.if_false, cont, "  └─", "    ", st, :red,   "✗ ")
end

function print_dag(io::IO, t::Timeout, pre::String, cont::String, st::Styler)
    print(io, pre); emit(st, io, "⏱ Timeout"; color=:cyan)
    println(io, " $(t.seconds)s")
    print_dag(io, t.node, cont * "    ", cont * "    ", st)
end

function print_dag(io::IO, r::Reduce, pre::String, cont::String, st::Styler)
    print(io, pre); emit(st, io, "⊛ Reduce"; color=:magenta)
    println(io, " :$(r.name)")
    print_dag(io, r.node, cont * "    ", cont * "    ", st)
end

function print_dag(io::IO, f::Force, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "⚡ Force"; color=:yellow, bold=true)
    print_dag(io, f.node, cont * "    ", cont * "    ", st)
end

function print_dag(io::IO, fe::ForEach{F, String}, pre::String, cont::String, st::Styler) where F
    print(io, pre); emit(st, io, "⊕ ForEach"; color=:magenta)
    println(io, " \"", fe.source, "\"")
end

function print_dag(io::IO, fe::ForEach{F, Vector{T}}, pre::String, cont::String, st::Styler) where {F, T}
    print(io, pre); emit(st, io, "⊕ ForEach"; color=:magenta)
    println(io, " ($(length(fe.source)) items)")
end

function print_dag(io::IO, p::Pipe, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "▸ Pipe (output → input)"; color=:cyan)
    print_dag(io, p.first,  cont * "  ├─", cont * "  │ ", st)
    print_dag(io, p.second, cont * "  └─", cont * "    ", st)
end

function print_dag(io::IO, sip::SameInputPipe, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "▸ SameInputPipe (same input)"; color=:cyan)
    print_dag(io, sip.first,  cont * "  ├─", cont * "  │ ", st)
    print_dag(io, sip.second, cont * "  └─", cont * "    ", st)
end

function print_dag(io::IO, bp::BroadcastPipe, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "▸ BroadcastPipe (.>>  each branch → second)"; color=:cyan)
    print_dag(io, bp.first,  cont * "  ├─", cont * "  │ ", st)
    print_dag(io, bp.second, cont * "  └─", cont * "    ", st)
end

function print_dag(io::IO, r::Resourced, pre::String, cont::String, st::Styler)
    print(io, pre)
    emitln(st, io, "▣ Resources(mem=$(r.resources.mem_mb)MB, threads=$(r.resources.threads))";
           color=:light_yellow)
    print_dag(io, r.node, cont * "    ", cont * "    ", st)
end

function print_dag(io::IO, ::NoWork, pre::String, cont::String, st::Styler)
    print(io, pre); emitln(st, io, "∅ NoWork"; color=:light_black)
end

function print_branch_arm(io::IO, node::AbstractNode, cont::String, branch::String, gutter::String,
                          st::Styler, marker_color::Symbol, marker::String)
    print(io, cont, branch)
    emit(st, io, marker; color=marker_color)
    print_dag(io, node, "", cont * gutter * "  ", st)
end

function print_children(io::IO, nodes, cont::String, st::Styler)
    n = length(nodes)
    for (i, node) in enumerate(nodes)
        last = i == n
        pre  = cont * (last ? "  └─" : "  ├─")
        next = cont * (last ? "    " : "  │ ")
        print_dag(io, node, pre, next, st)
    end
end

# One-line show: named fields, empty inputs/outputs omitted, optional ", result=…" tail.
# `result_str = ""` means "skip the result field" — chosen over a Union to keep dispatch flat.
function show_stepresult_oneline(io::IO, r::StepResult, dur::Float64, result_str::AbstractString="")
    st = styler(get(io, :color, false)::Bool)
    print(io, "StepResult(step=")
    show_step_inline(io, r.step, st)
    print(io, ", success="); emit(st, io, string(r.success); color=r.success ? :green : :red)
    print(io, ", duration="); emit(st, io, string(round(dur; digits=2)); color=:light_black)
    isempty(r.inputs)  || print(io, ", inputs=",  summary(r.inputs))
    isempty(r.outputs) || print(io, ", outputs=", summary(r.outputs))
    isempty(result_str) || (print(io, ", result="); emit(st, io, result_str; color=:light_black))
    print(io, ")")
end

show_step_inline(io::IO, s::Step, ::Color) = printstyled(io, "Step(:", step_label(s), ")", color=:cyan)
show_step_inline(io::IO, s::Step, ::Plain) = show(io, s)

function Base.show(io::IO, r::StepResult{S, I, O, String}) where {S, I, O}
    s = r.result
    show_stepresult_oneline(io, r, r.duration,
                            length(s) > 200 ? repr(first(s, 200) * "…") : repr(s))
end
Base.show(io::IO, r::StepResult{S, I, O, V}) where {S, I, O, V} =
    show_stepresult_oneline(io, r, r.duration, r.result === nothing ? "" : repr(r.result))

# Multi-line show for REPL: show only sections that have content (dispatch by presence of inputs/result).
# Steps with no input files (start nodes): omit input line. No "(none)" or "Nothing"; cleaner and consistent.
function Base.show(io::IO, ::MIME"text/plain", r::StepResult)
    st = styler(get(io, :color, false)::Bool)
    emit(st, io, r.success ? "✓ " : "✗ "; color = r.success ? :green : :red)
    print(io, "StepResult: ")
    emit(st, io, step_label(r.step); color=:cyan)
    print(io, " (")
    emit(st, io, string(round(r.duration; digits=2)); color=:light_black)
    println(io, "s)")
    if !isempty(r.inputs)
        print(io, "  input files:  ")
        emit(st, io, "← "; color=:green)
        println(io, join(r.inputs, "\n               "))
    end
    if !isempty(r.outputs)
        print(io, "  output files: ")
        emit(st, io, "→ "; color=:yellow)
        println(io, join(r.outputs, "\n               "))
    end
    if r.result !== nothing
        print(io, "  result:       ", summary(r.result))
        show_result_inline(io, r.result)
    end
end

# Inline preview of a step's result value: short string (truncated, newlines escaped),
# or just a newline for arbitrary types (the `summary` already named the type).
function show_result_inline(io::IO, s::String)
    isempty(s) && return println(io)
    trunc = length(s) > 80 ? first(s, 80) * "…" : s
    println(io, " \"", replace(trunc, '\n' => "\\n"), "\"")
end
show_result_inline(io::IO, e::StepFailure) = show_result_inline(io, string(e))
show_result_inline(io::IO, _) = println(io)

function print_wildcards(io::IO, wildcards::AbstractDict)
    pairs = sort!(collect(wildcards); by=first)
    isempty(pairs) && return println(io, "(none)")
    println(io, join(("$(k) => $(v)" for (k, v) in pairs), ", "))
end

function Base.show(io::IO, ::MIME"text/plain", c::RuleCheck)
    println(io, "Rule check: ", c.rule.name)
    println(io, "  input patterns:  ", isempty(c.rule.inputs) ? "(none)" : join(c.rule.inputs, ", "))
    println(io, "  output patterns: ", isempty(c.rule.outputs) ? "(none)" : join(c.rule.outputs, ", "))
    println(io, "  wildcards:       ", isempty(c.wildcards) ? "(none)" : join(c.wildcards, ", "))
    println(io, "  placeholders:    ", isempty(c.placeholders) ? "(none)" : join(c.placeholders, ", "))
end

function Base.show(io::IO, ::MIME"text/plain", c::RuleInstantiationCheck)
    println(io, "Rule target check: ", c.rule.name)
    println(io, "  target:    ", c.target)
    print(io, "  wildcards: ")
    print_wildcards(io, c.wildcards)
    println(io, "  inputs:    ", isempty(c.inputs) ? "(none)" : join(c.inputs, ", "))
    println(io, "  outputs:   ", isempty(c.outputs) ? "(none)" : join(c.outputs, ", "))
    isempty(c.command) || println(io, "  command:   ", c.command)
    isempty(c.function_note) || println(io, "  note:      ", c.function_note)
end

function Base.show(io::IO, ::MIME"text/plain", p::PlanExplanation)
    println(io, "Workflow explanation for target: ", p.target)
    for (i, step) in enumerate(p.steps)
        i > 1 && println(io)
        print_explanation_step(io, step)
    end
end

function print_explanation_step(io::IO, step::RuleExplanationStep)
    println(io, "Rule: ", step.rule.name)
    println(io, "  target:       ", step.target)
    print(io, "  wildcards:    ")
    print_wildcards(io, step.wildcards)
    println(io, "  inputs:       ", isempty(step.inputs) ? "(none)" : join(step.inputs, ", "))
    println(io, "  outputs:      ", isempty(step.outputs) ? "(none)" : join(step.outputs, ", "))
    isempty(step.command) || println(io, "  command:      ", step.command)
    isempty(step.function_note) || println(io, "  note:         ", step.function_note)
    println(io, "  dependencies: ", isempty(step.dependencies) ? "(none)" : join(step.dependencies, ", "))
end

Base.show(io::IO, s::Step) = print(io, "Step(:", s.name, ")")
Base.show(io::IO, s::Sequence) = print(io, "Sequence(", join(s.nodes, " >> "), ")")
Base.show(io::IO, p::Parallel) = print(io, "Parallel(", join(p.nodes, " & "), ")")
Base.show(io::IO, r::Retry) = print(io, "Retry(", r.node, ", ", r.max_attempts, ")")
Base.show(io::IO, f::Fallback) = print(io, "(", f.primary, " | ", f.fallback, ")")
Base.show(io::IO, b::Branch) = print(io, "Branch(?, ", b.if_true, ", ", b.if_false, ")")
Base.show(io::IO, t::Timeout) = print(io, "Timeout(", t.node, ", ", t.seconds, "s)")
Base.show(io::IO, r::Reduce) = print(io, "Reduce(:", r.name, ", ", r.node, ")")
Base.show(io::IO, f::Force) = print(io, "Force(", f.node, ")")
Base.show(io::IO, fe::ForEach{F, String}) where F = print(io, "ForEach(\"", fe.source, "\")")
Base.show(io::IO, fe::ForEach{F, Vector{T}}) where {F, T} = print(io, "ForEach(", length(fe.source), " items)")
Base.show(io::IO, p::Pipe) = print(io, "(", p.first, " |> ", p.second, ")")
Base.show(io::IO, sip::SameInputPipe) = print(io, "(", sip.first, " >>> ", sip.second, ")")
Base.show(io::IO, bp::BroadcastPipe) = print(io, "(", bp.first, " .>> ", bp.second, ")")
Base.show(io::IO, r::Resourced) = print(io, "Resourced(", r.node, ", mem=", r.resources.mem_mb, "MB)")
Base.show(io::IO, ::NoWork) = print(io, "NoWork()")
Base.show(io::IO, p::Pipeline) = print(io, "Pipeline(\"", p.name, "\", ", count_steps(p.root), " steps)")

function Base.show(io::IO, ::MIME"text/plain", p::Pipeline)
    st = styler(get(io, :color, false)::Bool)
    emit(st, io, "Pipeline: "; color=:blue, bold=true)
    emit(st, io, p.name; color=:white, bold=true)
    emitln(st, io, " ($(count_steps(p.root)) steps)"; color=:light_black)
    print_dag(io, p.root, "", "", st)
end

function Base.show(io::IO, ::MIME"text/plain", node::AbstractNode)
    print_dag(io, node, "", "", styler(get(io, :color, false)::Bool))
end
