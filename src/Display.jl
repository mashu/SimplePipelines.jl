# print_dag, print_children, Base.show for StepResult, nodes, Pipeline, MIME. Requires Types, RunNodes.

"""
    print_dag(node [; color=true])
    print_dag(io, node [, indent])

Print a tree visualization of the pipeline DAG. With `color=true` (default when writing to a terminal), uses colors for node types and status.
Steps with no inputs (start nodes) are shown with ◆ in light cyan; steps with inputs use ○ in cyan. See also [`run`](@ref) and `display(pipeline)`.
"""
print_dag(node::AbstractNode; color::Bool=true) = print_dag(stdout, node, "", "", color)
print_dag(io::IO, node::AbstractNode, ::Int=0) = print_dag(io, node, "", "", false)

# pre = prefix for first line, cont = continuation prefix for subsequent lines
function print_dag(io::IO, s::Step, pre::String, cont::String, color::Bool)
    print(io, pre)
    is_start = isempty(s.inputs)
    if color
        if is_start
            printstyled(io, "◆ ", color=:light_cyan)  # start node (no inputs)
        else
            printstyled(io, "○ ", color=:cyan)
        end
    else
        print(io, is_start ? "◆ " : "○ ")
    end
    println(io, step_label(s))
    if !isempty(s.inputs)
        print(io, cont, "    ")
        color ? printstyled(io, "← ", color=:green) : print(io, "← ")
        println(io, join(s.inputs, ", "))
    end
    if !isempty(s.outputs)
        print(io, cont, "    ")
        color ? printstyled(io, "→ ", color=:yellow) : print(io, "→ ")
        println(io, join(s.outputs, ", "))
    end
end

function print_dag(io::IO, s::Sequence, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "▸ Sequence\n", color=:blue) : println(io, "▸ Sequence")
    print_children(io, s.nodes, cont, color)
end

function print_dag(io::IO, p::Parallel, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⊕ Parallel\n", color=:magenta) : println(io, "⊕ Parallel")
    print_children(io, p.nodes, cont, color)
end

function print_dag(io::IO, r::Retry, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "↻ Retry", color=:yellow) : print(io, "↻ Retry")
    println(io, " ×$(r.max_attempts)", r.delay > 0 ? " ($(r.delay)s delay)" : "")
    print_dag(io, r.node, cont * "    ", cont * "    ", color)
end

function print_dag(io::IO, f::Fallback, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "↯ Fallback\n", color=:yellow) : println(io, "↯ Fallback")
    print_dag(io, f.primary,  cont * "  ├─", cont * "  │ ", color)
    print_dag(io, f.fallback, cont * "  └─", cont * "    ", color)
end

function print_dag(io::IO, b::Branch, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "? Branch\n", color=:blue) : println(io, "? Branch")
    if color
        print_dag(io, b.if_true,  cont * "  ├─", cont * "  │ ", color, :green, "✓ ")
        print_dag(io, b.if_false, cont * "  └─", cont * "    ", color, :red, "✗ ")
    else
        print_dag(io, b.if_true,  cont * "  ├─✓ ", cont * "  │   ", false)
        print_dag(io, b.if_false, cont * "  └─✗ ", cont * "      ", false)
    end
end

function print_dag(io::IO, t::Timeout, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⏱ Timeout", color=:cyan) : print(io, "⏱ Timeout")
    println(io, " $(t.seconds)s")
    print_dag(io, t.node, cont * "    ", cont * "    ", color)
end

function print_dag(io::IO, r::Reduce, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⊛ Reduce", color=:magenta) : print(io, "⊛ Reduce")
    println(io, " :$(r.name)")
    print_dag(io, r.node, cont * "    ", cont * "    ", color)
end

function print_dag(io::IO, f::Force, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⚡ Force\n", color=:yellow, bold=true) : println(io, "⚡ Force")
    print_dag(io, f.node, cont * "    ", cont * "    ", color)
end

function print_dag(io::IO, fe::ForEach{F, String}, pre::String, cont::String, color::Bool) where F
    print(io, pre)
    color ? printstyled(io, "⊕ ForEach", color=:magenta) : print(io, "⊕ ForEach")
    println(io, " \"", fe.source, "\"")
end

function print_dag(io::IO, fe::ForEach{F, Vector{T}}, pre::String, cont::String, color::Bool) where {F, T}
    print(io, pre)
    color ? printstyled(io, "⊕ ForEach", color=:magenta) : print(io, "⊕ ForEach")
    println(io, " ($(length(fe.source)) items)")
end

function print_dag(io::IO, p::Pipe, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "▸ Pipe (output → input)\n", color=:cyan) : println(io, "▸ Pipe (output → input)")
    print_dag(io, p.first,  cont * "  ├─", cont * "  │ ", color)
    print_dag(io, p.second, cont * "  └─", cont * "    ", color)
end

function print_dag(io::IO, sip::SameInputPipe, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "▸ SameInputPipe (same input)\n", color=:cyan) : println(io, "▸ SameInputPipe (same input)")
    print_dag(io, sip.first,  cont * "  ├─", cont * "  │ ", color)
    print_dag(io, sip.second, cont * "  └─", cont * "    ", color)
end

function print_dag(io::IO, bp::BroadcastPipe, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "▸ BroadcastPipe (.>>  each branch → second)\n", color=:cyan) : println(io, "▸ BroadcastPipe (.>>  each branch → second)")
    print_dag(io, bp.first,  cont * "  ├─", cont * "  │ ", color)
    print_dag(io, bp.second, cont * "  └─", cont * "    ", color)
end

# Branch helper with marker
function print_dag(io::IO, node::AbstractNode, pre::String, cont::String, color::Bool, marker_color::Symbol, marker::String)
    printstyled(io, marker, color=marker_color)
    print_dag(io, node, pre, cont * "  ", color)
end

function print_children(io::IO, nodes, cont::String, color::Bool)
    n = length(nodes)
    for (i, node) in enumerate(nodes)
        last = i == n
        pre  = cont * (last ? "  └─" : "  ├─")
        next = cont * (last ? "    " : "  │ ")
        print_dag(io, node, pre, next, color)
    end
end

# One-line show (e.g. in vectors): named fields, omit empty; use colors when io has :color for readability
function _show_stepresult_oneline(io::IO, r::StepResult, dur::Float64; result_str::Union{String,Nothing}=nothing)
    color = get(io, :color, false)::Bool
    print(io, "StepResult(step=")
    if color
        printstyled(io, "Step(:", step_label(r.step), ")", color=:cyan)
    else
        show(io, r.step)
    end
    print(io, ", success=")
    if color
        printstyled(io, string(r.success), color=r.success ? :green : :red)
    else
        print(io, r.success)
    end
    print(io, ", duration=")
    if color
        printstyled(io, string(round(dur; digits=2)), color=:light_black)
    else
        print(io, round(dur; digits=2))
    end
    !isempty(r.inputs) && print(io, ", inputs=", summary(r.inputs))
    !isempty(r.outputs) && print(io, ", outputs=", summary(r.outputs))
    if result_str !== nothing
        print(io, ", result=")
        color ? printstyled(io, result_str, color=:light_black) : print(io, result_str)
    end
    print(io, ")")
end
function Base.show(io::IO, r::StepResult{S, I, O, String}) where {S, I, O}
    s = r.result
    result_str = length(s) > 200 ? repr(first(s, 200) * "…") : repr(s)
    _show_stepresult_oneline(io, r, r.duration; result_str)
end
function Base.show(io::IO, r::StepResult{S, I, O, V}) where {S, I, O, V}
    result_str = r.result === nothing ? nothing : repr(r.result)
    _show_stepresult_oneline(io, r, r.duration; result_str)
end

# Multi-line show for REPL: show only sections that have content (dispatch by presence of inputs/result).
# Steps with no input files (start nodes): omit input line. No "(none)" or "Nothing"; cleaner and consistent.
function Base.show(io::IO, ::MIME"text/plain", r::StepResult)
    color = get(io, :color, false)::Bool
    if color
        printstyled(io, r.success ? "✓ " : "✗ ", color = r.success ? :green : :red)
        print(io, "StepResult: ")
        printstyled(io, step_label(r.step), color=:cyan)
        print(io, " (")
        printstyled(io, string(round(r.duration; digits=2)), color=:light_black)
        println(io, "s)")
    else
        print(io, r.success ? "✓ " : "✗ ")
        println(io, "StepResult: ", step_label(r.step), " (", round(r.duration; digits=2), "s)")
    end
    if !isempty(r.inputs)
        print(io, "  input files:  ")
        color && printstyled(io, "← ", color=:green)
        println(io, join(r.inputs, "\n               "))
    end
    if !isempty(r.outputs)
        print(io, "  output files: ")
        color && printstyled(io, "→ ", color=:yellow)
        println(io, join(r.outputs, "\n               "))
    end
    if r.result !== nothing
        print(io, "  result:       ", summary(r.result))
        s = r.result
        if s isa String && !isempty(s)
            trunc = length(s) > 80 ? first(s, 80) * "…" : s
            println(io, " \"", replace(trunc, '\n' => "\\n"), "\"")
        else
            println(io)
        end
    end
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
Base.show(io::IO, p::Pipeline) = print(io, "Pipeline(\"", p.name, "\", ", count_steps(p.root), " steps)")

function Base.show(io::IO, ::MIME"text/plain", p::Pipeline)
    color = get(io, :color, false)::Bool
    if color
        printstyled(io, "Pipeline: ", color=:blue, bold=true)
        printstyled(io, p.name, color=:white, bold=true)
        printstyled(io, " ($(count_steps(p.root)) steps)\n", color=:light_black)
    else
        println(io, "Pipeline: ", p.name, " (", count_steps(p.root), " steps)")
    end
    print_dag(io, p.root, "", "", color)
end

function Base.show(io::IO, ::MIME"text/plain", node::AbstractNode)
    color = get(io, :color, false)::Bool
    print_dag(io, node, "", "", color)
end
