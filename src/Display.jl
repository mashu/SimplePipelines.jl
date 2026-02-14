# print_dag, print_children, Base.show for StepResult, nodes, Pipeline, MIME. Requires Types, RunNodes.

"""
    print_dag(node [; color=true])
    print_dag(io, node [, indent])

Print a tree visualization of the pipeline DAG. With `color=true` (default when writing to a terminal), uses colors for node types and status. See also [`run`](@ref) and `display(pipeline)`.
"""
print_dag(node::AbstractNode; color::Bool=true) = print_dag(stdout, node, "", "", color)
print_dag(io::IO, node::AbstractNode, ::Int=0) = print_dag(io, node, "", "", false)

# pre = prefix for first line, cont = continuation prefix for subsequent lines
function print_dag(io::IO, s::Step, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "○ ", color=:cyan) : print(io, "○ ")
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

function print_dag(io::IO, fe::ForEach, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⊕ ForEach", color=:magenta) : print(io, "⊕ ForEach")
    println(io, " \"", fe.pattern, "\"")
end

function print_dag(io::IO, m::Map, pre::String, cont::String, color::Bool)
    print(io, pre)
    color ? printstyled(io, "⊕ Map", color=:magenta) : print(io, "⊕ Map")
    println(io, " ($(length(m.items)) items)")
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

# Custom show so StepResult doesn't print the ugly closure type (dispatch on output type)
function Base.show(io::IO, r::StepResult{S, I, String}) where {S, I}
    print(io, "StepResult(")
    show(io, r.step)
    print(io, ", ", r.success, ", ", round(r.duration; digits=2), ", ")
    s = r.output
    show(io, length(s) > 200 ? first(s, 200) * "…" : s)
    print(io, ")")
end
function Base.show(io::IO, r::StepResult{S, I, V}) where {S, I, V}
    print(io, "StepResult(")
    show(io, r.step)
    print(io, ", ", r.success, ", ", round(r.duration; digits=2), ", ")
    print(io, "<", summary(r.output), ">")
    print(io, ")")
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
Base.show(io::IO, fe::ForEach) = print(io, "ForEach(\"", fe.pattern, "\")")
Base.show(io::IO, m::Map) = print(io, "Map(", length(m.items), " items)")
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
