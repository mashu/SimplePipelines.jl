# Display: one-line `show` methods, REPL views and `print_dag`.

@nospecialize   # see runtime.jl

styled(io::IO, text, color::Symbol; bold::Bool=false) = printstyled(io, text; color, bold)

#==============================================================================#
# One-line forms
#==============================================================================#

Base.show(io::IO, s::Shell) = print(io, "sh", repr(s.script))
Base.show(io::IO, s::Step) = print(io, "Step(", is_gensym(s.name) ? repr(work_label(s.work)) : repr(s.name), ")")
Base.show(io::IO, s::Sequence) = print(io, "Sequence(", join(s.nodes, " >> "), ")")
Base.show(io::IO, p::Parallel) = print(io, "Parallel(", join(p.nodes, " & "), ")")
Base.show(io::IO, r::Retry) = print(io, "Retry(", r.node, ", ", r.max_attempts, ")")
Base.show(io::IO, f::Fallback) = print(io, "Fallback(", f.primary, ", ", f.fallback, ")")
Base.show(io::IO, b::Branch) = print(io, "Branch(", b.if_true, ", ", b.if_false, ")")
Base.show(io::IO, t::Timeout) = print(io, "Timeout(", t.node, ", ", t.seconds, ")")
Base.show(io::IO, f::Force) = print(io, "Force(", f.node, ")")
Base.show(io::IO, r::Resourced) = print(io, "with_resources(", r.node, resources_text(r.resources), ")")
Base.show(io::IO, fe::ForEach) = print(io, foreach_label(fe))
Base.show(io::IO, j::Job) = print(io, "Job(", j.step, ")")
Base.show(io::IO, r::Rule) = print(io, "Rule(", repr(r.name), ")")
Base.show(io::IO, w::Workflow) = print(io, "Workflow(", repr(w.name), ", ", length(w.rules), " rules)")
Base.show(io::IO, p::Pipeline) = print(io, "Pipeline(", repr(p.name), ")")
Base.show(io::IO, w::Wildcards) =
    print(io, "Wildcards(", join(("$k=$(repr(v))" for (k, v) in sort!(collect(getfield(w, :values)))), ", "), ")")
Base.show(io::IO, e::StepFailure) = print(io, "StepFailure(", repr(e.kind), ", ", repr(e.message), ")")
Base.show(io::IO, r::StepResult) = print(io, "StepResult(", repr(r.label), ", ", repr(r.status), ")")

function resources_list(r::Resources)
    parts = String[]
    r.threads > 0 && push!(parts, "threads=$(r.threads)")
    r.mem_mb > 0 && push!(parts, "mem_mb=$(r.mem_mb)")
    r.timeout > 0 && push!(parts, "timeout=$(r.timeout)")
    join(parts, ", ")
end
resources_text(r::Resources) = (list = resources_list(r); isempty(list) ? "" : "; " * list)

#==============================================================================#
# REPL views
#==============================================================================#

function Base.show(io::IO, ::MIME"text/plain", s::Step)
    styled(io, "Step ", :blue; bold=true)
    println(io, step_label(s))
    step_details(io, s, "  ")
end

function step_details(io::IO, s::Step, prefix::AbstractString)
    isempty(s.inputs) || (print(io, prefix); styled(io, "← ", :green); println(io, join(s.inputs, ", ")))
    isempty(s.outputs) || (print(io, prefix); styled(io, "→ ", :yellow); println(io, join(s.outputs, ", ")))
    cmd = command_text(s.work)
    isempty(cmd) || (print(io, prefix); styled(io, "\$ " * shorten(replace(cmd, '\n' => "; "), 120), :light_black); println(io))
    list = resources_list(s.resources)
    isempty(list) || (print(io, prefix); styled(io, list, :light_black); println(io))
end

function Base.show(io::IO, ::MIME"text/plain", r::Rule)
    styled(io, "Rule ", :blue; bold=true)
    println(io, r.name)
    ins = [i.gather ? "gather($(i.pattern.text))" : i.pattern.text for i in r.inputs]
    isempty(ins) || (styled(io, "  ← ", :green); println(io, join(ins, ", ")))
    isempty(r.outputs) || (styled(io, "  → ", :yellow); println(io, join((o.text for o in r.outputs), ", ")))
    print(io, "  ")
    styled(io, rule_work_text(r.work), :light_black)
    println(io)
end
rule_work_text(t::String) = "\$ " * shorten(replace(t, '\n' => "; "), 120)
rule_work_text(j::JuliaScript) = "\$ julia " * shorten(j.template, 120)
rule_work_text(f::Function) = "julia: " * work_label(f)
rule_work_text(c::Base.AbstractCmd) = "\$ " * command_text(c)

function Base.show(io::IO, ::MIME"text/plain", w::Workflow)
    styled(io, "Workflow ", :blue; bold=true)
    println(io, w.name)
    for entry in w.rules
        print(io, "  ")
        println(io, entry_text(entry))
    end
    print(io, "  targets: ")
    println(io, isempty(w.targets) ? "automatic (outputs no other rule uses)" : join(w.targets, ", "))
end
entry_text(r::Rule) = string(r.name, ": ", join((i.gather ? "gather($(i.pattern.text))" : i.pattern.text for i in r.inputs), ", "),
                             " → ", join((o.text for o in r.outputs), ", "))
entry_text(s::Step) = string(step_label(s), ": ", join(s.inputs, ", "), " → ", join(s.outputs, ", "))

Base.show(io::IO, ::MIME"text/plain", node::AbstractNode) = print_dag(io, node; expand=false)
Base.show(io::IO, ::MIME"text/plain", p::Pipeline) = print_dag(io, p; expand=false)

function Base.show(io::IO, ::MIME"text/plain", r::StepResult)
    status_marker(io, r.status)
    print(io, r.label)
    styled(io, "  $(r.status), $(seconds(r.duration))", :light_black)
    println(io)
    step_details(io, r.step, "  ")
    result_details(io, r.result)
end
function result_details(io::IO, f::StepFailure)
    print(io, "  ")
    styled(io, "error: ", :red)
    println(io, f.message)
    isempty(f.detail) || println(io, indent(f.detail, "    "))
end
result_details(io::IO, ::Nothing) = nothing
result_details(io::IO, s::AbstractString) = isempty(s) || println(io, "  result: ", repr(shorten(s, 200)))
result_details(io::IO, x) = println(io, "  result: ", summary(x))

status_marker(io::IO, status::Symbol) = status_marker(io, Val(status))
status_marker(io::IO, ::Val{:done}) = styled(io, "✓ ", :green; bold=true)
status_marker(io::IO, ::Val{:skipped}) = styled(io, "· ", :light_black; bold=true)
status_marker(io::IO, ::Val{:planned}) = styled(io, "▶ ", :cyan; bold=true)
status_marker(io::IO, ::Val{:failed}) = styled(io, "✗ ", :red; bold=true)

const LISTED_RESULTS = 30

function Base.show(io::IO, ::MIME"text/plain", r::RunResult)
    styled(io, r.planned ? "Check " : "Run ", :blue; bold=true)
    print(io, r.name, ": ")
    styled(io, summary_text(r), r.success ? :green : :red; bold=true)
    println(io, " in ", seconds(r.duration))
    listed = filter(s -> s.status !== :skipped, r.steps)
    for s in Iterators.take(listed, LISTED_RESULTS)
        print(io, "  ")
        status_marker(io, s.status)
        print(io, s.label)
        note = split(result_note(s), '\n'; limit=2)
        styled(io, "  " * first(note), :light_black)
        println(io)
        length(note) == 2 && styled(io, indent(note[2], "      ") * "\n", :light_black)
    end
    length(listed) > LISTED_RESULTS && println(io, "  … $(length(listed) - LISTED_RESULTS) more")
end
result_note(s::StepResult) = result_note(Val(s.status), s)
result_note(::Val{:done}, s) = seconds(s.duration)
result_note(::Val{:planned}, s) = string(s.result)
result_note(::Val{:failed}, s) = s.result.message
result_note(::Val{:skipped}, s) = "up to date"

#==============================================================================#
# print_dag
#==============================================================================#

const LISTED_CHILDREN = 12

struct DagPrinter
    io::IO
    expand::Bool
    seen::IdDict{Any,Nothing}
end

"""
    print_dag([io,] node; expand=true)

Print the pipeline as a tree. With `expand=true`, [`ForEach`](@ref) nodes are
expanded and [`Workflow`](@ref)s resolved against the files on disk, so you see the
concrete steps that would run, with their paths and commands. A sub-DAG shared by
several branches is printed once.
"""
print_dag(node; expand::Bool=true) = print_dag(stdout, node; expand)
function print_dag(io::IO, node; expand::Bool=true)
    dag(DagPrinter(io, expand, IdDict{Any,Nothing}()), dag_root(node), "", "")
    nothing
end
dag_root(p::Pipeline) = p.root
dag_root(x) = as_node(x)

function dag_children(pr::DagPrinter, nodes::AbstractVector, cont::String)
    n = length(nodes)
    shown = n > LISTED_CHILDREN ? LISTED_CHILDREN - 2 : n
    for (i, node) in enumerate(Iterators.take(nodes, shown))
        last = i == n
        dag(pr, node, cont * (last ? "└─ " : "├─ "), cont * (last ? "   " : "│  "))
    end
    shown < n && (print(pr.io, cont, "└─ "); styled(pr.io, "… $(n - shown) more\n", :light_black))
end

function header(pr::DagPrinter, prefix, marker, color::Symbol, text)
    print(pr.io, prefix)
    styled(pr.io, marker, color; bold=true)
    println(pr.io, text)
end

# Returns true when `node` was printed before (and prints a reference instead).
function repeated(pr::DagPrinter, node, prefix, label)
    haskey(pr.seen, node) || (pr.seen[node] = nothing; return false)
    print(pr.io, prefix)
    styled(pr.io, "↑ $label (shown above)\n", :light_black)
    true
end

function dag(pr::DagPrinter, s::Step, prefix, cont)
    repeated(pr, s, prefix, step_label(s)) && return
    header(pr, prefix, isempty(s.inputs) ? "◆ " : "○ ", :cyan, step_label(s))
    step_details(pr.io, s, cont * "  ")
end

function dag(pr::DagPrinter, j::Job, prefix, cont)
    repeated(pr, j, prefix, step_label(j.step)) && return
    header(pr, prefix, isempty(j.step.inputs) ? "◆ " : "○ ", :cyan, step_label(j.step))
    step_details(pr.io, j.step, cont * (isempty(j.deps) ? "  " : "│ "))
    dag_children(pr, j.deps, cont)
end

function composite(pr::DagPrinter, node, prefix, cont, marker, color, text)
    repeated(pr, node, prefix, text) && return
    header(pr, prefix, marker, color, text)
    dag_children(pr, children(node), cont)
end

dag(pr::DagPrinter, n::Sequence, p, c) = composite(pr, n, p, c, "▸ ", :blue, "Sequence")
dag(pr::DagPrinter, n::Parallel, p, c) = composite(pr, n, p, c, "⊕ ", :magenta, "Parallel")
dag(pr::DagPrinter, n::Retry, p, c) = composite(pr, n, p, c, "↻ ", :yellow, "Retry ×$(n.max_attempts)" * (n.delay > 0 ? " (delay $(n.delay)s)" : ""))
dag(pr::DagPrinter, n::Fallback, p, c) = composite(pr, n, p, c, "↯ ", :yellow, "Fallback")
dag(pr::DagPrinter, n::Branch, p, c) = composite(pr, n, p, c, "? ", :blue, "Branch (true, false)")
dag(pr::DagPrinter, n::Timeout, p, c) = composite(pr, n, p, c, "⏱ ", :cyan, "Timeout $(n.seconds)s")
dag(pr::DagPrinter, n::Force, p, c) = composite(pr, n, p, c, "⚡ ", :yellow, "Force")
dag(pr::DagPrinter, n::Resourced, p, c) = composite(pr, n, p, c, "▣ ", :light_yellow, "with_resources" * resources_text(n.resources))

function dag(pr::DagPrinter, fe::ForEach, prefix, cont)
    header(pr, prefix, "⊕ ", :magenta, foreach_label(fe))
    pr.expand || return
    expanded = run_safely(() -> expand_foreach(fe))
    expanded.ok || return unexpandable(pr, cont, expanded.value.message)
    dag_children(pr, first(expanded.value), cont)
end

function dag(pr::DagPrinter, wf::Workflow, prefix, cont)
    header(pr, prefix, "⊕ ", :magenta, "Workflow $(wf.name)")
    pr.expand || return dag_rules(pr, wf, cont)
    planned = run_safely(() -> plan_workflow(wf))
    planned.ok || return unexpandable(pr, cont, planned.value.message)
    dag_plan(pr, planned.value, cont)
end
dag_plan(pr::DagPrinter, p::WorkflowPlan, cont) = dag_children(pr, children_of_plan(p.node), cont)
dag_plan(pr::DagPrinter, u::Unresolved, cont) = unexpandable(pr, cont, u.reason)
children_of_plan(p::Parallel) = p.nodes
children_of_plan(node::AbstractNode) = AbstractNode[node]

function dag_rules(pr::DagPrinter, wf::Workflow, cont)
    for entry in wf.rules
        print(pr.io, cont, "   ")
        styled(pr.io, entry_text(entry) * "\n", :light_black)
    end
end

unexpandable(pr::DagPrinter, cont, message) =
    styled(pr.io, indent("cannot expand yet: " * message, cont * "   ") * "\n", :light_black)

@specialize
