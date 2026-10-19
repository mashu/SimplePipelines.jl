# Workflow resolution: rules + targets → a DAG of Jobs.
#
# Concrete targets resolve backward through the rules. When several rules match,
# the most specific one (fewest characters captured by wildcards) whose inputs can
# be satisfied wins. Like Make, an existing file that no applicable rule can rebuild
# is used as it is. Pattern targets ("bam/{s}.bam") and gather inputs get their
# wildcard values by tracing the rules back to source files on disk.

@nospecialize   # see runtime.jl

"""A rule instance: `step` runs after the jobs that produce its inputs."""
struct Job <: AbstractNode
    step::Step
    deps::Vector{AbstractNode}
end
children(j::Job) = j.deps
function collect_steps!(found, j::Job, seen)
    haskey(seen, j) && return found
    seen[j] = nothing
    foreach(d -> collect_steps!(found, d, seen), j.deps)
    collect_steps!(found, j.step, seen)
end

struct Leaf end                       # an existing file with nothing to run
struct Unresolved
    reason::String
end

const Assignments = Vector{Dict{String,String}}

struct Resolver
    rules::Vector{Union{Rule,Step}}
    filter::Union{Nothing,Function}
    pending::Set{String}              # files earlier steps will create (during `check`)
    nodes::Dict{String,Union{Job,Leaf,Unresolved}}
    active::Set{String}               # concrete paths being resolved (cycle check)
    traced::Dict{String,Assignments}  # pattern → discovered assignments
    tracing::Set{String}
    sources::Dict{String,Int}         # source patterns globbed → number of matches
end
Resolver(rules, filter, pending=Set{String}()) = Resolver(rules, filter, pending, Dict{String,Union{Job,Leaf,Unresolved}}(),
                                   Set{String}(), Dict{String,Assignments}(), Set{String}(), Dict{String,Int}())

#==============================================================================#
# Concrete paths
#==============================================================================#

struct Candidate{E}
    entry::E
    wildcards::Dict{String,String}
    specificity::Int                  # characters captured by wildcards; fewer is more specific
end

function candidates(R::Resolver, path::String)
    found = Candidate[]
    foreach(entry -> add_candidate!(found, entry, path), R.rules)
    sort!(found; by=c -> c.specificity, alg=MergeSort)
end

add_candidate!(found, s::Step, path) =
    path in s.outputs ? push!(found, Candidate(s, Dict{String,String}(), 0)) : found
function add_candidate!(found, r::Rule, path)
    for out in r.outputs
        wildcards = match_path(out, path)
        wildcards === nothing && continue
        return push!(found, Candidate(r, wildcards, sum(length, values(wildcards); init=0)))
    end
    found
end

"""Resolve one concrete path into a Job, a Leaf (existing file) or Unresolved."""
function resolve!(R::Resolver, path::String)
    haskey(R.nodes, path) && return R.nodes[path]
    path in R.active && return Unresolved("dependency cycle through `$path`")
    push!(R.active, path)
    result = resolve_path(R, path, candidates(R, path))
    delete!(R.active, path)
    get!(R.nodes, path, result)
end

function resolve_path(R::Resolver, path::String, cands)
    isempty(cands) && return available(R, path) ? Leaf() : Unresolved("`$path` does not exist and no rule produces it")
    reasons = String[]
    for c in cands
        job = accept(instantiate!(R, c), reasons)
        job === nothing || return job
    end
    available(R, path) ? Leaf() : Unresolved(join(unique!(reasons), "\n"))
end
available(R::Resolver, path::String) = ispath(path) || path in R.pending
accept(j::Job, _) = j
accept(u::Unresolved, reasons) = (push!(reasons, u.reason); nothing)

register!(R::Resolver, job::Job) = (foreach(o -> R.nodes[o] = job, job.step.outputs); job)

add_dep!(deps, ::Leaf, _, _) = nothing
add_dep!(deps, j::Job, _, _) = (push_unique!(deps, j); nothing)
add_dep!(deps, u::Unresolved, who, path) = Unresolved("$who needs `$path`\n$(u.reason)")

function instantiate!(R::Resolver, c::Candidate{<:Step})
    s = c.entry
    deps = AbstractNode[]
    for path in s.inputs
        u = add_dep!(deps, resolve!(R, path), "step `$(step_label(s))`", path)
        u === nothing || return u
    end
    register!(R, Job(s, deps))
end

"""Inputs of a rule instance under construction: path groups, call arguments, dependencies."""
struct Draft
    groups::Vector{Vector{String}}
    args::Vector{Union{String,Vector{String}}}
    deps::Vector{AbstractNode}
end

function instantiate!(R::Resolver, c::Candidate{<:Rule})
    rule, wildcards = c.entry, c.wildcards
    draft = Draft(Vector{String}[], Union{String,Vector{String}}[], AbstractNode[])
    who = "rule `$(rule.name)$(wildcard_suffix(rule, wildcards))`"
    for inp in rule.inputs
        u = add_input!(draft, R, inp, input_paths!(R, inp, wildcards), who)
        u === nothing || return u
    end
    register!(R, Job(rule_step(rule, wildcards, draft), draft.deps))
end

wildcard_suffix(rule::Rule, wildcards) = isempty(wildcards) ? "" :
    string("[", join(("$k=$(wildcards[k])" for k in rule_wildcards(rule) if haskey(wildcards, k)), ","), "]")

add_input!(draft::Draft, R, inp, u::Unresolved, who) = Unresolved("$who\n$(u.reason)")
function add_input!(draft::Draft, R, inp, paths::Vector{String}, who)
    for path in paths
        u = add_dep!(draft.deps, resolve!(R, path), who, path)
        u === nothing || return u
    end
    push!(draft.groups, paths)
    push!(draft.args, inp.gather ? paths : only(paths))
    nothing
end

input_paths!(R::Resolver, inp::RuleInput, wildcards) =
    inp.gather ? gather_paths!(R, inp.pattern, wildcards) : [substitute(inp.pattern, wildcards)]

function gather_paths!(R::Resolver, p::Pattern, wildcards)
    bound = Pattern(bind_known(p.text, wildcards))
    has_wildcards(bound) || return [bound.text]
    found = keep(R.filter, trace!(R, bound))
    isempty(found) && return Unresolved(no_matches_message(R, "gather(\"$(bound.text)\")"))
    sort!(unique!([substitute(bound, a) for a in found]))
end

function rule_step(rule::Rule, wildcards, draft::Draft)
    outputs = [substitute(p, wildcards) for p in rule.outputs]
    inputs = reduce(vcat, draft.groups; init=String[])
    res = rule.resources
    ctx = TemplateContext(draft.groups, outputs, wildcards)
    labels = [k => wildcards[k] for k in rule_wildcards(rule)]
    recipe = fingerprint((recipe_source(rule.work), (string(k, "=", v) for (k, v) in labels)..., inputs...))
    Step(rule.name, instance_work(rule.work, ctx, draft), inputs, outputs, res, labels, recipe)
end

instance_work(template::String, ctx, _) = Template(template, ctx)
instance_work(j::JuliaScript, ctx, _) = JuliaScript(j.template, ctx)
instance_work(f::Function, ctx, draft) = Call(f, (draft.args..., ctx.outputs...))
instance_work(c::Base.AbstractCmd, _, _) = c

recipe_source(template::String) = template
recipe_source(j::JuliaScript) = "julia " * j.template
recipe_source(::Function) = "julia"
recipe_source(c::Base.AbstractCmd) = string(c)

keep(::Nothing, found::Assignments) = found
keep(f, found::Assignments) = Assignments([a for a in found if f(Wildcards(a))::Bool])

#==============================================================================#
# Pattern tracing: which wildcard values can a pattern take?
#==============================================================================#

# Probe tokens stand in for a pattern's wildcards while it is matched against a rule
# output: control characters cannot occur in rule literals, so a token is never split.
const TOKEN = r"\x01(\x03*)\x02"
probe_token(k::Int) = string('\x01', '\x03'^k, '\x02')

"""
How rule output `out` must be filled to produce pattern `p`: a map from the rule's
wildcards to text in which `p`'s wildcards appear as probe tokens, or `nothing`.
"""
function unify(out::Pattern, p::Pattern)
    names = wildcard_names(p)
    probe = substitute(p.text, Dict(n => probe_token(k - 1) for (k, n) in enumerate(names)))
    mapping = match_path(out, probe)
    mapping === nothing && return nothing
    all(intact, values(mapping)) ? mapping : nothing
end
intact(v::AbstractString) = !occursin(r"[\x01\x02\x03]", replace(v, TOKEN => ""))

"""Turn probe tokens back into `{name}` wildcards of `p`."""
untokenize(text::AbstractString, names::Vector{String}) =
    replace(text, TOKEN => m -> string("{", names[count(==('\x03'), m) + 1], "}"))

# Rule wildcards absent from the mapping (gather-only) get a private name so they
# cannot collide with the wildcards of the traced pattern.
bind_for_trace(text::AbstractString, mapping) =
    replace(text, WILDCARD => m -> get(() -> string("{", SubString(m, 2, lastindex(m) - 1), "__free}"),
                                       mapping, SubString(m, 2, lastindex(m) - 1)))

"""Wildcard assignments of `p` that can be built from existing source files."""
function trace!(R::Resolver, p::Pattern)
    haskey(R.traced, p.text) && return R.traced[p.text]
    p.text in R.tracing && return Assignments()
    push!(R.tracing, p.text)
    found = Assignments()
    producible = false
    for entry in R.rules
        producible |= trace_entry!(found, R, entry, p)
    end
    producible || append!(found, source_assignments!(R, p))
    delete!(R.tracing, p.text)
    R.traced[p.text] = unique!(found)
end

function source_assignments!(R::Resolver, p::Pattern)
    found = Assignments([values for (_, values) in glob(p)])
    for path in R.pending
        values = match_path(p, path)
        values === nothing || push!(found, values)
    end
    R.sources[p.text] = length(found)
    unique!(found)
end

function trace_entry!(found, R::Resolver, s::Step, p::Pattern)
    produced = false
    for out in s.outputs
        values = match_path(p, out)
        values === nothing && continue
        push!(found, values)
        produced = true
    end
    produced
end

function trace_entry!(found, R::Resolver, rule::Rule, p::Pattern)
    for out in rule.outputs
        mapping = unify(out, p)
        mapping === nothing && continue
        append!(found, trace_rule(R, rule, mapping, wildcard_names(p)))
        return true
    end
    false
end

"""Join the assignments of every input of `rule` (with `mapping` applied), keeping `names`."""
function trace_rule(R::Resolver, rule::Rule, mapping, names::Vector{String})
    partial = Assignments([Dict{String,String}()])
    for inp in rule.inputs
        ip = Pattern(untokenize(bind_for_trace(inp.pattern.text, mapping), names))
        if !has_wildcards(ip)
            feasible(resolve!(R, ip.text)) || return Assignments()
            continue
        end
        partial = join_assignments(partial, project(trace!(R, ip), names))
        isempty(partial) && return partial
    end
    Assignments([a for a in partial if all(n -> haskey(a, n), names)])
end

feasible(::Union{Job,Leaf}) = true
feasible(::Unresolved) = false

project(found::Assignments, names) =
    unique!(Assignments([Dict(k => v for (k, v) in a if k in names) for a in found]))

"""Natural join: combine assignments that agree on their shared wildcards."""
function join_assignments(a::Assignments, b::Assignments)
    (isempty(a) || isempty(b)) && return Assignments()
    shared = sort!(collect(intersect(keys(first(a)), keys(first(b)))))
    index = Dict{Vector{String},Assignments}()
    for y in b
        push!(get!(Assignments, index, [y[k] for k in shared]), y)
    end
    joined = Assignments()
    for x in a, y in get(index, [x[k] for k in shared], Assignments())
        push!(joined, merge(x, y))
    end
    unique!(joined)
end

function no_matches_message(R::Resolver, what::AbstractString)
    checked = isempty(R.sources) ? "" : "\nSource patterns checked on disk: " *
        join(("$p ($n match$(n == 1 ? "" : "es"))" for (p, n) in sort!(collect(R.sources))), ", ")
    "$what matched nothing: no source files give values for its wildcards.$checked"
end

#==============================================================================#
# Targets
#==============================================================================#

"""What a resolved workflow runs and the paths it builds."""
struct WorkflowPlan
    node::AbstractNode
    paths::Vector{String}
    jobs::Int
end

"""Default requests: outputs of entries no other entry consumes, and output-less rules."""
function auto_requests(rules::AbstractVector)
    requests = Union{String,Rule}[]
    for entry in rules
        consumed(entry, rules) && continue
        add_request!(requests, entry)
    end
    requests
end
add_request!(requests, s::Step) = push!(requests, first(s.outputs))
add_request!(requests, r::Rule) = push!(requests, isempty(r.outputs) ? r : first(r.outputs).text)

output_patterns(s::Step) = Pattern.(s.outputs)
output_patterns(r::Rule) = r.outputs
input_patterns(s::Step) = Pattern.(s.inputs)
input_patterns(r::Rule) = [i.pattern for i in r.inputs]

consumed(entry, rules) = any(other -> other !== entry && feeds(entry, other), rules)
feeds(producer, consumer) =
    any(out -> any(inp -> unify(out, inp) !== nothing, input_patterns(consumer)), output_patterns(producer))

"""Resolve a workflow into a WorkflowPlan, or Unresolved with the reason."""
function plan_workflow(wf::Workflow, pending::Set{String}=Set{String}())
    R = Resolver(wf.rules, wf.filter, pending)
    requests = isempty(wf.targets) ? auto_requests(wf.rules) : Union{String,Rule}[wf.targets...]
    isempty(requests) && return Unresolved("workflow `$(wf.name)` has nothing to build: add rules or targets")
    jobs = AbstractNode[]
    paths = String[]
    for request in requests
        u = request!(jobs, paths, R, request)
        u === nothing || return u
    end
    node = length(jobs) == 1 ? only(jobs) : Parallel(jobs)
    WorkflowPlan(node, paths, count_steps(node))
end

function request!(jobs, paths, R::Resolver, target::String)
    p = Pattern(target)
    has_wildcards(p) || return request_path!(jobs, paths, R, target)
    found = keep(R.filter, trace!(R, p))
    isempty(found) && return Unresolved(no_matches_message(R, "target \"$target\""))
    for a in found
        u = request_path!(jobs, paths, R, substitute(p, a))
        u === nothing || return u
    end
    nothing
end

function request_path!(jobs, paths, R::Resolver, path::String)
    path in paths && return nothing
    push!(paths, path)
    add_request_node!(jobs, resolve!(R, path), path)
end
add_request_node!(jobs, j::Job, _) = (push_unique!(jobs, j); nothing)
add_request_node!(jobs, ::Leaf, _) = nothing
add_request_node!(jobs, u::Unresolved, path) = Unresolved("cannot build `$path`:\n" * indent(u.reason, "  "))

# A rule without outputs runs once per assignment of its input wildcards.
function request!(jobs, paths, R::Resolver, sink::Rule)
    names = rule_wildcards(sink)
    identity_map = Dict(n => "{$n}" for n in names)
    found = keep(R.filter, trace_rule(R, sink, identity_map, names))
    isempty(found) && return Unresolved(no_matches_message(R, "rule `$(sink.name)`"))
    for a in found
        u = add_request_node!(jobs, instantiate!(R, Candidate(sink, a, 0)), "rule `$(sink.name)`")
        u === nothing || return u
    end
    nothing
end

"""
    plan(rules...; targets=String[], filter=nothing)
    plan(workflow::Workflow)

Resolve rules into the concrete DAG that would run, without running anything. Print
it with [`print_dag`](@ref), run it with [`run`](@ref), or compose it with other
nodes. Errors with an explanation when a target cannot be built.

```julia
print_dag(plan(trim, align; targets="bam/A.bam"))
```
"""
plan(wf::Workflow) = plan_result(plan_workflow(wf))
plan(r::Rule, more::Union{Rule,Step}...; targets=String[], filter=nothing, name::AbstractString="workflow") =
    plan(Workflow(r, more...; targets, filter, name))
plan_result(p::WorkflowPlan) = p.node
plan_result(u::Unresolved) = error(u.reason)

@specialize
