# Rules are wildcard step templates; a Workflow is a set of rules (and concrete
# steps) plus the targets to build. Resolution into concrete steps happens in
# resolve.jl, lazily, when a Workflow runs.

@nospecialize   # see runtime.jl

"""A `gather(...)` rule input."""
struct Gather
    pattern::String
end
"""
    gather(pattern)

Rule input that collects *every* value of the wildcards that do not appear in the
outputs. The values are found by tracing the rules back to existing source files,
so `gather` works for files that other rules have not produced yet.

```julia
@rule merge(gather("bam/{sample}.bam") => "all.vcf") =
    "bcftools mpileup -f ref.fa {input} | bcftools call -mv -o {output}"
```

A function rule receives a gathered input as a `Vector{String}`; in a shell template
`{input[i]}` expands to the space-separated list.
"""
gather(pattern::AbstractString) = Gather(String(pattern))

struct RuleInput
    pattern::Pattern
    gather::Bool
end
RuleInput(s::AbstractString) = RuleInput(Pattern(s), false)
RuleInput(g::Gather) = RuleInput(Pattern(g.pattern), true)
RuleInput(x) = throw(ArgumentError("rule inputs must be paths or gather(...), got $(summary(x))"))

"""
    Rule(name, inputs, outputs, work; threads=0, mem_mb=0)

A step template whose paths contain `{wildcards}`. Usually built with
[`@rule`](@ref) or with [`@step`](@ref) when the paths contain wildcards.

- `inputs`: patterns, possibly [`gather`](@ref)`(...)` entries.
- `outputs`: patterns; every output must use the same wildcards, and every
  (non-gather) input wildcard must appear in the outputs.
- `work`: a shell template (`String` or `sh"..."`) with placeholders `{input}`,
  `{output}`, `{input[i]}`, `{output[i]}`, `{threads}`, `{mem_mb}` and `{wildcard}`
  (`{{x}}` is a literal `{x}`), or a function called as `f(inputs..., outputs...)`.

A rule produces nothing by itself: use it in a [`Workflow`](@ref), [`run`](@ref) it,
or compose rules with `>>`/`&` (which merges them into one Workflow).
"""
struct Rule
    name::Symbol
    inputs::Vector{RuleInput}
    outputs::Vector{Pattern}
    work::Union{String,Function,Base.AbstractCmd}
    resources::Resources
end

function Rule(name::Symbol, inputs, outputs, work; threads::Integer=0, mem_mb::Integer=0)
    ins = RuleInput[RuleInput(x) for x in input_list(inputs)]
    outs = Pattern[Pattern(x) for x in path_list(outputs)]
    rule = Rule(name, ins, outs, rule_work(work), Resources(; threads, mem_mb))
    validate(rule)
    rule
end

input_list(x::Union{AbstractString,Gather}) = [x]
input_list(xs::Union{AbstractVector,Tuple}) = xs
input_list(x) = throw(ArgumentError("rule inputs must be paths or gather(...), got $(summary(x))"))

rule_work(s::AbstractString) = String(s)
rule_work(s::Shell) = s.script
rule_work(f::Function) = f
rule_work(c::Base.AbstractCmd) = c
rule_work(w) = throw(ArgumentError(
    "rule work must be a shell template (String or sh\"...\"), a Cmd or a function; got $(summary(w))"))

"""Wildcards that identify one instance of a rule (in order of first appearance)."""
rule_wildcards(r::Rule) = isempty(r.outputs) ?
    unique(n for i in r.inputs if !i.gather for n in i.pattern.names) :
    wildcard_names(first(r.outputs))

function validate(r::Rule)
    isempty(r.inputs) && isempty(r.outputs) && rule_error(r, "has neither inputs nor outputs")
    keys = rule_wildcards(r)
    for out in r.outputs
        Set(out.names) == Set(keys) ||
            rule_error(r, "all outputs must use the same wildcards; `$(out.text)` differs from `$(first(r.outputs).text)`")
    end
    for name in keys
        name in RESERVED_NAMES && rule_error(r, "{$name} is reserved; rename the wildcard")
    end
    for inp in r.inputs
        inp.gather && continue
        for name in wildcard_names(inp.pattern)
            name in keys && continue
            rule_error(r, "input `$(inp.pattern.text)` uses {$name}, which no output defines " *
                          "(outputs use: $(join(("{$k}" for k in keys), ", "))). " *
                          "Check the spelling, or use gather(...) to collect all values.")
        end
    end
    validate_template(r, r.work)
end

validate_template(r::Rule, ::Union{Function,Base.AbstractCmd}) = nothing
function validate_template(r::Rule, template::String)
    groups = [[i.pattern.text] for i in r.inputs]
    outputs = [o.text for o in r.outputs]
    wildcards = Dict(n => n for n in rule_wildcards(r))
    render(template, TemplateContext(groups, outputs, wildcards, 1, 0))
    nothing
end

rule_error(r::Rule, msg) = throw(ArgumentError("rule `$(r.name)` $msg"))

#==============================================================================#
# Workflow
#==============================================================================#

"""
    Workflow(rules...; targets=String[], name="workflow", filter=nothing)

A set of [`Rule`](@ref)s and concrete [`Step`](@ref)s with declared outputs, plus the
targets to build. A Workflow is a node: it is resolved into concrete steps when it
runs, so it sees files produced by earlier steps.

Targets are concrete paths or *patterns*. A pattern target such as `"bam/{s}.bam"`
builds every instance whose source files exist: wildcard values are discovered by
tracing the rules back to files on disk. Without targets, the workflow builds the
outputs of every rule that no other rule consumes (and runs rules that have no
outputs once per matching input).

`filter(w::Wildcards) -> Bool` drops discovered wildcard values, e.g.
`filter = w -> w.sample != "control"`.

The value of a Workflow (passed downstream with `>>`) is the vector of target paths.

```julia
trim  = @rule trim("raw/{s}.fq.gz" => "trim/{s}.fq.gz") = "fastp -i {input} -o {output}"
align = @rule align("trim/{s}.fq.gz" => "bam/{s}.bam"; threads=8) =
    "bwa mem -t {threads} ref.fa {input} | samtools sort -o {output}"

run(Workflow(trim, align))                            # every sample with raw data
run(Workflow(trim, align; targets="bam/A.bam"))       # just sample A
```

See also [`@workflow`](@ref), [`plan`](@ref), [`check`](@ref).
"""
struct Workflow <: AbstractNode
    name::String
    rules::Vector{Union{Rule,Step}}
    targets::Vector{String}
    filter::Union{Nothing,Function}
    function Workflow(items...; name::AbstractString="workflow", targets=String[], filter=nothing)
        wf = new(String(name), Union{Rule,Step}[], String[], filter)
        push!(wf, items...)
        push!(wf, path_list(targets))
        wf
    end
end

Base.push!(wf::Workflow, items...) = (foreach(x -> add_entry!(wf, x), items); wf)

add_entry!(wf::Workflow, r::Rule) = push_unique!(wf.rules, r)
function add_entry!(wf::Workflow, s::Step)
    isempty(s.outputs) && throw(ArgumentError(
        "step `$(step_label(s))` declares no outputs, so a Workflow cannot use it"))
    push_unique!(wf.rules, s)
end
add_entry!(wf::Workflow, t::AbstractString) = (String(t) in wf.targets || push!(wf.targets, String(t)); wf)
add_entry!(wf::Workflow, w::Workflow) = (foreach(x -> add_entry!(wf, x), w.rules); foreach(t -> add_entry!(wf, t), w.targets); wf)
add_entry!(wf::Workflow, xs::Union{AbstractVector,Tuple}) = (foreach(x -> add_entry!(wf, x), xs); wf)
add_entry!(::Workflow, x) = throw(ArgumentError(
    "cannot add $(summary(x)) to a Workflow; expected rules, steps with outputs, or target paths"))

push_unique!(v::Vector, x) = any(y -> y === x, v) ? v : push!(v, x)

as_node(r::Rule) = Workflow(r)

rules_of(r::Rule) = Union{Rule,Step}[r]
rules_of(w::Workflow) = w.rules
targets_of(::Rule) = String[]
targets_of(w::Workflow) = w.targets
filter_of(::Rule) = nothing
filter_of(w::Workflow) = w.filter
name_of(::Rule) = "workflow"
name_of(w::Workflow) = w.name

pick_filter(::Nothing, b) = b
pick_filter(a, _) = a

"""
Merge rules and workflows (`a >> b`, `a & b`). Rules are unordered: dependencies come
from their file patterns. Targets stay automatic unless one side has explicit targets.
"""
function merge_workflows(a, b)
    automatic = isempty(targets_of(a)) && isempty(targets_of(b))
    targets = automatic ? String[] : unique!(vcat(effective_targets(a), effective_targets(b)))
    Workflow(rules_of(a), rules_of(b); name=name_of(a), targets,
             filter=pick_filter(filter_of(a), filter_of(b)))
end

effective_targets(x) = isempty(targets_of(x)) ?
    String[t for r in auto_requests(rules_of(x)) for t in request_paths(r)] : targets_of(x)
request_paths(t::String) = [t]
request_paths(::Rule) = String[]

@specialize
