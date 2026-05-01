# Snakemake-style output-pattern rules.
#
# A `Rule` declares input and output *patterns* (with `{wildcard}` placeholders) and
# a work template. Calling `resolve(rules, targets)` walks each requested target
# backward through the rule set, instantiating concrete `Step`s for every link in
# the dependency chain. Steps that produce shared dependencies are deduplicated by
# output-path identity, so the runtime DAG protocol (claim/in-flight) executes
# them exactly once even when reached from multiple targets.
#
# Substitutions in the work template:
#     {input}      → space-joined concrete input paths
#     {input[i]}   → i-th concrete input
#     {output}     → space-joined concrete output paths
#     {output[i]}  → i-th concrete output
#     {wildcard}   → the value extracted from the matched output pattern

const WILDCARD_RE = r"\{(\w+)\}"

"""
    Rule(name, inputs, outputs, work)

A reusable production recipe. `inputs` and `outputs` are vectors of pattern strings
(e.g. `"data/{sample}.fq"`). `work` is either:

- `String`: a shell command template using `{input}`, `{output}`, `{wildcard}` placeholders.
- `Function(inputs, outputs, wildcards)`: builds a node (Step / Cmd / function) at resolve time.

Use [`@rule`](@ref) for ergonomic construction, and [`resolve`](@ref) to turn a set
of rules + targets into a runnable DAG.
"""
struct Rule{W}
    name::Symbol
    inputs::Vector{String}
    outputs::Vector{String}
    work::W
    # Inner constructor only: the default `Rule{W}(::Vector{String}, ...)` would bypass
    # `validate_rule_wildcards` when argument types match the struct fields exactly.
    function Rule(name::Symbol, inputs::AbstractVector, outputs::AbstractVector, work::W) where {W}
        inputs_s = String[String(s) for s in inputs]
        outputs_s = String[String(s) for s in outputs]
        validate_rule_wildcards(name, inputs_s, outputs_s)
        new{W}(name, inputs_s, outputs_s, work)
    end
end

"""Collect distinct wildcard names that appear in any of the patterns, in order of first appearance."""
function pattern_wildcards(patterns::Vector{String})
    seen = Set{String}()
    names = String[]
    for p in patterns, m in eachmatch(WILDCARD_RE, p)
        n = String(m.captures[1])
        n in seen && continue
        push!(seen, n)
        push!(names, n)
    end
    names
end

# Snakemake contract: every wildcard referenced in inputs must come from outputs (or the
# concrete target). Catch typos like `{smaple}` in inputs vs `{sample}` in outputs.
function validate_rule_wildcards(name::Symbol, inputs::Vector{String}, outputs::Vector{String})
    out_wc = Set(pattern_wildcards(outputs))
    in_wc = pattern_wildcards(inputs)
    extras = String[w for w in in_wc if !(w in out_wc)]
    isempty(extras) ||
        error("Rule `$name`: input pattern uses wildcard(s) $(extras) not present in any output pattern $(outputs).")
    nothing
end

"""
    @rule name(inputs => outputs) = work

Construct a [`Rule`](@ref). Mirrors `@step` but uses *patterns* rather than concrete paths.

# Examples
```julia
align = @rule align("data/{sample}.fq" => "out/{sample}.bam") =
    "bwa mem ref.fa {input} > {output}"
index = @rule index("out/{sample}.bam" => "out/{sample}.bam.bai") =
    "samtools index {input}"

plan = resolve([align, index], ["out/A.bam.bai", "out/B.bam.bai"])
run(plan)
```
"""
macro rule(expr)
    rule_expr(expr)
end

function rule_expr(expr::Expr)
    expr.head === :(=) || error("@rule expects `name(inputs => outputs) = work`, got $(expr)")
    lhs, rhs = expr.args[1], expr.args[2]
    rule_lhs(lhs, rhs)
end

function rule_lhs(lhs::Expr, rhs)
    lhs.head === :call || error("@rule expects `name(inputs => outputs) = work`, got $(lhs)")
    length(lhs.args) >= 2 || error("@rule needs an inputs => outputs pair, got $(lhs)")
    name = QuoteNode(lhs.args[1])
    deps = length(lhs.args) == 2 ? lhs.args[2] : Expr(:vect, lhs.args[2:end]...)
    rule_deps(name, deps, rhs)
end

function rule_deps(name, deps::Expr, rhs)
    if deps.head === :call && length(deps.args) >= 3 && deps.args[1] === :(=>)
        inputs = deps.args[2]
        outputs = deps.args[3]
        return :(Rule($name,
                      $(rule_paths_expr(inputs)),
                      $(rule_paths_expr(outputs)),
                      $(esc(rhs))))
    end
    error("@rule needs `inputs => outputs`, got $(deps)")
end

rule_paths_expr(s::String) = :([$s])
rule_paths_expr(x) = esc(x)

"""
    pattern_to_regex(pattern) -> (Regex, Vector{String})

Compile a wildcard pattern (with `{name}` placeholders) to a regex matching the
whole string, returning the regex and the wildcard names in left-to-right order.
"""
function pattern_to_regex(pattern::String)
    placeholder = "\x00WILD\x00"
    names = String[]
    temp = replace(pattern, WILDCARD_RE => function(m)
        push!(names, m[2:end-1])
        placeholder
    end)
    # Escape backslashes in the pattern first; otherwise a later `\.` becomes `\\.` after
    # the `\`-pass. Do not escape `/` (not special in Julia/PCRE here; escaping `/` then
    # `\` had produced broken patterns like `data\\/...`).
    temp = replace(temp, "\\" => "\\\\")
    for c in ".+^*?\$()[]|"
        temp = replace(temp, string(c) => "\\" * c)
    end
    for _ in names
        temp = replace(temp, placeholder => "([^/]+)"; count=1)
    end
    (Regex("^" * temp * "\$"), names)
end

"""
    match_pattern(pattern, concrete) -> Union{Nothing, Dict{String,String}}

Return the wildcard map if `concrete` matches `pattern`, else `nothing`.
"""
function match_pattern(pattern::String, concrete::String)
    rx, names = pattern_to_regex(pattern)
    m = match(rx, concrete)
    m === nothing && return nothing
    out = Dict{String,String}()
    for (i, n) in enumerate(names)
        v = m.captures[i]
        v === nothing || (out[n] = String(v))
    end
    out
end

"""
    substitute(template, wildcards) -> String

Replace each `{name}` in `template` with `wildcards[name]`. Errors if a wildcard
is not in the dict.
"""
function substitute(template::String, wildcards::AbstractDict)
    replace(template, WILDCARD_RE => function(m)
        key = m[2:end-1]
        haskey(wildcards, key) ||
            error("Substitution: wildcard `{$key}` has no value (have: $(collect(keys(wildcards))))")
        wildcards[key]
    end)
end

"""
    fill_special(template, inputs, outputs) -> String

Expand `{input}`, `{input[i]}`, `{output}`, `{output[i]}` in a shell template.
"""
function fill_special(template::String, inputs::Vector{String}, outputs::Vector{String})
    s = template
    s = replace(s, r"\{input\[(\d+)\]\}" => sub -> begin
        i = parse(Int, match(r"\{input\[(\d+)\]\}", sub).captures[1])
        inputs[i]
    end)
    s = replace(s, r"\{output\[(\d+)\]\}" => sub -> begin
        i = parse(Int, match(r"\{output\[(\d+)\]\}", sub).captures[1])
        outputs[i]
    end)
    s = replace(s, "{input}" => join(inputs, " "))
    s = replace(s, "{output}" => join(outputs, " "))
    s
end

# A rule produces concrete (inputs, outputs, node) at resolve time.
# Dispatch on the work type.
function instantiate(rule::Rule{<:AbstractString}, inputs::Vector{String},
                     outputs::Vector{String}, wildcards::AbstractDict, deps::Vector{<:AbstractNode})
    cmd_str = substitute(fill_special(String(rule.work), inputs, outputs), wildcards)
    compose_with_deps(Step(rule.name, sh(cmd_str), inputs, outputs), deps)
end

function instantiate(rule::Rule{<:Function}, inputs::Vector{String},
                     outputs::Vector{String}, wildcards::AbstractDict, deps::Vector{<:AbstractNode})
    work = rule.work(inputs, outputs, wildcards)
    compose_with_deps(build_step_from_work(rule.name, work, inputs, outputs), deps)
end

function compose_with_deps(node::AbstractNode, deps::Vector{<:AbstractNode})
    isempty(deps) && return node
    length(deps) == 1 && return Sequence(AbstractNode[deps[1], node])
    Sequence(AbstractNode[Parallel(AbstractNode[deps...]), node])
end

build_step_from_work(name::Symbol, w::Cmd, inputs, outputs) = Step(name, w, inputs, outputs)
build_step_from_work(name::Symbol, w::String, inputs, outputs) = Step(name, sh(w), inputs, outputs)
build_step_from_work(name::Symbol, w::Function, inputs, outputs) = Step(name, w, inputs, outputs)
build_step_from_work(name::Symbol, w::ShRun, inputs, outputs) = Step(name, w, inputs, outputs)
build_step_from_work(name::Symbol, w::AbstractNode, _inputs, _outputs) = w
build_step_from_work(name::Symbol, w, inputs, outputs) =
    error("Rule `$name`: work function must return Cmd, String, Function, ShRun, or AbstractNode, got $(typeof(w))")

"""
    resolve(rules, targets) -> AbstractNode

Build a DAG that produces every path in `targets`, working backward through
`rules`. A target that already exists on disk is treated as satisfied (no node
emitted for it). Multiple targets sharing a dependency reuse the same `Step`
instance, so the runtime DAG protocol executes that dependency exactly once.

If every requested target already exists on disk, returns a no-op node
([`NoWork`](@ref)) that produces no step results when run — making "rerun the
pipeline; everything's fresh" friendly instead of an error.

Throws if a non-existent target has no producing rule, or if rule resolution
forms a cycle.
"""
resolve(rules::AbstractVector{<:Rule}, target::AbstractString) =
    resolve(rules, [String(target)])
function resolve(rules::AbstractVector{<:Rule}, targets::AbstractVector{<:AbstractString})
    cache = Dict{String, AbstractNode}()
    visited = Set{String}()
    nodes = AbstractNode[]
    for t in targets
        n = resolve_target!(rules, String(t), cache, visited)
        n === nothing && continue
        push!(nodes, n)
    end
    isempty(nodes) && return NoWork()
    length(nodes) == 1 && return nodes[1]
    Parallel(nodes)   # Vector{AbstractNode}
end

# Returns AbstractNode (the node producing target) or nothing (target already on disk).
function resolve_target!(rules::AbstractVector{<:Rule}, target::String,
                         cache::Dict{String,AbstractNode}, visited::Set{String})
    haskey(cache, target) && return cache[target]
    target in visited && error("resolve: cycle detected at `$target`")
    isfile(target) && return nothing
    rule_match = find_rule(rules, target)
    rule_match === nothing &&
        error("resolve: no rule produces `$target` and the file does not exist")
    rule, wildcards = rule_match
    inputs = String[substitute(p, wildcards) for p in rule.inputs]
    outputs = String[substitute(p, wildcards) for p in rule.outputs]
    push!(visited, target)
    deps = AbstractNode[]
    for inp in inputs
        sub = resolve_target!(rules, inp, cache, visited)
        sub === nothing || push!(deps, sub)
    end
    delete!(visited, target)
    node = instantiate(rule, inputs, outputs, wildcards, deps)
    # Cache by every output the rule produces, so any sibling target sharing
    # this dep gets the exact same node instance.
    for op in outputs
        cache[op] = node
    end
    node
end

function find_rule(rules::AbstractVector{<:Rule}, target::String)
    for r in rules
        for op in r.outputs
            wc = match_pattern(op, target)
            wc === nothing && continue
            return (r, wc)
        end
    end
    nothing
end

#==============================================================================#
# expand — declarative target generation by Cartesian product of wildcard values
#==============================================================================#

"""
    expand(template, wildcards) -> Vector{String}
    expand(template; wildcard1=values1, wildcard2=values2, ...) -> Vector{String}
    expand(templates::AbstractVector, ...) -> Vector{String}

Generate concrete paths from one or more pattern templates by taking the
Cartesian product of the wildcard value lists. `wildcards` may be a `NamedTuple`
or supplied as keyword arguments; values are anything that can be turned into a
string via `string`.

# Examples
```julia
expand("out/{sample}.bam"; sample=["A","B","C"])
# ["out/A.bam", "out/B.bam", "out/C.bam"]

expand("out/{s}.{ext}"; s=["A","B"], ext=["bam","bai"])
# ["out/A.bam", "out/A.bai", "out/B.bam", "out/B.bai"]

expand(["raw/{s}.fq", "qc/{s}.html"]; s=["A","B"])
# ["raw/A.fq", "raw/B.fq", "qc/A.html", "qc/B.html"]
```

When there are multiple wildcards, `Iterators.product` is used so combinations are
generated with the *first* keyword varying slowest and the last varying fastest.
"""
expand(template::AbstractString; kwargs...) = expand(template, NamedTuple(kwargs))
expand(templates::AbstractVector{<:AbstractString}; kwargs...) =
    expand(templates, NamedTuple(kwargs))

expand(template::AbstractString, wildcards::NamedTuple) =
    expand_each(String[String(template)], wildcards)
expand(templates::AbstractVector{<:AbstractString}, wildcards::NamedTuple) =
    expand_each(String[String(t) for t in templates], wildcards)

# Substitute supports NamedTuples too — keys are Symbols, values stringified on demand.
function substitute(template::String, wildcards::NamedTuple)
    replace(template, WILDCARD_RE => function(m)
        key = Symbol(m[2:end-1])
        haskey(wildcards, key) ||
            error("Substitution: wildcard `{$key}` has no value (have: $(collect(keys(wildcards))))")
        string(getfield(wildcards, key))
    end)
end

# Inner workhorse: take the Cartesian product over the NamedTuple's fields and
# substitute into every template once per combination.
function expand_each(templates::Vector{String}, wildcards::NamedTuple)
    isempty(wildcards) && return copy(templates)
    ks = keys(wildcards)
    vs = values(wildcards)
    out = String[]
    sizehint!(out, length(templates) * prod(length, vs; init=1))
    for tmpl in templates, combo in Iterators.product(vs...)
        push!(out, substitute(tmpl, NamedTuple{ks}(combo)))
    end
    out
end

#==============================================================================#
# Workflow — registry of rules + default targets
#==============================================================================#

"""
    Workflow(; name="workflow") -> Workflow

A registry of [`Rule`](@ref)s and default targets. Sugar over `resolve(rules, targets)`
that lets a user describe a pipeline as one object:

```julia
wf = Workflow(name="rnaseq")
push!(wf,
    @rule align("raw/{s}.fq" => "out/{s}.bam") = "bwa mem ref.fa {input} > {output}",
    @rule index("out/{s}.bam" => "out/{s}.bam.bai") = "samtools index {input}")
push!(wf, expand("out/{s}.bam.bai"; s=["A","B","C"]))
run(wf)
```

Items pushed to a workflow are dispatched by type: `Rule` items go to `wf.rules`,
strings (or vectors of strings) go to `wf.targets`. You can override targets at run
time: `run(wf; targets=["out/A.bam.bai"])`.
"""
mutable struct Workflow
    name::String
    rules::Vector{Rule}
    targets::Vector{String}
end
Workflow(; name::AbstractString="workflow") = Workflow(String(name), Rule[], String[])

# Heterogeneous push: dispatch routes each item to the right sub-vector. This
# replaces ad-hoc add_rule!/add_target! helpers — Julia's verb-based mutation
# convention covers the case.
Base.push!(wf::Workflow, items...) = (foreach(it -> push_item!(wf, it), items); wf)

push_item!(wf::Workflow, r::Rule) = push!(wf.rules, r)
push_item!(wf::Workflow, t::AbstractString) = push!(wf.targets, String(t))
push_item!(wf::Workflow, ts::AbstractVector{<:AbstractString}) =
    append!(wf.targets, (String(t) for t in ts))
push_item!(wf::Workflow, rs::AbstractVector{<:Rule}) = append!(wf.rules, rs)

"""
    plan(wf::Workflow; targets=wf.targets) -> AbstractNode

Resolve the workflow's rules against `targets`, returning a node ready to `run`.
Useful for inspecting the build plan with [`print_dag`](@ref) before execution.
"""
function plan(wf::Workflow; targets::AbstractVector{<:AbstractString}=wf.targets)
    isempty(wf.rules) &&
        error("Workflow `$(wf.name)` has no rules; push! a Rule onto it first.")
    isempty(targets) &&
        error("Workflow `$(wf.name)` has no targets; push! some, or pass targets= to run.")
    resolve(wf.rules, [String(t) for t in targets])
end

# A Workflow is runnable directly via Base.run.
Base.run(wf::Workflow; targets::AbstractVector{<:AbstractString}=wf.targets, kwargs...) =
    run(Pipeline(plan(wf; targets=targets); name=wf.name); kwargs...)
