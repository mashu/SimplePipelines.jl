# Rules: `{wildcard}` patterns on paths; `resolve(rules, targets)` builds concrete Steps.
# Shared outputs deduplicate so each Step runs once per run.
#
# Work template placeholders:
#     {input}      → space-joined concrete input paths
#     {input[i]}   → i-th concrete input
#     {output}     → space-joined concrete output paths
#     {output[i]}  → i-th concrete output
#     {wildcard}   → the value extracted from the matched output pattern

# WILDCARD_RE and escape_regex_literal are defined in ForEach.jl (loaded first)
# and reused here for pattern→regex compilation.

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
        # Typed `String[...]` prefix is load-bearing: `inputs` is the unconstrained
        # `AbstractVector`, so an *empty* untyped comprehension would infer `Vector{Any}`
        # and miss the `validate_rule_wildcards(::Vector{String}, ...)` dispatch.
        inputs_s = String[String(s) for s in inputs]
        outputs_s = String[String(s) for s in outputs]
        validate_rule_wildcards(name, inputs_s, outputs_s)
        new{W}(name, inputs_s, outputs_s, work)
    end
end

"""Distinct wildcard names appearing in any pattern, in order of first appearance."""
pattern_wildcards(patterns::Vector{String}) =
    unique(String(m.captures[1]) for p in patterns for m in eachmatch(WILDCARD_RE, p))

# Every wildcard used in input patterns must appear in output patterns (or the concrete
# target). Catches typos like `{smaple}` in inputs vs `{sample}` in outputs.
function validate_rule_wildcards(name::Symbol, inputs::Vector{String}, outputs::Vector{String})
    out_wc = Set(pattern_wildcards(outputs))
    extras = [w for w in pattern_wildcards(inputs) if w ∉ out_wc]
    isempty(extras) && return
    error("Rule `$name`: input pattern uses wildcard(s) $(extras) not present in any output pattern $(outputs).")
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

"""
    @targets "out/{sample}.bam" sample=["A", "B"]
    @targets ["out/{sample}.bam", "qc/{sample}.html"] sample=["A", "B"]

Generate concrete target paths from wildcard values. This is macro sugar over
[`expand`](@ref) for use inside [`@workflow`](@ref) blocks or `push!(wf, ...)`.
"""
macro targets(template, args...)
    Expr(:call, :expand, Expr(:parameters, (esc(a) for a in args)...), esc(template))
end

rule_expr(x) = error("@rule expects `name(inputs => outputs) = work`, got $(x)")

function rule_expr(expr::Expr)
    expr.head === :tuple && return rule_expr_tuple(expr)
    expr.head === :(=) && return rule_expr_assign(expr)
    error("@rule expects `name(inputs => outputs) = work`, got $(expr)")
end

unwrap_singleton_block(x) = x
function unwrap_singleton_block(expr::Expr)
    expr.head === :block || return expr
    stmts = Any[]
    for a in expr.args
        push_stmt_unless_linenumber!(stmts, a)
    end
    length(stmts) == 1 ? stmts[1] : expr
end

push_stmt_unless_linenumber!(::Vector{Any}, ::LineNumberNode) = nothing
push_stmt_unless_linenumber!(stmts::Vector{Any}, a) = push!(stmts, a)

is_tuple_expr(x) = false
is_tuple_expr(expr::Expr) = (expr.head === :tuple)

macrocall_rule_payload(x) = x
function macrocall_rule_payload(expr::Expr)
    (expr.head === :macrocall && expr.args[1] === Symbol("@rule")) ? expr.args[end] : expr
end

function rule_expr_tuple(expr::Expr)
    rules = Any[rule_expr(macrocall_rule_payload(a)) for a in expr.args]
    Expr(:vect, rules...)
end

function rule_expr_assign(expr::Expr)
    lhs, rhs = expr.args[1], expr.args[2]
    rhs_unwrapped = unwrap_singleton_block(rhs)

    # In `push!(wf, @rule a(...) = "cmd a", @rule b(...) = "cmd b")`, parsing can turn
    # the RHS into a tuple: `"cmd a", @rule b(...) = "cmd b"`. Treat that as multiple
    # rules returned as a vector.
    if is_tuple_expr(rhs_unwrapped)
        rhs_tuple = rhs_unwrapped::Expr
        first_work = rhs_tuple.args[1]
        rest = rhs_tuple.args[2:end]
        rules = Any[rule_lhs(lhs, first_work)]
        append!(rules, (rule_expr(macrocall_rule_payload(a)) for a in rest))
        return Expr(:vect, rules...)
    end

    rule_lhs(lhs, rhs_unwrapped)
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
    # Escape backslashes first so the regex-meta pass below doesn't double-escape
    # the backslashes it just inserted. `/` is not regex-meta, leave it alone.
    temp = escape_regex_literal(replace(temp, "\\" => "\\\\"))
    temp = replace(temp, placeholder => "([^/]+)")
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
# Parse N from a matched "{input[N]}" / "{output[N]}" substring (digits sit between `[` and `]`).
bracket_index(m::AbstractString) = parse(Int, m[findfirst('[', m)+1 : findlast(']', m)-1])

function fill_special(template::String, inputs::Vector{String}, outputs::Vector{String})
    s = replace(template, r"\{input\[(\d+)\]\}"  => m -> inputs[bracket_index(m)])
    s = replace(s,        r"\{output\[(\d+)\]\}" => m -> outputs[bracket_index(m)])
    s = replace(s, "{input}"  => join(inputs, " "))
    replace(s, "{output}" => join(outputs, " "))
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
    error("Rule `$name`: work function must return Cmd, String, Function, ShRun, or AbstractNode.")

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
    inputs = [substitute(p, wildcards) for p in rule.inputs]
    outputs = [substitute(p, wildcards) for p in rule.outputs]
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
# Rule checks and workflow explanations
#==============================================================================#

"""
    check(rule)
    check(rule, target)

Inspect a rule without running it. `check(rule)` shows the wildcard shape and
template placeholders; `check(rule, target)` tests one concrete target and shows
the wildcards, inputs, outputs, and rendered shell command when the rule work is
a string template.
"""
struct RuleCheck{W}
    rule::Rule{W}
    wildcards::Vector{String}
    placeholders::Vector{String}
end

"""
    RuleInstantiationCheck

Diagnostic returned by `check(rule, target)`. It records the concrete target,
wildcards inferred from the rule output pattern, substituted input/output paths,
and a rendered command preview for string-template rules.
"""
struct RuleInstantiationCheck{W}
    rule::Rule{W}
    target::String
    wildcards::Dict{String,String}
    inputs::Vector{String}
    outputs::Vector{String}
    command::String
    function_note::String
end

"""
    RuleExplanationStep

One resolved rule in a [`PlanExplanation`](@ref), including inferred wildcards,
concrete paths, rendered command preview, and dependency targets.
"""
struct RuleExplanationStep{W}
    rule::Rule{W}
    target::String
    wildcards::Dict{String,String}
    inputs::Vector{String}
    outputs::Vector{String}
    command::String
    function_note::String
    dependencies::Vector{String}
end

"""
    PlanExplanation

Diagnostic returned by `explain(rules, target)` or `explain(workflow; target)`.
It lists the rule chain needed to produce the requested target without running
the pipeline.
"""
struct PlanExplanation
    target::String
    steps::Vector{RuleExplanationStep}
end

rule_placeholders(rule::Rule{<:AbstractString}) =
    unique(String(m.match) for m in eachmatch(WILDCARD_RE, String(rule.work)))
rule_placeholders(::Rule) = String[]

check(rule::Rule) = RuleCheck(rule, pattern_wildcards(vcat(rule.inputs, rule.outputs)),
                              rule_placeholders(rule))

function check(rule::Rule, target::AbstractString)
    t = String(target)
    wildcards = rule_target_wildcards(rule, t)
    inputs = [substitute(p, wildcards) for p in rule.inputs]
    outputs = [substitute(p, wildcards) for p in rule.outputs]
    command, note = rule_work_preview(rule, inputs, outputs, wildcards)
    RuleInstantiationCheck(rule, t, wildcards, inputs, outputs, command, note)
end

@doc """
    check(rule)
    check(rule, target)

Inspect rule patterns without running work. `check(rule)` reports wildcard names,
input/output patterns, and placeholders used by string work templates.
`check(rule, target)` matches one concrete target and reports the inferred
wildcards, concrete inputs/outputs, and rendered shell command when available.
""" check

function rule_target_wildcards(rule::Rule, target::String)
    for op in rule.outputs
        wildcards = match_pattern(op, target)
        wildcards === nothing && continue
        return wildcards
    end
    error("Rule `$(rule.name)` does not produce target `$target`; output patterns: $(rule.outputs)")
end

function rule_work_preview(rule::Rule{<:AbstractString}, inputs::Vector{String},
                           outputs::Vector{String}, wildcards::AbstractDict)
    command = substitute(fill_special(String(rule.work), inputs, outputs), wildcards)
    command, ""
end
rule_work_preview(::Rule{<:Function}, ::Vector{String}, ::Vector{String}, ::AbstractDict) =
    ("", "function rule: would receive (inputs, outputs, wildcards)")
rule_work_preview(::Rule, ::Vector{String}, ::Vector{String}, ::AbstractDict) =
    ("", "rule work is not a string template")

explain(rules::AbstractVector{<:Rule}, target::AbstractString) =
    PlanExplanation(String(target), explain_steps(rules, String(target), Set{String}()))

@doc """
    explain(rules, target)
    explain(workflow; target)

Explain how a concrete target is resolved through one or more rules without
running the pipeline. The result shows each matched rule, inferred wildcards,
concrete inputs/outputs, rendered string command, and rule dependencies.
""" explain

function explain_steps(rules::AbstractVector{<:Rule}, target::String, visiting::Set{String})
    target in visiting && error("explain: cycle detected at `$target`")
    rule_match = find_rule(rules, target)
    rule_match === nothing && error("explain: no rule produces `$target`")
    rule, wildcards = rule_match
    inputs = [substitute(p, wildcards) for p in rule.inputs]
    outputs = [substitute(p, wildcards) for p in rule.outputs]
    command, note = rule_work_preview(rule, inputs, outputs, wildcards)
    push!(visiting, target)
    deps = RuleExplanationStep[]
    dependencies = String[]
    for inp in inputs
        find_rule(rules, inp) === nothing && continue
        push!(dependencies, inp)
        append!(deps, explain_steps(rules, inp, visiting))
    end
    delete!(visiting, target)
    push!(deps, RuleExplanationStep(rule, target, wildcards, inputs, outputs,
                                    command, note, dependencies))
    deps
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
    expand_each([String(template)], wildcards)
expand(templates::AbstractVector{<:AbstractString}, wildcards::NamedTuple) =
    expand_each([String(t) for t in templates], wildcards)

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

    # Keyword arguments get materialized as a NamedTuple whose field order is not
    # guaranteed to match the user-written order. Re-order by first appearance in
    # the templates so `expand("x/{a}_{b}")` is deterministic regardless of kwarg order.
    ks = unique(Symbol(m.captures[1]) for tmpl in templates for m in eachmatch(WILDCARD_RE, tmpl))

    # Validate: every wildcard referenced in templates must be provided.
    for k in ks
        haskey(wildcards, k) ||
            error("Substitution: wildcard `{$k}` has no value (have: $(collect(keys(wildcards))))")
    end

    vs = Any[getfield(wildcards, k) for k in ks]
    out = String[]
    sizehint!(out, length(templates) * prod(length, vs; init=1))
    # `Iterators.product` varies its *first* iterator fastest; we want the last
    # wildcard to vary fastest, matching the docs/tests here. Reverse for product,
    # then flip back before substitution.
    for tmpl in templates, combo_rev in Iterators.product(reverse(vs)...)
        combo = reverse(combo_rev)
        nt = (; (ks[i] => combo[i] for i in eachindex(ks))...)
        push!(out, substitute(tmpl, nt))
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
explain(wf::Workflow; target::AbstractString) = explain(wf.rules, String(target))

"""
    @workflow "name" begin
        @rule ...
        @targets ...
    end

Build a [`Workflow`](@ref) from rule and target entries. The macro is shallow:
entries are still ordinary [`@rule`](@ref) and [`@targets`](@ref) forms, collected
with `push!` into a `Workflow`.
"""
macro workflow(name, block)
    workflow_expr(name, block)
end

workflow_expr(name, block) = error("@workflow expects `@workflow \"name\" begin ... end`")
function workflow_expr(name, block::Expr)
    block.head === :block || error("@workflow expects `@workflow \"name\" begin ... end`")
    wf = gensym(:workflow)
    body = Any[:($wf = Workflow(name=$(esc(name))))]
    for stmt in block.args
        workflow_push_expr!(body, wf, stmt)
    end
    push!(body, wf)
    Expr(:block, body...)
end

workflow_push_expr!(::Vector{Any}, ::Symbol, ::LineNumberNode) = nothing
function workflow_push_expr!(body::Vector{Any}, wf::Symbol, stmt)
    workflow_entry_ok(stmt) ||
        error("@workflow entries must be @rule or @targets forms, got $(stmt)")
    push!(body, :(push!($wf, $(esc(stmt)))))
    nothing
end

workflow_entry_ok(::LineNumberNode) = true
workflow_entry_ok(x) = false
function workflow_entry_ok(expr::Expr)
    expr.head === :macrocall || return false
    expr.args[1] in (Symbol("@rule"), Symbol("@targets"))
end

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
