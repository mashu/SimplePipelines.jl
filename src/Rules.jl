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

abstract type AbstractRulePipeline end

"""
    Rule(name, inputs, outputs, work)

A reusable production recipe. `inputs` and `outputs` are vectors of pattern strings
(e.g. `"data/{sample}.fq"`). `work` is either:

- `String`: a shell command template using `{input}`, `{output}`, `{wildcard}` placeholders.
- `Function(inputs, outputs, wildcards)`: builds a node (Step / Cmd / function) at resolve time.

Use [`@rule`](@ref) for ergonomic construction, and [`resolve`](@ref) to turn a set
of rules + targets into a runnable DAG.
"""
struct Rule{W} <: AbstractRulePipeline
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

"""
    RuleChain(rules)

An input-driven chain of wildcard [`Rule`](@ref) templates. Build one with
`first_rule |> second_rule`; when run without explicit targets, the first rule
discovers existing inputs and the final rule's outputs become the concrete DAG
targets for each discovered wildcard assignment.
"""
struct RuleChain <: AbstractRulePipeline
    rules::Vector{Rule}
    function RuleChain(rules::AbstractVector{<:Rule})
        length(rules) >= 2 || error("RuleChain requires at least two rules.")
        new(collect(Rule, rules))
    end
end

|>(left::Rule, right::Rule) = RuleChain(Rule[left, right])
|>(left::RuleChain, right::Rule) = RuleChain(vcat(left.rules, Rule[right]))
|>(left::Rule, right::RuleChain) = RuleChain(vcat(Rule[left], right.rules))
|>(left::RuleChain, right::RuleChain) = RuleChain(vcat(left.rules, right.rules))

"""
    RuleValuePipe(source, sink)

A pipeline that first resolves and runs one or more wildcard rules, then pipes
the resulting output values into a normal node/function stage.
"""
struct RuleValuePipe{P<:AbstractRulePipeline,N<:AbstractNode} <: AbstractRulePipeline
    source::P
    sink::N
end

|>(left::AbstractRulePipeline, right::AbstractNode) = RuleValuePipe(left, right)
|>(left::AbstractRulePipeline, right::Base.AbstractCmd) = RuleValuePipe(left, node_operand(right))
|>(left::AbstractRulePipeline, right::Function) = RuleValuePipe(left, node_operand(right))

"""True when any declared path contains a `{wildcard}` placeholder."""
path_has_wildcards(path::String) = occursin(WILDCARD_RE, path)
paths_have_wildcards(paths::Vector{String}) = any(path_has_wildcards, paths)

"""
    step_or_rule(name, work, inputs, outputs)

Shared constructor used by `@step`. Concrete file dependencies create a
[`Step`](@ref); wildcard dependencies create a [`Rule`](@ref) template.
"""
function step_or_rule(name::Symbol, work, inputs, outputs)
    inputs_s = collect(String, inputs)
    outputs_s = collect(String, outputs)
    paths_have_wildcards(vcat(inputs_s, outputs_s)) &&
        return Rule(name, inputs_s, outputs_s, work)
    Step(name, work, inputs_s, outputs_s)
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
    work = function_rule_work(rule.name, rule.work, inputs, outputs, wildcards)
    compose_with_deps(build_step_from_work(rule.name, work, inputs, outputs), deps)
end

function instantiate(rule::Rule, inputs::Vector{String},
                     outputs::Vector{String}, wildcards::AbstractDict, deps::Vector{<:AbstractNode})
    compose_with_deps(build_step_from_work(rule.name, rule.work, inputs, outputs), deps)
end

function compose_with_deps(node::AbstractNode, deps::Vector{<:AbstractNode})
    isempty(deps) && return node
    length(deps) == 1 && return SameInputPipe(deps[1], node)
    SameInputPipe(Parallel(AbstractNode[deps...]), node)
end

build_step_from_work(name::Symbol, w::Cmd, inputs, outputs) = Step(name, w, inputs, outputs)
build_step_from_work(name::Symbol, w::String, inputs, outputs) = Step(name, sh(w), inputs, outputs)
build_step_from_work(name::Symbol, w::Function, inputs, outputs) = Step(name, w, inputs, outputs)
build_step_from_work(name::Symbol, w::ShRun, inputs, outputs) = Step(name, w, inputs, outputs)
build_step_from_work(name::Symbol, w::AbstractNode, _inputs, _outputs) = w
build_step_from_work(name::Symbol, w, inputs, outputs) =
    error("Rule `$name`: work function must return Cmd, String, Function, ShRun, or AbstractNode.")

function function_rule_work(name::Symbol, f::Function, inputs::Vector{String},
                            outputs::Vector{String}, wildcards::AbstractDict)
    length(inputs) == 1 && length(outputs) == 1 &&
        applicable(f, inputs[1], outputs[1]) &&
        return input -> f(input, outputs[1])
    length(inputs) == 1 && isempty(outputs) &&
        applicable(f, inputs[1]) &&
        return input -> f(input)
    work = f(inputs, outputs, wildcards)
    build_step_from_work(name, work, inputs, outputs)
end

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
resolve(rules::AbstractVector{<:Rule}, target::AbstractString; satisfy_existing::Bool=true) =
    resolve(rules, [String(target)]; satisfy_existing=satisfy_existing)
function resolve(rules::AbstractVector{<:Rule}, targets::AbstractVector{<:AbstractString};
                 satisfy_existing::Bool=true)
    cache = Dict{String, AbstractNode}()
    visited = Set{String}()
    nodes = AbstractNode[]
    for t in targets
        n = resolve_target!(rules, String(t), cache, visited, satisfy_existing)
        n === nothing && continue
        push!(nodes, n)
    end
    isempty(nodes) && return NoWork()
    length(nodes) == 1 && return nodes[1]
    Parallel(nodes)   # Vector{AbstractNode}
end

# Returns AbstractNode (the node producing target) or nothing (target already on disk).
function resolve_target!(rules::AbstractVector{<:Rule}, target::String,
                         cache::Dict{String,AbstractNode}, visited::Set{String},
                         satisfy_existing::Bool)
    haskey(cache, target) && return cache[target]
    target in visited && error("resolve: cycle detected at `$target`")
    satisfy_existing && isfile(target) && return nothing
    rule_match = find_rule(rules, target)
    if rule_match === nothing
        isfile(target) && return nothing
        error("resolve: no rule produces `$target` and the file does not exist")
    end
    rule, wildcards = rule_match
    inputs = [substitute(p, wildcards) for p in rule.inputs]
    outputs = [substitute(p, wildcards) for p in rule.outputs]
    push!(visited, target)
    deps = AbstractNode[]
    for inp in inputs
        sub = resolve_target!(rules, inp, cache, visited, satisfy_existing)
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

"""
    plan(rule; wildcards_filter=nothing)

Build a runnable plan for a single wildcard template by discovering existing
input files. Use `run(rule; targets=[...])` when the desired outputs, rather
than existing inputs, should drive resolution.

If `wildcards_filter` is a function `f(wildcards) -> Bool`, only wildcard
assignments for which `f` returns `true` are included (e.g. skip high replicate
indices after splitting the stem into `{donor}_{mid}_{rep}`).
"""
function plan(rule::Rule; wildcards_filter=nothing)
    nodes = discover_rule_nodes(rule; wildcards_filter=wildcards_filter)
    isempty(nodes) &&
        error("Rule `$(rule.name)` found no matching input files. Pass `targets=...` to run, or use a Workflow with @targets.")
    length(nodes) == 1 && return nodes[1]
    Parallel(nodes)
end

"""
    plan(chain::RuleChain; wildcards_filter=nothing)

Build a concrete DAG for a piped wildcard rule chain. Discovery starts from the
first rule's input pattern; for every discovered wildcard assignment, the final
rule's output pattern(s) become targets resolved through all rules in the chain.

Unlike explicit output-driven [`resolve`](@ref), chain planning does not treat
existing produced outputs as already satisfied; it still builds the DAG and lets
normal freshness checks (or `force=true`) decide what actually runs.
"""
function plan(chain::RuleChain; wildcards_filter=nothing)
    first_rule = first(chain.rules)
    final_rule = last(chain.rules)
    isempty(final_rule.outputs) &&
        error("RuleChain final rule `$(final_rule.name)` has no outputs to use as targets.")
    matches = rule_runnable_wildcards(first_rule; wildcards_filter=wildcards_filter)
    isempty(matches) &&
        error("RuleChain `$(rule_chain_name(chain))` found no matching input files.")
    targets = String[]
    for wc in matches
        append!(targets, (substitute(output, wc) for output in final_rule.outputs))
    end
    resolve(chain.rules, targets; satisfy_existing=false)
end

function plan(pipe::RuleValuePipe; wildcards_filter=nothing)
    plan(pipe.source; wildcards_filter=wildcards_filter) |> pipe.sink
end

rule_plan(rule::Rule, ::Nothing; wildcards_filter=nothing) =
    plan(rule; wildcards_filter=wildcards_filter)
rule_plan(rule::Rule, target::AbstractString; wildcards_filter=nothing) =
    resolve([rule], target)
rule_plan(rule::Rule, targets::AbstractVector{<:AbstractString}; wildcards_filter=nothing) =
    resolve([rule], targets)

@doc """
    run(rule::Rule; targets=nothing, wildcards_filter=nothing, kwargs...)

Run a wildcard [`Rule`](@ref) template. With `targets=nothing` (default), inputs are
discovered from disk; pass `targets` as a concrete path or vector of paths to
resolve from desired outputs instead.

`wildcards_filter` may be a function `f(wildcards) -> Bool`. When discovering inputs,
assignments for which `f(wildcards)` is false are skipped (e.g. replicate index
`parse(Int, wildcards[\"rep\"]) < 100`). Ignored when `targets` is passed (output-driven
[`resolve`](@ref)).

Remaining keyword arguments are forwarded to [`run(::Pipeline)`](@ref) (e.g. `jobs`,
`verbose`, `force`).
"""
function Base.run(rule::Rule; targets=nothing, wildcards_filter=nothing, kwargs...)
    run(
        Pipeline(rule_plan(rule, targets; wildcards_filter=wildcards_filter); name=string(rule.name));
        kwargs...,
    )
end

rule_chain_name(chain::RuleChain) = join((string(rule.name) for rule in chain.rules), " |> ")

rule_chain_plan(chain::RuleChain, ::Nothing; wildcards_filter=nothing) =
    plan(chain; wildcards_filter=wildcards_filter)
rule_chain_plan(chain::RuleChain, target::AbstractString; wildcards_filter=nothing) =
    resolve(chain.rules, target)
rule_chain_plan(chain::RuleChain, targets::AbstractVector{<:AbstractString}; wildcards_filter=nothing) =
    resolve(chain.rules, targets)

@doc """
    run(chain::RuleChain; targets=nothing, wildcards_filter=nothing, kwargs...)

Run a piped wildcard rule chain built with `rule1 |> rule2`. With `targets=nothing`
(default), the first rule discovers inputs and the final rule supplies concrete
targets for a per-wildcard DAG. Pass explicit final targets to use output-driven
resolution instead.

`wildcards_filter` applies to input-driven discovery, matching [`run(::Rule)`](@ref).
Remaining keyword arguments are forwarded to [`run(::Pipeline)`](@ref).
""" function Base.run(chain::RuleChain; targets=nothing, wildcards_filter=nothing, kwargs...)
    run(
        Pipeline(
            rule_chain_plan(chain, targets; wildcards_filter=wildcards_filter);
            name=rule_chain_name(chain),
        );
        kwargs...,
    )
end

rule_pipeline_name(rule::Rule) = string(rule.name)
rule_pipeline_name(chain::RuleChain) = rule_chain_name(chain)
rule_pipeline_name(pipe::RuleValuePipe) = string(rule_pipeline_name(pipe.source), " |> ", pipe.sink)

rule_pipeline_plan(pipe::RuleValuePipe, ::Nothing; wildcards_filter=nothing) =
    plan(pipe; wildcards_filter=wildcards_filter)
rule_pipeline_plan(pipe::RuleValuePipe, target::AbstractString; wildcards_filter=nothing) =
    resolve(rules_in(pipe.source), target) |> pipe.sink
rule_pipeline_plan(pipe::RuleValuePipe, targets::AbstractVector{<:AbstractString}; wildcards_filter=nothing) =
    resolve(rules_in(pipe.source), targets) |> pipe.sink

rules_in(rule::Rule) = Rule[rule]
rules_in(chain::RuleChain) = chain.rules

@doc """
    run(pipe::RuleValuePipe; targets=nothing, wildcards_filter=nothing, kwargs...)

Run a wildcard rule pipeline followed by a normal function/node stage. With
`targets=nothing`, rule discovery is input-driven; the sink receives the usual
`|>` value from the resolved rule DAG (one output path for one branch, or a vector
of branch outputs for many).
""" function Base.run(pipe::RuleValuePipe; targets=nothing, wildcards_filter=nothing, kwargs...)
    run(
        Pipeline(
            rule_pipeline_plan(pipe, targets; wildcards_filter=wildcards_filter);
            name=rule_pipeline_name(pipe),
        );
        kwargs...,
    )
end

function discover_rule_nodes(rule::Rule; wildcards_filter=nothing)
    isempty(rule.inputs) &&
        error("Rule `$(rule.name)` has no input pattern to discover. Pass `targets=...` to run, or use a Workflow with @targets.")
    paths_have_wildcards(rule.inputs) ||
        error("Rule `$(rule.name)` has no wildcard input pattern to discover. Pass `targets=...` to run.")
    first_input = first_discoverable_input(rule)
    wildcards = discover_wildcards(first_input)
    nodes = AbstractNode[]
    seen_outputs = Set{String}()
    for wc in wildcards
        wildcards_filter !== nothing && !wildcards_filter(wc) && continue
        inputs = [substitute(p, wc) for p in rule.inputs]
        all(isfile, inputs) || continue
        outputs = [substitute(p, wc) for p in rule.outputs]
        key = join(outputs, "\0")
        key in seen_outputs && continue
        push!(seen_outputs, key)
        push!(nodes, instantiate(rule, inputs, outputs, wc, AbstractNode[]))
    end
    nodes
end

function first_discoverable_input(rule::Rule)
    for input in rule.inputs
        path_has_wildcards(input) && return input
    end
    error("Rule `$(rule.name)` has no wildcard input pattern to discover.")
end

function discover_wildcards(pattern::String)
    matches = Dict{String,String}[]
    for path in discover_candidate_paths(pattern)
        wc = match_pattern(pattern, path)
        wc === nothing || push!(matches, wc)
    end
    matches
end

function discover_candidate_paths(pattern::String)
    root = discover_root(pattern)
    isdir(root) || return String[]
    paths = String[]
    for (dir, _, files) in walkdir(root)
        for file in files
            push!(paths, relpath(joinpath(dir, file), "."))
        end
    end
    sort!(paths)
end

function discover_root(pattern::String)
    m = match(WILDCARD_RE, pattern)
    m === nothing && return dirname(pattern)
    prefix = pattern[begin:m.offset - 1]
    dir = dirname(prefix)
    isempty(dir) && return "."
    dir
end

#==============================================================================#
# Rule checks and workflow explanations
#==============================================================================#

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
    RuleCheck

Summary of a wildcard template from [`check`](@ref)`(rule; ...)`: pattern shape,
optional [`RuleInstantiationCheck`](@ref) previews from files on disk, and a
`discovery_note` when previews are not available.
"""
struct RuleCheck{W}
    rule::Rule{W}
    wildcards::Vector{String}
    placeholders::Vector{String}
    instantiations::Vector{RuleInstantiationCheck{W}}
    discovery_note::String
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

"""Wildcards dicts for which every declared input path exists on disk (input-driven run)."""
function rule_runnable_wildcards(rule::Rule; wildcards_filter=nothing)
    out = Dict{String,String}[]
    isempty(rule.inputs) && return out
    paths_have_wildcards(rule.inputs) || return out
    fin = first_discoverable_input(rule)
    for wc in discover_wildcards(fin)
        wildcards_filter !== nothing && !wildcards_filter(wc) && continue
        ins = [substitute(p, wc) for p in rule.inputs]
        all(isfile, ins) || continue
        push!(out, wc)
    end
    out
end

function rule_instantiation(rule::Rule{W}, wildcards::AbstractDict, target::AbstractString) where {W}
    t = String(target)
    inputs = [substitute(p, wildcards) for p in rule.inputs]
    outputs = [substitute(p, wildcards) for p in rule.outputs]
    command, note = rule_work_preview(rule, inputs, outputs, wildcards)
    RuleInstantiationCheck(rule, t, Dict{String,String}(wildcards), inputs, outputs, command, note)
end

@doc """
    check(rule; limit=20, wildcards_filter=nothing)
    check(rule, target)

Inspect a wildcard template without running it.

`check(rule)` reports wildcard names, input/output patterns, placeholders for string
work, and — when the rule has wildcard **input** patterns — up to `limit` concrete
[`RuleInstantiationCheck`](@ref) previews inferred from files already on disk
(same selection as input-driven [`run`](@ref)`(rule)`). If inputs are not
discoverable, read `discovery_note` on the returned [`RuleCheck`](@ref); it tells
you to use `check(rule, concrete_output_path)` instead.

`check(rule, target)` matches one concrete output path and returns a single
[`RuleInstantiationCheck`](@ref).

Use `check(rule; limit=N)` when many samples match and you want more previews.

Use `check(rule; wildcards_filter=f)` with the same `f` as [`run`](@ref)`(rule;
wildcards_filter=f)` so previews match what the runner would schedule.
""" function check(rule::Rule{W}; limit::Int=20, wildcards_filter=nothing) where {W}
    shape = pattern_wildcards(vcat(rule.inputs, rule.outputs))
    ph = rule_placeholders(rule)
    matches = rule_runnable_wildcards(rule; wildcards_filter=wildcards_filter)
    inst = RuleInstantiationCheck{W}[]
    discovery_note = ""
    if isempty(rule.inputs)
        discovery_note = "No input patterns: this template cannot be auto-previewed from disk. Use check(rule, \"path/to/output\") with a concrete output this rule produces."
    elseif !paths_have_wildcards(rule.inputs)
        discovery_note = "Inputs have no wildcards: use check(rule, concrete_output_target) to preview substitution from an output path."
    elseif isempty(matches)
        discovery_note = "No runnable matches found (no files on disk satisfied all inputs under the discovered wildcard combinations). Use check(rule, concrete_output_target) once you know one output path, or create the expected input files first."
    else
        ntotal = length(matches)
        for (i, wc) in enumerate(matches)
            i > limit && break
            outs = [substitute(p, wc) for p in rule.outputs]
            isempty(outs) && continue
            push!(inst, rule_instantiation(rule, wc, outs[1]))
        end
        if ntotal > limit
            discovery_note = "$ntotal runnable matches on disk; showing the first $limit preview(s). Use check(rule; limit=N) for more."
        end
    end
    RuleCheck(rule, shape, ph, inst, discovery_note)
end

@doc """
    check(rule::Rule, target::AbstractString) -> RuleInstantiationCheck

Preview one concrete **output** path `target` (must match an output pattern).
See the `check(rule; limit=...)` docstring for full-disk previews.
""" function check(rule::Rule, target::AbstractString)
    t = String(target)
    wildcards = rule_target_wildcards(rule, t)
    rule_instantiation(rule, wildcards, t)
end

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
    ("", "function template: would receive concrete paths when run")
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
        @step ...
        @rule ...
        @targets ...
    end

Build a [`Workflow`](@ref) from rule and target entries. The macro is shallow:
entries are still ordinary wildcard [`@step`](@ref), [`@rule`](@ref), and
[`@targets`](@ref) forms, collected with `push!` into a `Workflow`.
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
        error("@workflow entries must be wildcard @step, @rule, or @targets forms, got $(stmt)")
    push!(body, :(push!($wf, $(esc(stmt)))))
    nothing
end

workflow_entry_ok(::LineNumberNode) = true
workflow_entry_ok(x) = false
function workflow_entry_ok(expr::Expr)
    expr.head === :macrocall || return false
    expr.args[1] in (Symbol("@step"), Symbol("@rule"), Symbol("@targets"))
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
