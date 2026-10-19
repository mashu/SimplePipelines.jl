# @step, @rule, @branch and @workflow: thin syntax over the constructors.

@nospecialize   # see runtime.jl

"""
    @step name = work
    @step name(inputs => outputs; threads=0, mem_mb=0) = work
    @step name(inputs) = work
    @step work

Create a named [`Step`](@ref). When a path contains a `{wildcard}`, the result is a
[`Rule`](@ref) (a step template) instead.

`work` is a shell command (`sh"..."`, a `String` template, a `Cmd`) or a function.
A function call on the right-hand side is deferred until the step runs
(`@step load = read_table("x.csv")`), except for command builders (`sh(...)`,
`string(...)`, `*`, `Cmd(...)`, `pipeline(...)`). A function that returns a command
has that command run.

```julia
@step hello = sh"echo hello"
@step sorted("data.txt" => "sorted.txt") = "sort {input} > {output}"
@step bam(["r1.fq", "r2.fq"] => "a.bam"; threads=8) = "bwa mem -t {threads} ref.fa {input} > {output}"
@step stats("a.bam" => "a.txt") = (bam, txt) -> write(txt, string(filesize(bam)))
@step align("raw/{s}.fq" => "bam/{s}.bam") = "bwa mem ref.fa {input} > {output}"   # a Rule
```
"""
macro step(ex)
    step_expr(ex)
end

step_expr(ex) = :(Step($(work_expr(ex))))
function step_expr(ex::Expr)
    ex.head === :(=) || return :(Step($(work_expr(ex))))
    step_lhs(ex.args[1], ex.args[2])
end

step_lhs(name::Symbol, rhs) = :(Step($(QuoteNode(name)), $(work_expr(rhs))))
function step_lhs(lhs::Expr, rhs)
    name, args, kwargs = split_call(lhs, "@step")
    inputs, outputs = io_exprs(args)
    :(step_or_rule($(QuoteNode(name)), $(work_expr(rhs)), $inputs, $outputs; $(kwargs...)))
end
step_lhs(lhs, rhs) = error("@step expects `name = work` or `name(inputs => outputs) = work`, got `$lhs`")

"""
    @rule name(inputs => outputs; threads=0, mem_mb=0) = work

Create a [`Rule`](@ref): a step template with `{wildcard}` patterns.

```julia
@rule trim("raw/{s}_R1.fq.gz" => "trim/{s}.fq.gz"; threads=4) =
    "fastp -w {threads} -i {input} -o {output}"
@rule merge(gather("bam/{s}.bam") => "all.vcf") = "bcftools mpileup -f ref.fa {input} | bcftools call -mv -o {output}"
```
"""
macro rule(ex)
    rule_expr(ex)
end

rule_expr(ex) = error("@rule expects `name(inputs => outputs) = work`, got `$ex`")
rule_expr(ex::Expr) = ex.head === :(=) ? rule_lhs(ex.args[1], ex.args[2]) :
    error("@rule expects `name(inputs => outputs) = work`, got `$ex`")
function rule_lhs(lhs::Expr, rhs)
    name, args, kwargs = split_call(lhs, "@rule")
    inputs, outputs = io_exprs(args)
    :(Rule($(QuoteNode(name)), $inputs, $outputs, $(esc(plain_rhs(rhs, "@rule"))); $(kwargs...)))
end
rule_lhs(lhs, rhs) = error("@rule expects `name(inputs => outputs) = work`, got `$lhs`")

"""Split `name(args...; kwargs...)` into the name, positional and keyword expressions."""
function split_call(lhs::Expr, macroname)
    lhs.head === :call || error("$macroname expects `name(inputs => outputs) = work`, got `$lhs`")
    args, kwargs = Any[], Any[]
    for a in lhs.args[2:end]
        call_part!(args, kwargs, a)
    end
    call_name(lhs.args[1], macroname), args, kwargs
end

call_name(name::Symbol, _) = name
call_name(x, macroname) = error("$macroname: the step name must be a plain name, got `$x`")

call_part!(args, kwargs, a) = push!(args, a)
function call_part!(args, kwargs, a::Expr)
    if a.head === :parameters
        foreach(p -> push!(kwargs, kw_expr(p)), a.args)
    elseif a.head === :kw
        push!(kwargs, kw_expr(a))
    else
        push!(args, a)
    end
end
kw_expr(k::Symbol) = Expr(:kw, k, esc(k))
kw_expr(a::Expr) = Expr(:kw, a.args[1], esc(a.args[2]))

io_exprs(args) = isempty(args) ? (:(String[]), :(String[])) :
    length(args) == 1 ? single_io(args[1]) : (Expr(:vect, map(esc, args)...), :(String[]))
single_io(a) = (esc(a), :(String[]))
single_io(a::Expr) = is_pair(a) ? (esc(a.args[2]), esc(a.args[3])) : (esc(a), :(String[]))
is_pair(a::Expr) = a.head === :call && length(a.args) == 3 && a.args[1] === :(=>)

# Calls that build commands run immediately; any other call is deferred to run time.
const EAGER_CALLS = (:sh, :Shell, :string, :*, :Cmd, :pipeline)

function work_expr(rhs)
    body = plain_rhs(rhs, "@step")
    deferred(body) ? :(() -> $(esc(body))) : esc(body)
end
deferred(x) = false
deferred(ex::Expr) = ex.head === :call && !(ex.args[1] in EAGER_CALLS)

is_line(::LineNumberNode) = true
is_line(_) = false

plain_rhs(x, _) = x
function plain_rhs(ex::Expr, macroname)
    ex.head === :tuple && error("$macroname: the work is a tuple. When passing several ",
        "$macroname expressions as arguments, wrap each one in parentheses.")
    ex.head === :block || return ex
    statements = filter(!is_line, ex.args)
    length(statements) == 1 ? plain_rhs(only(statements), macroname) : ex
end

"""
    step_or_rule(name, work, inputs, outputs; kwargs...)

Used by `@step`: concrete paths make a [`Step`](@ref), wildcard paths (or a
`gather`) make a [`Rule`](@ref).
"""
step_or_rule(name::Symbol, work, inputs, outputs; kwargs...) =
    templated(inputs) || templated(outputs) ? Rule(name, inputs, outputs, work; kwargs...) :
                                             Step(name, work, inputs, outputs; kwargs...)

templated(p::AbstractString) = has_wildcards(p)
templated(::Gather) = true
templated(ps::Union{AbstractVector,Tuple}) = any(templated, ps)
templated(_) = false

"""
    @branch condition if_true if_false

[`Branch`](@ref) with a condition written inline; it is evaluated when the branch is
reached. `@branch isfile("cache.bin") load compute` is
`Branch(() -> isfile("cache.bin"), load, compute)`.
"""
macro branch(condition, if_true, if_false)
    :(Branch(() -> $(esc(condition)), $(esc(if_true)), $(esc(if_false))))
end

"""
    @workflow "name" begin
        rules, steps and targets...
    end

Build a [`Workflow`](@ref) from a block. Every expression that evaluates to a
[`Rule`](@ref), a [`Step`](@ref) or a vector of them is added; a string or vector of
strings (that is not assigned to a variable) is a target. Assignments of other
values are allowed for local helpers.

```julia
wf = @workflow "rnaseq" begin
    samples = ["A", "B"]
    @rule trim("raw/{s}.fq.gz" => "trim/{s}.fq.gz") = "fastp -i {input} -o {output}"
    @rule align("trim/{s}.fq.gz" => "bam/{s}.bam") = "bwa mem ref.fa {input} > {output}"
    expand("bam/{s}.bam"; s=samples)
end
```
"""
macro workflow(name, block)
    workflow_expr(name, block)
end
macro workflow(block)
    workflow_expr("workflow", block)
end

function workflow_expr(name, block::Expr)
    block.head === :block || error("@workflow expects a begin ... end block")
    wf = gensym(:workflow)
    body = Any[:($wf = Workflow(; name=$(esc(name))))]
    for stmt in block.args
        is_line(stmt) && continue
        push!(body, :(workflow_entry!($wf, $(esc(stmt)), $(is_assignment(stmt)))))
    end
    push!(body, wf)
    Expr(:block, body...)
end
workflow_expr(name, block) = error("@workflow expects a begin ... end block")

is_assignment(ex::Expr) = ex.head === :(=)
is_assignment(_) = false

workflow_entry!(wf::Workflow, x::Union{Rule,Step,Workflow}, ::Bool) = push!(wf, x)
workflow_entry!(wf::Workflow, x::AbstractVector{<:Union{Rule,Step}}, ::Bool) = push!(wf, x)
workflow_entry!(wf::Workflow, x::Union{AbstractString,AbstractVector{<:AbstractString}}, assigned::Bool) =
    assigned ? wf : push!(wf, x)
workflow_entry!(wf::Workflow, ::Nothing, ::Bool) = wf
workflow_entry!(wf::Workflow, x, assigned::Bool) = assigned ? wf : throw(ArgumentError(
    "@workflow: cannot use $(summary(x)) as an entry; expected rules, steps or target paths"))

@specialize
