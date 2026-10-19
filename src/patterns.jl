# Wildcard patterns such as "raw/{sample}_R1.fq.gz". A wildcard matches one path
# segment (anything but "/"). Patterns drive rule inputs/outputs, ForEach file
# discovery, `expand`, `discover` and `gather`.

const WILDCARD = r"\{(\w+)\}"
const REGEX_META = r"[\\^$.|?*+()\[\]{}]"

regex_escape(s::AbstractString) = replace(s, REGEX_META => m -> "\\" * m)

"""
A compiled wildcard pattern. Regex group `i` captures wildcard `names[i]`; a name
may repeat, in which case every occurrence must capture the same value.
"""
struct Pattern
    text::String
    regex::Regex
    names::Vector{String}
end

function Pattern(text::AbstractString)
    t = String(text)
    names = String[]
    io = IOBuffer()
    print(io, '^')
    pos = 1
    for m in eachmatch(WILDCARD, t)
        print(io, regex_escape(SubString(t, pos, prevind(t, m.offset))), "([^/]+)")
        push!(names, m.captures[1])
        pos = m.offset + ncodeunits(m.match)
    end
    print(io, regex_escape(SubString(t, pos)), '$')
    Pattern(t, Regex(String(take!(io))), names)
end

wildcard_names(p::Pattern) = unique(p.names)
has_wildcards(p::Pattern) = !isempty(p.names)
has_wildcards(s::AbstractString) = occursin(WILDCARD, s)

"""Wildcard values when `path` matches `p`, otherwise `nothing`."""
function match_path(p::Pattern, path::AbstractString)
    m = match(p.regex, path)
    m === nothing && return nothing
    values = Dict{String,String}()
    for (name, capture) in zip(p.names, m.captures)
        value = String(something(capture))
        get!(values, name, value) == value || return nothing
    end
    values
end

"""Replace every `{name}` in `text` by `values[name]`; error when a value is missing."""
function substitute(text::AbstractString, values::AbstractDict)
    replace(text, WILDCARD => function (m)
        name = SubString(m, 2, lastindex(m) - 1)
        haskey(values, name) || error("wildcard {$name} in `$text` has no value; known: ",
                                      join(sort!(collect(keys(values))), ", "))
        values[name]
    end)
end
substitute(p::Pattern, values::AbstractDict) = substitute(p.text, values)

"""Substitute the wildcards that have a value and keep the others as `{name}`."""
bind_known(text::AbstractString, values::AbstractDict) =
    replace(text, WILDCARD => m -> get(values, SubString(m, 2, lastindex(m) - 1), m))

#==============================================================================#
# Wildcards: what users see in `discover` results and Workflow filters
#==============================================================================#

"""
    Wildcards

The wildcard values of one match, as returned by [`discover`](@ref) and passed to a
Workflow `filter`. Read values as properties or by name:

```julia
w.sample       # "A"
w["sample"]    # "A"
```
"""
struct Wildcards
    values::Dict{String,String}
end

Base.getproperty(w::Wildcards, name::Symbol) = getfield(w, :values)[String(name)]
Base.getindex(w::Wildcards, name::Union{AbstractString,Symbol}) = getfield(w, :values)[String(name)]
Base.haskey(w::Wildcards, name::Union{AbstractString,Symbol}) = haskey(getfield(w, :values), String(name))
Base.propertynames(w::Wildcards) = Tuple(Symbol.(sort!(collect(keys(getfield(w, :values))))))
Base.Dict(w::Wildcards) = copy(getfield(w, :values))
Base.:(==)(a::Wildcards, b::Wildcards) = getfield(a, :values) == getfield(b, :values)
Base.hash(w::Wildcards, h::UInt) = hash(getfield(w, :values), hash(:Wildcards, h))

#==============================================================================#
# Globbing: walk only the directories the pattern can match
#==============================================================================#

"""Existing paths matching `p` with their wildcard values, sorted by path."""
function glob(p::Pattern)
    absolute = startswith(p.text, '/')
    segments = [Pattern(s) for s in split(p.text, '/') if !isempty(s)]
    found = Tuple{String,Dict{String,String}}[]
    isempty(segments) || glob_walk!(found, absolute ? "/" : "", segments, 1, Dict{String,String}())
    sort!(found; by=first)
end

function glob_walk!(found, dir::String, segments::Vector{Pattern}, i::Int, values::Dict{String,String})
    segment = segments[i]
    final = i == length(segments)
    if !has_wildcards(segment)
        glob_visit!(found, child_path(dir, segment.text), segments, i, final, values)
        return found
    end
    listing = isempty(dir) ? "." : dir
    isdir(listing) || return found
    for entry in readdir(listing)
        startswith(entry, '.') && !startswith(segment.text, '.') && continue
        captured = match_path(segment, entry)
        captured === nothing && continue
        merged = merge_consistent(values, captured)
        merged === nothing && continue
        glob_visit!(found, child_path(dir, entry), segments, i, final, merged)
    end
    found
end

function glob_visit!(found, path::String, segments, i, final::Bool, values)
    if final
        ispath(path) && push!(found, (path, values))
    elseif isdir(path)
        glob_walk!(found, path, segments, i + 1, values)
    end
    found
end

child_path(dir::String, name::AbstractString) = isempty(dir) ? String(name) : joinpath(dir, name)

function merge_consistent(a::Dict{String,String}, b::Dict{String,String})
    for (k, v) in b
        get(a, k, v) == v || return nothing
    end
    merge(a, b)
end

"""
    discover(pattern) -> Vector{Wildcards}

Find existing files matching `pattern` and return their wildcard values, sorted by
path. Use it to check that a pattern matches what you expect, or to build sample
lists:

```julia
discover("raw/{sample}_R1.fq.gz")          # [Wildcards(sample="A"), ...]
samples = [w.sample for w in discover("raw/{sample}_R1.fq.gz")]
```
"""
discover(pattern::AbstractString) = [Wildcards(values) for (_, values) in glob(Pattern(pattern))]

#==============================================================================#
# expand: Cartesian product of wildcard values
#==============================================================================#

"""
    expand(template; wildcard=values, ...) -> Vector{String}
    expand(templates::AbstractVector; wildcard=values, ...) -> Vector{String}

Fill a pattern with every combination of the given values. The last wildcard varies
fastest; with several templates, all combinations of the first come before the
second. A single string or number counts as one value.

```julia
expand("bam/{sample}.bam"; sample=["A", "B"])        # ["bam/A.bam", "bam/B.bam"]
expand("{s}_R{r}.fq"; s=["A", "B"], r=1:2)           # ["A_R1.fq", "A_R2.fq", "B_R1.fq", "B_R2.fq"]
expand(["bam/{s}.bam", "qc/{s}.html"]; s="A")       # ["bam/A.bam", "qc/A.html"]
```
"""
expand(template::AbstractString; values...) = expand([String(template)]; values...)

function expand(templates::AbstractVector{<:AbstractString}; values...)
    names = unique(String(m.captures[1]) for t in templates for m in eachmatch(WILDCARD, t))
    given = Dict(String(k) => expansion_values(v) for (k, v) in values)
    for name in names
        haskey(given, name) || error("expand: no values given for {$name}")
    end
    for name in keys(given)
        name in names || error("expand: {$name} does not appear in ", join(templates, ", "))
    end
    lists = [given[n] for n in names]
    out = String[]
    for t in templates, combo in Iterators.product(reverse(lists)...)
        push!(out, substitute(t, Dict(zip(names, reverse(collect(combo))))))
    end
    out
end

expand(template, values::NamedTuple) = expand(template; values...)

expansion_values(v::Union{AbstractString,Number,Symbol}) = [string(v)]
expansion_values(v) = [string(x) for x in v]

#==============================================================================#
# Shell templates: {input} {output} {input[i]} {output[i]} {threads} {mem_mb} {wildcard}
#==============================================================================#

# `{{name}}` is an escaped literal `{name}`; `${VAR}` belongs to the shell.
const TEMPLATE_TOKEN = r"\{\{(\w+(?:\[\d+\])?)\}\}|(?<!\$)\{(\w+)(?:\[(\d+)\])?\}"
const RESERVED_NAMES = ("input", "output", "threads", "mem_mb")

"""A template placeholder that cannot be filled."""
struct TemplateError <: Exception
    message::String
end
Base.showerror(io::IO, e::TemplateError) = print(io, e.message)
template_error(parts...) = throw(TemplateError(string(parts...)))

"""Paths and wildcard values available to a template: one input group per declared input."""
struct TemplateContext
    inputs::Vector{Vector{String}}
    outputs::Vector{String}
    wildcards::Dict{String,String}
end
TemplateContext() = TemplateContext(Vector{String}[], String[], Dict{String,String}())

"""
Fill the placeholders of a template. `{threads}` and `{mem_mb}` come from the resources the
step was granted when it starts (`mem_mb = 0`: none declared, so `{mem_mb}` is an error).
Paths and wildcard values are shell-quoted when needed.
"""
render(template::AbstractString, ctx::TemplateContext, threads::Int, mem_mb::Int) =
    replace(template, TEMPLATE_TOKEN => token ->
        render_token(match(TEMPLATE_TOKEN, token), ctx, threads, mem_mb, template))

"""Render with stand-in resources, to reject unknown placeholders when a step is defined."""
validate_template(template::AbstractString, ctx::TemplateContext) = (render(template, ctx, 1, 1); nothing)

"""The template as displayed before it runs: paths filled in, `{threads}` and `{mem_mb}` kept."""
preview(template::AbstractString, ctx::TemplateContext) = render(template, ctx, -1, -1)

function render_token(m::RegexMatch, ctx::TemplateContext, threads::Int, mem_mb::Int, template)
    escaped = m.captures[1]
    escaped === nothing || return string("{", escaped, "}")
    placeholder(String(m.captures[2]), m.captures[3], ctx, threads, mem_mb, template)
end

function placeholder(name::String, index, ctx::TemplateContext, threads::Int, mem_mb::Int, template)
    name == "input" && return paths_text(ctx.inputs, index, name, template)
    name == "output" && return paths_text([[o] for o in ctx.outputs], index, name, template)
    index === nothing || template_error("only {input[i]} and {output[i]} take an index; got {$name[$index]} in: $template")
    name == "threads" && return threads < 0 ? "{threads}" : string(threads)
    name == "mem_mb" && return mem_mb < 0 ? "{mem_mb}" : mem_mb > 0 ? string(mem_mb) : template_error(
        "uses {mem_mb} but declares no mem_mb; declare mem_mb on the step or with with_resources")
    haskey(ctx.wildcards, name) && return shell_quote(ctx.wildcards[name])
    known = join(("{$n}" for n in [collect(RESERVED_NAMES); sort!(collect(keys(ctx.wildcards)))]), ", ")
    template_error("unknown placeholder {$name} in: $template\nAvailable: $known. ",
                   "Write {{$name}} for a literal {$name}.")
end

paths_text(groups, ::Nothing, name, template) = join((shell_quote(p) for g in groups for p in g), " ")

function paths_text(groups, index::AbstractString, name, template)
    i = parse(Int, index)
    1 <= i <= length(groups) || template_error("{$name[$i]} is out of range: there are $(length(groups)) $(name)s in: $template")
    join((shell_quote(p) for p in groups[i]), " ")
end
