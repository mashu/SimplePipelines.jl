# ForEach/Map constructors, pattern matching, as_node.
# Required before run_node(ForEach) / run_node(Map).

"""
    Map(f, items)
    Map(f)

Lazy parallel node: applies `f` to each item **when you call `run(pipeline)`**, not when the pipeline is built.
`Map(f)` returns a function `items -> Map(f, items)`.

# Examples
```julia
Map([1, 2, 3]) do n
    @step step_n = `echo n=\$n`
end
```
"""
function Map(f::Function, items)
    vec = collect(items)
    isempty(vec) && error("Map requires at least one item")
    Map(f, vec)
end
Map(f::F, items::Vector{T}) where {F<:Function, T} = (isempty(items) && error("Map requires at least one item"); Map{F, T}(f, items))
Map(f::Function) = items -> Map(f, items)

as_node(n::AbstractNode) = n
as_node(x) = Step(x)

function for_each_regex(pattern::String)::Regex
    wildcard_rx = r"\{(\w+)\}"
    parts = split(pattern, "/")
    first_wild = findfirst(p -> contains(p, "{"), parts)
    first_wild === nothing && error("ForEach pattern must contain {wildcard}: $pattern")
    pattern_suffix = join(parts[first_wild:end], "/")
    placeholder = "\x00WILD\x00"
    temp = replace(pattern_suffix, wildcard_rx => placeholder)
    for c in ".+^*?\$()[]|"
        temp = replace(temp, string(c) => "\\" * c)
    end
    Regex("^" * replace(temp, placeholder => "([^/]+)") * "\$")
end

"""
    ForEach(pattern) do wildcards...
        # return a Step or node
    end
    fe(pattern) do wildcards... end   # short alias

Discover files matching pattern with `{name}` placeholders; create one parallel branch per match.
The block is run once per match **when you call `run(pipeline)`**, not when the pipeline is built.
It must return a Step or other node (e.g. `@step name = sh\"cmd\"`).
Concurrency is set at run time: `run(pipeline; jobs=8)` (default 8; use `jobs=0` for unbounded).
"""
function ForEach(f::Function, pattern::String)
    wildcard_rx = r"\{(\w+)\}"
    contains(pattern, wildcard_rx) || error("ForEach pattern must contain {wildcard}: $pattern")
    ForEach{typeof(f)}(f, pattern)
end
ForEach(pattern::String) = f -> ForEach(f, pattern)

const fe = ForEach
@doc "Short alias for [`ForEach`](@ref). Use `fe(\"pattern\") do x ... end`." fe

function find_matches(pattern::String, regex::Regex)
    parts = split(pattern, "/")
    first_wild = findfirst(p -> contains(p, "{"), parts)
    first_wild === nothing && error("Pattern must contain {wildcard}")
    
    base = first_wild == 1 ? "." : joinpath(parts[1:first_wild-1]...)
    isdir(base) || return Vector{Vector{String}}()
    
    matches = Vector{Vector{String}}()
    scan_dir!(matches, base, base, parts, first_wild, regex)
    matches
end

function scan_dir!(matches, base, dir, parts, idx, regex)
    idx > length(parts) && return
    is_last = idx == length(parts)
    
    for entry in readdir(dir)
        path = joinpath(dir, entry)
        if is_last && isfile(path)
            rel = replace(relpath(path, base), "\\" => "/")
            m = match(regex, rel)
            m !== nothing && push!(matches, collect(String, m.captures))
        elseif !is_last && isdir(path)
            scan_dir!(matches, base, path, parts, idx + 1, regex)
        end
    end
end
