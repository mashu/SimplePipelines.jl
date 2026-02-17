# ForEach constructors (pattern or collection), pattern matching, as_node.
# Required before run_node(ForEach).

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
    ForEach(pattern) do wildcards... end
    ForEach(items) do item ... end
    fe(...)   # short alias

Single lazy node with two behaviors (multiple dispatch on second argument):

- **String (file pattern)**: Discover files matching pattern with `{name}` placeholders; create one parallel branch per match. The block receives the captured wildcard(s). Pattern must contain `{wildcard}`.
- **Collection**: Apply the block to each item (one parallel branch per element). Use any vector or iterable (e.g. `eachrow(df)`, `1:10`). The block receives the item and must return a Step or node.

The block is run **when you call `run(pipeline)`**, not when the pipeline is built. Concurrency: `run(pipeline; jobs=8)` (default 8; `jobs=0` for unbounded).

# Examples
```julia
# File discovery
ForEach(\"data/{id}.csv\") do id
    @step process = sh\"process data/\$id.csv\"
end

# Over a collection
ForEach([1, 2, 3]) do n
    @step step_n = `echo n=\$n`
end
ForEach(eachrow(df)) do row
    @step p = sh\"echo \$(row.id)\"
end
```
"""
function ForEach(f::Function, pattern::String)
    wildcard_rx = r"\{(\w+)\}"
    contains(pattern, wildcard_rx) || error("ForEach pattern must contain {wildcard}: $pattern")
    ForEach{typeof(f), String}(f, pattern)
end
ForEach(pattern::String) = f -> ForEach(f, pattern)

function ForEach(f::Function, items)
    vec = collect(items)
    isempty(vec) && error("ForEach requires at least one item")
    ForEach{typeof(f), typeof(vec)}(f, vec)
end
ForEach(f::Function) = items -> ForEach(f, items)

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
