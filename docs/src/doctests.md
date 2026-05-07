# Doctests

Runnable [`jldoctest`](https://documenter.juliadocs.org/stable/man/doctests/) blocks checked by
[`Documenter.jl`](https://github.com/JuliaDocs/Documenter.jl) on every full doc build and from
`Pkg.test()` via `makedocs(doctest = :only)` (see `docs/make.jl` and the `DOCUMENTER_DOCTEST` environment variable).

```@meta
CurrentModule = SimplePipelines
DocTestSetup = quote
    using SimplePipelines
    SimplePipelines.clear_state!()
end
```

## Shell sequence

```jldoctest
julia> using SimplePipelines

julia> r = run(sh"echo hi" >> sh"echo second", verbose=false);

julia> length(r)
2

julia> all(x -> x.success, r)
true
```

## Named steps and parallel

```jldoctest
julia> using SimplePipelines

julia> a = @step a = sh"echo A";

julia> b = @step b = sh"echo B";

julia> r = run(a & b, verbose=false);

julia> length(r)
2

julia> all(x -> x.success, r)
true
```

## Fallback

```jldoctest
julia> using SimplePipelines

julia> r = run((@step bad = sh"false") | (@step ok = sh"echo ok"), verbose=false);

julia> r[end].success
true
```

## `resolve` returns `NoWork` when the target already exists

```jldoctest
julia> using SimplePipelines

julia> nw = mktempdir() do dir
           cd(dir) do
               write("done.txt", "ok")
               r = @rule a([] => "done.txt") = "echo y > {output}"
               resolve([r], ["done.txt"])
           end
       end;

julia> nw isa NoWork
true
```
