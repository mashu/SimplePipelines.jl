# Doctests

These examples are run by Documenter on every documentation build, so they are
guaranteed to match the package.

```@meta
CurrentModule = SimplePipelines
DocTestSetup = quote
    using SimplePipelines
    docs_dir = pwd()
    cd(mktempdir())
end
DocTestTeardown = :(cd(docs_dir))
```

## Values flow with `>>`

```jldoctest
julia> half = @step half = () -> 21;

julia> double = @step double = x -> 2x;

julia> run(half >> double; verbose=false).value
42

julia> run((sh"echo a" & sh"echo b") >> (@step n = v -> length(v)); verbose=false).value
2
```

## Files, templates and freshness

```jldoctest
julia> make = @step make([] => "data.txt") = "seq 1 5 > {output}";

julia> total = @step total("data.txt" => "total.txt") = "paste -sd+ {input} | bc > {output}";

julia> r = run(make >> total; verbose=false);

julia> success(r), [s.status for s in r]
(true, [:done, :done])

julia> read("total.txt", String)
"15\n"

julia> [s.status for s in run(make >> total; verbose=false)]
2-element Vector{Symbol}:
 :skipped
 :skipped
```

## Failures are results

```jldoctest
julia> r = run(sh"false | cat"; verbose=false);

julia> success(r), r[1].result.kind
(false, :process_failed)

julia> run(sh"false" | sh"echo recovered"; verbose=false).value
"recovered\n"
```

## Rules and gather

```jldoctest
julia> mkpath("raw"); foreach(s -> write("raw/$s.txt", s), ["a", "b"]);

julia> upper = @rule upper("raw/{s}.txt" => "up/{s}.txt") = "tr a-z A-Z < {input} > {output}";

julia> joined = @rule joined(gather("up/{s}.txt") => "all.txt") = "cat {input} > {output}";

julia> r = run(upper >> joined; verbose=false);

julia> [s.label for s in r if s.status === :done] |> sort
3-element Vector{String}:
 "joined"
 "upper[s=a]"
 "upper[s=b]"

julia> read("all.txt", String)
"AB"
```

## Wildcards on disk

```jldoctest
julia> mkpath("fq"); foreach(s -> write("fq/$(s)_R1.fq", ""), ["S1", "S2"]);

julia> [w.sample for w in discover("fq/{sample}_R1.fq")]
2-element Vector{String}:
 "S1"
 "S2"

julia> expand("bam/{sample}.bam"; sample=["S1", "S2"])
2-element Vector{String}:
 "bam/S1.bam"
 "bam/S2.bam"
```
