# Development

## Tests

```bash
julia --project=. -t 4 -e 'using Pkg; Pkg.test()'
```

The suite lives in `test/`, one file per area (`execution.jl`, `rules.jl`,
`scheduler.jl`, …), all included from `test/runtests.jl`. Every test runs in its own
temporary directory, so state files and outputs never collide. Run it with one and
with several threads: the scheduler and memoization tests exercise concurrency. The
first testset checks the package for method ambiguities and unbound type parameters.

Coverage:

```bash
julia --project=. -e 'using Pkg; Pkg.test(coverage=true)'
```

## Documentation

```bash
julia --project=docs -e 'using Pkg; Pkg.develop(path="."); Pkg.instantiate(); include("docs/make.jl")'
```

`docs/make.jl` runs the doctests in `docs/src/doctests.md` during the build. Set
`DOCUMENTER_DOCTEST=only` to run only the doctests, or `false` to skip them. The
Documentation workflow builds the docs, with doctests, on every pull request.

## Adding a node type

A node is a subtype of `AbstractNode` with an `exec` method. Implement the pieces
the runtime and display use:

```julia
struct Repeat <: SimplePipelines.AbstractNode
    node::SimplePipelines.AbstractNode
    times::Int
end

function SimplePipelines.exec(r::Repeat, rt, scope, input)
    out = SimplePipelines.run_node(r.node, rt, scope, input)
    for _ in 2:r.times
        out.ok || return out
        SimplePipelines.forget_failures!(rt)
        out = SimplePipelines.exec(r.node, rt, scope, input)   # bypass the memo: run again
    end
    out
end

SimplePipelines.children(r::Repeat) = SimplePipelines.AbstractNode[r.node]
SimplePipelines.dag(pr::SimplePipelines.DagPrinter, r::Repeat, prefix, cont) =
    SimplePipelines.composite(pr, r, prefix, cont, "⟳ ", :yellow, "Repeat ×$(r.times)")
```

`children` makes `steps`, `count_steps` and `print_dag` work. These are internals,
not part of the stable API.

## Code conventions

The repository's standards are in `.cursor/rules/julia-standards.mdc`. In short:
dispatch instead of `isa`, no leading underscores, no `try` outside `run_safely`,
validation in constructors, and small named functions. See [Design](design.md) for
the architecture and the reasons behind the type layout.
