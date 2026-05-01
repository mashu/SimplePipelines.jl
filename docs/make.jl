using Documenter
using SimplePipelines

# Resolve paths regardless of process working directory (CI runs `julia docs/make.jl` from repo root).
const DOCS_DIR = dirname(@__FILE__)

# `true` / unset: full build + doctests. `only`: doctests only (used from `Pkg.test()`). `false`: skip doctests.
const DOCTEST_ENV = get(ENV, "DOCUMENTER_DOCTEST", "true")
const DOCTEST = if DOCTEST_ENV == "only"
    :only
elseif DOCTEST_ENV == "false"
    false
else
    true
end
const CHECKDOCS = DOCTEST === :only ? :none : :exports

makedocs(
    root = DOCS_DIR,
    sitename = "SimplePipelines.jl",
    modules = [SimplePipelines],
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", nothing) == "true",
        canonical = "https://mashu.github.io/SimplePipelines.jl",
        edit_link = "main",  # avoid git remote lookup when building locally
    ),
    pages = [
        "Home" => "index.md",
        "Tutorial" => "tutorial.md",
        "Examples" => "examples.md",
        "Doctests" => "doctests.md",
        "API Reference" => "api.md",
        "Design" => "design.md",
        "Development" => "development.md",
    ],
    checkdocs = CHECKDOCS,
    doctest = DOCTEST,
)

if DOCTEST !== :only
    deploydocs(
        repo = "github.com/mashu/SimplePipelines.jl.git",
        devbranch = "main",
    )
end
