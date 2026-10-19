using Documenter
using SimplePipelines

const DOCS_DIR = dirname(@__FILE__)

# `true` / unset: full build with doctests. `only`: doctests only. `false`: skip doctests.
const DOCTEST_ENV = get(ENV, "DOCUMENTER_DOCTEST", "true")
const DOCTEST = DOCTEST_ENV == "only" ? :only : DOCTEST_ENV != "false"

makedocs(
    root = DOCS_DIR,
    sitename = "SimplePipelines.jl",
    modules = [SimplePipelines],
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", nothing) == "true",
        canonical = "https://mashu.github.io/SimplePipelines.jl",
        edit_link = "main",
        assets = ["assets/custom.css"],
    ),
    pages = [
        "Home" => "index.md",
        "Guide" => [
            "Getting started" => "guide/getting-started.md",
            "Steps" => "guide/steps.md",
            "Composing pipelines" => "guide/composition.md",
            "Wildcard rules and workflows" => "guide/rules.md",
            "Fan-out with ForEach" => "guide/foreach.md",
            "Control flow" => "guide/control-flow.md",
            "Running and freshness" => "guide/running.md",
            "Resource limits and safety" => "guide/resources.md",
        ],
        "Examples" => [
            "Basics" => "examples/basics.md",
            "Bioinformatics" => "examples/bioinformatics.md",
        ],
        "Reference" => [
            "Quick reference" => "reference/quickref.md",
            "API" => "api.md",
            "Migrating from 0.3" => "reference/migration.md",
        ],
        "Contributing" => [
            "Design" => "design.md",
            "Development" => "development.md",
            "Doctests" => "doctests.md",
        ],
    ],
    checkdocs = DOCTEST === :only ? :none : :exports,
    doctest = DOCTEST,
)

if DOCTEST !== :only
    deploydocs(
        repo = "github.com/mashu/SimplePipelines.jl.git",
        devbranch = "main",
    )
end
