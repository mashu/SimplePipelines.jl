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
        assets = ["assets/custom.css"],
    ),
    pages = [
        "Home" => "index.md",
        "User guide" => [
            "Steps and shell" => "guide/steps-and-shell.md",
            "Composing pipelines" => "guide/composition.md",
            "Rules and diagnostics" => "guide/rules-and-diagnostics.md",
            "Choosing operators & Workflow" => "guide/decision-guide.md",
            "Control flow" => "guide/control-flow.md",
            "Fan-out and reduce" => "guide/foreach-reduce.md",
            "Running and inspecting" => "guide/running-and-results.md",
        ],
        "Examples" => [
            "Basics" => "examples/basics.md",
            "Control flow" => "examples/control-flow.md",
            "Complex DAGs" => "examples/complex-dags.md",
            "Bioinformatics" => "examples/bioinformatics.md",
        ],
        "Reference" => [
            "Quick reference" => "reference/quickref.md",
            "Public API policy" => "reference/public-api.md",
            "API" => "api.md",
        ],
        "Internals" => "design.md",
        "Contributing" => [
            "Extending & dev workflow" => "development.md",
            "Doctest blocks" => "doctests.md",
        ],
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
