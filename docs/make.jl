using Documenter
using SimplePipelines

makedocs(
    sitename = "SimplePipelines.jl",
    modules = [SimplePipelines],
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", nothing) == "true",
        canonical = "https://mashu.github.io/SimplePipelines.jl",
    ),
    pages = [
        "Home" => "index.md",
        "Tutorial" => "tutorial.md",
        "Examples" => "examples.md",
        "API Reference" => "api.md",
        "Design" => "design.md",
        "Development" => "development.md",
    ],
    checkdocs = :exports,
)

deploydocs(
    repo = "github.com/mashu/SimplePipelines.jl.git",
    devbranch = "main",
)
