![simplepipelines_logo](https://github.com/user-attachments/assets/f4a31cd2-294e-42ac-86be-ab4a6386c78a)

# SimplePipelines.jl

[![Stable Docs](https://img.shields.io/badge/docs-stable-blue.svg)](https://mashu.github.io/SimplePipelines.jl/stable)
[![Dev Docs](https://img.shields.io/badge/docs-dev-blue.svg)](https://mashu.github.io/SimplePipelines.jl/dev)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)
[![Julia](https://img.shields.io/badge/julia-1.9+-9558B2.svg?logo=julia&logoColor=white)](https://julialang.org/)
[![Release](https://img.shields.io/github/v/release/mashu/SimplePipelines.jl?label=version&logo=github)](https://github.com/mashu/SimplePipelines.jl/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey.svg)](https://github.com/mashu/SimplePipelines.jl/actions)

Minimal, type-stable DAG pipelines for Julia with Make-like incremental builds.

## Quick start

```julia
using SimplePipelines

run(sh"echo hi" >> sh"wc -c")

run((sh"a" & sh"b") >> sh"merge")

@step prep = sh"(echo 1; echo 2) > data.csv" >>
@step sum = () -> sum(parse.(Int, readlines("data.csv"))) |> run
```
