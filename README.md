![simplepipelines_logo](https://github.com/user-attachments/assets/f4a31cd2-294e-42ac-86be-ab4a6386c78a)

# SimplePipelines.jl

[![Docs](https://img.shields.io/badge/docs-dev-blue.svg)](https://mashu.github.io/SimplePipelines.jl/dev)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Minimal, type-stable DAG pipelines for Julia.

## Quick Start

```julia
using SimplePipelines

# Chain steps with >>
pipeline = sh"echo '1,2,3' > data.csv" >> sh"wc -l data.csv"

# Redirection and pipes
pipeline = sh"(echo a; echo b; echo a) > data.csv" >> sh"sort data.csv | uniq > sorted.csv"

# Run in parallel with &
pipeline = (sh"echo task_a" & sh"echo task_b" & sh"echo task_c") >> sh"echo merge"

# Mix shell and Julia
pipeline = @step fetch = sh"(echo 1; echo 2; echo 3) > data.csv" >>
           @step analyze = () -> sum(parse.(Int, readlines("data.csv")))

run(pipeline)
```

## Interface

<table>
<tr style="background:#e8eef4; color:#1a1a2e;">
<th style="padding:6px 10px; text-align:left;">Concept</th>
<th style="padding:6px 10px; text-align:left;">Syntax</th>
<th style="padding:6px 10px; text-align:left;">Description</th>
</tr>
<tr style="background:#f8f9fa;"><td colspan="3" style="padding:4px 10px; font-weight:600; color:#2d3748;">Commands</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">sh"cmd"</td><td style="padding:4px 10px;">shell, redirection, pipes</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">sh("$(var)")</td><td style="padding:4px 10px;">with interpolation</td></tr>
<tr style="background:#f8f9fa;"><td colspan="3" style="padding:4px 10px; font-weight:600; color:#2d3748;">Operators</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">a >> b</td><td style="padding:4px 10px;">sequence</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">a & b</td><td style="padding:4px 10px;">parallel</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">a &#124; b</td><td style="padding:4px 10px;">fallback (b if a fails)</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">a^3</td><td style="padding:4px 10px;">retry up to 3×</td></tr>
<tr style="background:#f8f9fa;"><td colspan="3" style="padding:4px 10px; font-weight:600; color:#2d3748;">Control</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">Branch(cond, a, b)</td><td style="padding:4px 10px;">conditional</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">Timeout(a, 30.0)</td><td style="padding:4px 10px;">time limit</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">Reduce(f, a & b)</td><td style="padding:4px 10px;">combine parallel outputs</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">ForEach(pattern) do ...</td><td style="padding:4px 10px;">discover files, parallel branches</td></tr>
<tr style="background:#f8f9fa;"><td colspan="3" style="padding:4px 10px; font-weight:600; color:#2d3748;">Run</td></tr>
<tr><td style="padding:4px 10px;"></td><td style="padding:4px 10px; font-family:monospace; background:#f0f4f8;">run(pipeline)</td><td style="padding:4px 10px;">execute pipeline</td></tr>
</table>

## Multi-file Processing

```julia
# Discover files, create parallel branches automatically
ForEach("data/{sample}_R1.fq.gz") do sample
    sh("pear -f $(sample)_R1.fq.gz -r $(sample)_R2.fq.gz") >> sh("process $(sample)")
end
```
