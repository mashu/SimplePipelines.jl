![simplepipelines_logo](https://github.com/user-attachments/assets/f4a31cd2-294e-42ac-86be-ab4a6386c78a)

# SimplePipelines.jl

[![Stable Docs](https://img.shields.io/badge/docs-stable-blue.svg)](https://mashu.github.io/SimplePipelines.jl/stable)
[![Dev Docs](https://img.shields.io/badge/docs-dev-blue.svg)](https://mashu.github.io/SimplePipelines.jl/dev)
[![Build Status](https://github.com/mashu/SimplePipelines.jl/workflows/CI/badge.svg)](https://github.com/mashu/SimplePipelines.jl/actions)
[![codecov](https://codecov.io/gh/mashu/SimplePipelines.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/mashu/SimplePipelines.jl)
[![Julia](https://img.shields.io/badge/julia-1.10+-9558B2.svg?logo=julia&logoColor=white)](https://julialang.org/)
[![Release](https://img.shields.io/github/v/release/mashu/SimplePipelines.jl?label=version&logo=github)](https://github.com/mashu/SimplePipelines.jl/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey.svg)](https://github.com/mashu/SimplePipelines.jl/actions)

Incremental, parallel pipelines for shell commands and Julia functions, with
Snakemake-style wildcard rules.

```julia
using SimplePipelines

trim  = @rule trim("raw/{s}_R1.fq.gz" => "trim/{s}.fq.gz"; threads=4) =
    "fastp -w {threads} -i {input} -o {output}"
align = @rule align(["trim/{s}.fq.gz", "ref.fa"] => "bam/{s}.bam"; threads=8) =
    "bwa mem -t {threads} ref.fa {input[1]} | samtools sort -o {output}"
calls = @rule calls(gather("bam/{s}.bam") => "calls.vcf") =
    "bcftools mpileup -f ref.fa {input} | bcftools call -mv -o {output}"

check(trim >> align >> calls)    # what would run, and why
run(trim >> align >> calls)      # every sample found in raw/, then one joint call
```

- **Wildcards end to end**: samples are discovered from the files on disk; pattern
  targets and `gather` need no sample lists.
- **Correct incremental builds**: steps are skipped only when outputs are newer than
  inputs *and* were made by the same command. Partial outputs of failed steps are
  removed.
- **Cannot freeze your computer**: a resource guard terminates commands that exceed
  their `mem_mb` or `timeout`, pauses and requeues work before memory runs out, and
  kills everything on Ctrl-C, a closed terminal or a crash. CPU and memory budgets
  follow container and cluster limits.
- **Clear errors**: `bash -e -o pipefail`, typo checks when a rule is defined, and
  an explanation of why a target cannot be built.
- **Fast**: loads in about 0.2 s, with little compilation on the first run.

Plain steps compose the same way:

```julia
prep  = @step prep([] => "data.txt") = "seq 1 100 > {output}"
total = @step total("data.txt" => "total.txt") = "paste -sd+ {input} | bc > {output}"
show_total = @step show_total = paths -> read(only(paths), String)

run(prep >> total >> show_total).value      # "5050\n"
```

See the [documentation](https://mashu.github.io/SimplePipelines.jl/dev) for the guide,
examples, and notes on [migrating from 0.3](https://mashu.github.io/SimplePipelines.jl/dev/reference/migration/).
