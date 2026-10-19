# Shell commands. `sh"..."` builds a `Shell` value that runs under
# `bash -e -o pipefail -c`, so a failure anywhere in `cmd1 | cmd2` fails the step.

"""
    Shell(script)

A shell script, run with `bash -e -o pipefail -c script` (or `sh -e -c script` when
bash is not installed). Strict mode means a failing command, including one in the
middle of a pipe, fails the step instead of being silently ignored.

Build one with [`@sh_str`](@ref) or [`sh`](@ref).
"""
struct Shell
    script::String
end

"""
    sh"command"
    sh(command)

A shell command. The literal form does not interpolate `\$`, so shell variables
such as `\$HOME` reach the shell untouched. Use `sh("... \$(x) ...")` when the
command needs Julia values.

```julia
sh"sort data.txt | uniq -c > counts.txt"
sh("bwa mem -t \$(n) ref.fa \$(sample).fq > \$(sample).sam")
```

Shell commands run with `bash -e -o pipefail`, so a failure anywhere in a pipe fails
the step. See also [`Shell`](@ref SimplePipelines.Shell).
"""
macro sh_str(s)
    :(Shell($s))
end

"""
    sh(command::AbstractString) -> Shell

Same as [`sh"..."`](@ref @sh_str), but takes an ordinary string so the command can
interpolate Julia values: `sh("samtools index \$(bam)")`.
"""
sh(s::AbstractString) = Shell(String(s))

"""Program and flags that run a `Shell` script: bash strict mode when available."""
function shell_program()
    bash = Sys.which("bash")
    bash === nothing ? String["sh", "-e", "-c"] : String[bash, "-e", "-o", "pipefail", "-c"]
end

command(s::Shell) = Cmd([shell_program(); s.script])
command(c::Base.AbstractCmd) = c

const SHELL_SAFE = r"^[A-Za-z0-9_./:@%+=,-]+$"

"""Quote `s` for a POSIX shell unless it consists only of safe characters."""
shell_quote(s::AbstractString) =
    occursin(SHELL_SAFE, s) ? String(s) : string("'", replace(s, "'" => "'\\''"), "'")
