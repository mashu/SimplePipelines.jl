# @step macro and step_* helpers. Requires Types.jl.

"""
    @step name = work
    @step name(inputs => outputs) = work
    @step work

Create a named step with optional file dependencies. Steps are **lazy**: if the right-hand side
is a function call (other than `sh(...)`), it is wrapped in a thunk and runs only when the
pipeline is run via `run(pipeline)`.

Use `sh"..."` for literal commands; use `sh("... \$(var) ...")` or `sh("... " * var * " ...")`
when you need interpolation. Interpolated `sh(...)` is evaluated at step construction time
and produces a `Cmd`, so the step runs that command when executed.

# Examples
```julia
@step download = sh"curl -o data.csv http://example.com"
@step download([] => ["data.csv"]) = sh("curl -L -o " * repr(path) * " " * url)
@step process(["input.csv"] => ["output.csv"]) = sh"sort input.csv > output.csv"
@step process("path") = process_file   # function by name, receives path at run time
@step process(donor, "path/\$(donor).tsv") = process_file   # multiple inputs
@step sh"echo hello"
```
"""
macro step(expr)
    step_expr(expr)
end

"""Treat sh(cmd_expr) as interpolated shell: evaluate at construction time (no thunk)."""
function step_work_expr(e::Expr)
    if e.head === :call && !isempty(e.args) && e.args[1] === :sh
        return esc(e)
    end
    e.head === :call ? :(() -> $(esc(e))) : esc(e)
end
step_work_expr(rhs) = esc(rhs)

step_expr(expr::Symbol) = :(Step($(esc(expr))))
step_expr(expr) = :(Step($(esc(expr))))

function step_expr(expr::Expr)
    expr.head === :(=) || return :(Step($(esc(expr))))
    lhs, rhs = expr.args[1], expr.args[2]
    step_lhs(lhs, rhs)
end

step_lhs(lhs::Symbol, rhs) = :(Step($(QuoteNode(lhs)), $(step_work_expr(rhs))))

function step_lhs(lhs::Expr, rhs)
    lhs.head === :call || return :(Step($(esc(Expr(:(=), lhs, rhs)))))
    name = QuoteNode(lhs.args[1])
    length(lhs.args) >= 2 || return :(Step($name, $(step_work_expr(rhs))))
    deps = length(lhs.args) == 2 ? lhs.args[2] : Expr(:vect, lhs.args[2:end]...)
    step_deps(name, deps, rhs)
end

step_deps(name, deps, rhs) = :(Step($name, $(step_work_expr(rhs)), [$(esc(deps))], []))

function step_deps(name, deps::Expr, rhs)
    if deps.head === :call && length(deps.args) >= 3 && deps.args[1] === :(=>)
        inputs = deps.args[2]
        outputs = deps.args[3]
        return :(Step($name, $(step_work_expr(rhs)), $(step_inputs_expr(inputs)), $(step_outputs_expr(outputs))))
    end
    inputs_expr = deps.head === :vect ? esc(deps) : :([$(esc(deps))])
    :(Step($name, $(step_work_expr(rhs)), $inputs_expr, []))
end
step_inputs_expr(s::String) = :([$s])
step_inputs_expr(x) = esc(x)
step_outputs_expr(s::String) = :([$s])
step_outputs_expr(x) = esc(x)
