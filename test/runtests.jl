using SimplePipelines
using Test

clear_state!()

@testset "SimplePipelines" begin
    include("suites/01_shell_steps_display.jl")
    include("suites/02_utilities_run_display.jl")
    include("suites/03_errors_control_flow.jl")
    include("suites/04_foreach_pipe_reduce.jl")
    include("suites/05_dag_rules_resources.jl")
    include("suites/06_report_branch_timeout_tables.jl")
    include("suites/07_rule_onboarding.jl")
end

# Documenter `jldoctest` blocks in `docs/src/` (see `doctests.md`). Spawn a fresh Julia with a
# normal `JULIA_LOAD_PATH` (the `Pkg.test` sandbox hides `Pkg` from the parent process).
@testset "Documentation (Documenter doctest :only)" begin
    root = dirname(@__DIR__)
    docs = joinpath(root, "docs")
    make_jl = joinpath(docs, "make.jl")
    code = string(
        "import Pkg; ",
        "Pkg.activate(", repr(docs), "); ",
        "Pkg.develop(path=", repr(root), "); ",
        "Pkg.instantiate(); ",
        "ENV[\"DOCUMENTER_DOCTEST\"]=\"only\"; ",
        "include(", repr(make_jl), ")",
    )
    cmd = `$(Base.julia_cmd()) -e $(code)`
    ok = withenv("JULIA_LOAD_PATH" => "@:@stdlib") do
        success(run(cmd))
    end
    @test ok
end
