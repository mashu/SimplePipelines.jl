using SimplePipelines
using Test

const SP = SimplePipelines

"""Run `f(dir)` inside a fresh temporary directory (so state files stay there)."""
in_tempdir(f) = mktempdir(dir -> cd(() -> f(dir), dir))

quiet(x; kwargs...) = run(x; verbose=false, kwargs...)
statuses(r) = [s.status for s in r]
labels(r) = [s.label for s in r]
nstatus(r, status) = count(s -> s.status === status, r)

"""Call `f()` with stdout going to a file; returns its value and what it printed."""
function logged(f)
    path = tempname()
    value = open(io -> redirect_stdout(f, io), path, "w")
    value, read(path, String)
end

"""Tracks how many callers are inside `busy` at once."""
struct Gauge
    active::Threads.Atomic{Int}
    peak::Threads.Atomic{Int}
end
Gauge() = Gauge(Threads.Atomic{Int}(0), Threads.Atomic{Int}(0))
function busy(g::Gauge, seconds=0.15)
    now = Threads.atomic_add!(g.active, 1) + 1
    Threads.atomic_max!(g.peak, now)
    sleep(seconds)
    Threads.atomic_sub!(g.active, 1)
    nothing
end

@testset "SimplePipelines" begin
    @testset "package hygiene" begin
        @test isempty(Test.detect_ambiguities(SimplePipelines))
        @test isempty(Test.detect_unbound_args(SimplePipelines))
    end
    include("shell_and_patterns.jl")
    include("steps_and_operators.jl")
    include("execution.jl")
    include("control_flow.jl")
    include("foreach.jl")
    include("rules.jl")
    include("freshness.jl")
    include("scheduler.jl")
    include("display.jl")
    include("guard.jl")
    include("limits.jl")
end
