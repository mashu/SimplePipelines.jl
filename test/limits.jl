# Resource limits end to end: the guard terminates commands that use too much memory or
# time, cleans up what steps leave behind, and stops runs safely. Memory is touched with
# perl string repetition (calloc'd pages would not count).

alive(cmdline) = success(pipeline(`pgrep -x -f $cmdline`; stdout=devnull))

"""Write the guard's fake memory file atomically: available and total MiB."""
function set_memory(path, available, total)
    write(path * ".tmp", "$available $total 0\n")
    mv(path * ".tmp", path; force=true)
end

perl_hog(mb, seconds) = "perl -e '\$x = \"a\" x ($mb * 2**20); sleep $seconds'"

SP.restart_guard!(; interval=0.05)

@testset "threads and templates at launch" begin
    in_tempdir() do _
        # every command of a pipeline gets the thread variables, in its own session
        cmd = pipeline(Cmd(["sh", "-c", "echo \$OMP_NUM_THREADS"]), Cmd(["sh", "-c", "cat; echo \$RAYON_NUM_THREADS"]))
        @test quiet(Step(:env, cmd; threads=3); jobs=4).value == "3\n3\n"
        Sys.islinux() && let stat = quiet(Step(:sid, sh"cat /proc/$$/stat")).value
            @test split(stat[findlast(')', stat)+2:end])[4] == first(split(stat))    # session id == pid
        end
        # templates are filled with what the step was granted when it starts
        r = quiet(with_resources(Step(:r, "echo {threads} {mem_mb}", String[], String[]); threads=3, mem_mb=400); jobs=4)
        @test r.value == "3 400\n"
        # a declaration over the run's budget is clamped, and the template sees the clamped value
        r, text = logged(() -> run(Step(:c, "echo {mem_mb}", String[], String[]; mem_mb=100_000); mem_mb=5_000))
        @test r.value == "5000\n" && occursin("declares mem_mb = 100000", text)
        # {mem_mb} without a declared mem_mb fails before anything runs, in run and in check
        undeclared = Step(:u, "echo {mem_mb}", String[], String[])
        @test quiet(undeclared)[1].result.kind === :template
        @test check(undeclared; verbose=false)[1].result.kind === :template
        # nested with_resources: the inner one sets threads, the outer one still gives mem_mb
        r = quiet(with_resources(with_resources(Step(:n, "echo {threads} {mem_mb}", String[], String[]); threads=2);
                                 mem_mb=300); jobs=4)
        @test r.value == "2 300\n"
        @test_throws ArgumentError Step(:inf, sh"true"; timeout=Inf)
        @test_throws ArgumentError Timeout(sh"true", Inf)
        # a missing program fails before anything starts, and the message does not list the environment
        r = quiet(Step(:typo, pipeline(`sleep 316`, `nonexistent_tool_xyz`)))
        @test r[1].result.kind === :process_failed && occursin("nonexistent_tool_xyz` was not found", r[1].result.message)
        @test !occursin("THREADS", r[1].result.message) && !alive("sleep 316")
        # without the guard, steps still run (and the run says what is not enforced), and
        # the end of a step still cleans up, including children in their own process group
        r, text = logged(() -> run(sh"echo free"; guard=false))
        @test r.value == "free\n" && occursin("without the resource guard", text)
        t = @elapsed r = quiet(@step(pg = "timeout 25 sleep 319 & echo hi"); guard=false)
        @test r.value == "hi\n" && t < 5 && !alive("sleep 319")
    end
end

@testset "julia() runs in a separate, limited process" begin
    in_tempdir() do _
        r = quiet((@step n(; threads=2) = julia("-e 'write(\"n.txt\", string(Threads.nthreads()))'")); jobs=4)
        @test r.success && read("n.txt", String) == "2"
        w = @step w([] => "out.txt") = julia("-e 'write(ARGS[1], \"hi\")' {output}")
        @test quiet(w).success && read("out.txt", String) == "hi"
    end
end

@testset "step timeout" begin
    in_tempdir() do _
        t = @elapsed r = quiet(@step slow(; timeout=0.3) = "sleep 30")
        @test r[1].result.kind === :timed_out && occursin("timeout = 0.3 s", r[1].result.message) && t < 3
        r = quiet(with_resources((@step w = "sleep 30"); timeout=0.3))
        @test r[1].result.kind === :timed_out
        # a command that ignores SIGTERM is killed after the grace period
        t = @elapsed r = quiet(@step stubborn(; timeout=0.3) = "trap '' TERM; while :; do sleep 0.1; done")
        @test r[1].result.kind === :timed_out && t < 8
    end
end

@testset "leftover processes" begin
    in_tempdir() do _
        t = @elapsed r = quiet(@step bg = "sleep 301 & echo hi")
        @test r[1].result.kind === :process_failed && occursin("background", r[1].result.message) && t < 3
        sleep(0.3)
        @test !alive("sleep 301")
        @test quiet(@step waits = "sleep 0.2 & wait; echo ok").value == "ok\n"
        # children that move to their own process group (timeout, set -m) do not escape a Timeout
        r = quiet(Timeout(Step(:t, sh"timeout 30 sleep 302; echo x"), 0.5))
        sleep(0.5)
        @test !r.success && !alive("sleep 302")
        # Julia code abandoned by a Timeout keeps running, but gives its slot back
        t = @elapsed r = quiet(Fallback(Timeout(Step(:stuck, () -> (sleep(20); 1)), 0.5), Step(:cached, () -> 2)); jobs=1)
        @test r.success && r.value == 2 && t < 5
        # a daemon that escapes the step but keeps its output open does not hang the run
        if Sys.islinux() && Sys.which("setsid") !== nothing
            t = @elapsed r = quiet(@step d = "setsid -f sleep 318; echo done")
            @test r[1].result.kind === :process_failed && t < 5
            run(ignorestatus(`pkill -x -f "sleep 318"`))
        end
    end
end

if Sys.islinux()
    @testset "memory limit" begin
        in_tempdir() do _
            t = @elapsed r = quiet(Step(:big, perl_hog(300, 10); mem_mb=100))
            @test r[1].result.kind === :memory_limit && occursin("mem_mb = 100 MiB", r[1].result.message)
            @test r[1].peak_mb >= 100 && t < 5
            r = quiet(Step(:fine, perl_hog(100, 1); mem_mb=500))
            @test r.success && r[1].peak_mb >= 90
            # the guard keeps enforcing while this process is busy
            busy() = (t0 = time(); while time() - t0 < 3 end; nothing)
            t = @elapsed r = quiet(Step(:busy, busy) & Step(:hog, perl_hog(300, 20); mem_mb=100); jobs=2)
            @test only(filter(s -> s.label == "hog", r)).result.kind === :memory_limit && t < 10
        end
    end

    @testset "memory pressure" begin
        in_tempdir() do dir
            mem = joinpath(dir, "memory")
            set_memory(mem, 4000, 4000)
            SP.restart_guard!(; interval=0.05, fake_memory=mem)
            # two steps, memory runs low: one is stopped and runs again when memory is free
            script(i) = "if [ -e started_$i ]; then echo again; else touch started_$i; " *
                        "trap 'touch killed_$i; exit 143' TERM; $(perl_hog(100, 6)); fi"
            task = Threads.@spawn logged(() -> run(Step(:a, script(1)) & Step(:b, script(2)); jobs=2, mem_mb=0))
            @test timedwait(() -> isfile("started_1") && isfile("started_2"), 20) === :ok
            sleep(2.5)                  # fast growth raises the reserve for 2 s: this would be critical
            set_memory(mem, 200, 4000)
            @test timedwait(() -> isfile("killed_1") || isfile("killed_2"), 10) === :ok
            sleep(0.3)
            set_memory(mem, 4000, 4000)
            r, text = fetch(task)
            @test r.success && nstatus(r, :done) == 2 && occursin("↺", text)
            @test count(startswith("killed_"), readdir()) == 1
            # pressure from other programs: nothing is killed, the run waits and warns
            SP.restart_guard!(; interval=0.05, fake_memory=mem)     # warnings are shown every 30 s
            task = Threads.@spawn logged(() -> run(Step(:small, "touch small_started; sleep 1.5")))
            @test timedwait(() -> isfile("small_started"), 20) === :ok
            set_memory(mem, 220, 4000)
            sleep(0.5)
            set_memory(mem, 4000, 4000)
            r, text = fetch(task)
            @test r.success && occursin("other programs", text)
            # a step that alone needs more than is free fails instead of looping
            task = Threads.@spawn quiet(Step(:hog, "touch hog_started; $(perl_hog(100, 10))"))
            @test timedwait(() -> isfile("hog_started"), 20) === :ok
            sleep(0.5)
            set_memory(mem, 50, 4000)
            r = fetch(task)
            set_memory(mem, 4000, 4000)
            @test r[1].result.kind === :out_of_memory && occursin("alone", r[1].result.message)
            SP.restart_guard!(; interval=0.05)
        end
    end
end

@testset "a lost guard stops the run" begin
    in_tempdir() do _
        task = Threads.@spawn logged(() -> run(Retry(Step(:s, "touch s_started; sleep 303"), 2) >> Step(:after, sh"true") &
                                               Step(:f, () -> (sleep(2); 1))))
        @test timedwait(() -> isfile("s_started"), 20) === :ok
        ccall(:kill, Cint, (Cint, Cint), (@atomic SP.CURRENT.link).pid, 9)
        @test timedwait(() -> istaskdone(task), 20) === :ok
        r, text = fetch(task)
        @test !r.success && r[1].result.kind === :stopped && occursin("exited unexpectedly", text)
        @test !occursin("retry", text)
        sleep(0.3)
        @test !alive("sleep 303")
        @test quiet(sh"echo again").value == "again\n"          # the next run starts a new guard
    end
end

"""Start `body` as a pipeline script in its own process group; returns the process."""
function start_driver(dir, body; threads="1")
    script = joinpath(dir, "driver.jl")
    write(script, "using SimplePipelines\n" * body)
    julia = `$(Base.julia_cmd()) --startup-file=no --project=$(dirname(Base.active_project())) -t $threads $script`
    run(pipeline(Cmd(julia; detach=true, dir=dir); stdout=devnull, stderr=joinpath(dir, "stderr.txt")); wait=false)
end

@testset "nothing outlives the pipeline" begin
    in_tempdir() do dir
        body = "run(Step(:s, \"touch started; sleep 309\"); verbose=false)\nwrite(\"finished\", \"\")\n"
        # Ctrl-C, SIGTERM and a closed terminal reach the whole group; SIGKILL only Julia
        for (signal, group, threads) in ((2, true, "1"), (2, true, "4"), (15, true, "4"), (1, true, "1"), (9, false, "1"))
            rm("started"; force=true)
            p = start_driver(dir, body; threads)
            @test timedwait(() -> isfile("started"), 300) === :ok
            sleep(0.3)
            ccall(:kill, Cint, (Cint, Cint), group ? -getpid(p) : getpid(p), signal)
            @test timedwait(() -> process_exited(p), 20) === :ok
            process_exited(p) || kill(p, 9)
            sleep(0.5)
            @test !alive("sleep 309")
            @test !isfile("finished")
        end
        # REPL-like: a terminal's Ctrl-C reaches the whole process group and makes run throw
        # InterruptException promptly, even while a Julia function step is still computing
        repl = "Base.exit_on_sigint(false)\n" *
               "run(Step(:s, \"touch started; sleep 309\") & Step(:f, () -> sleep(60)); verbose=false)\n" *
               "write(\"finished\", \"\")\n"
        for threads in ("1", "2,1")
            rm("started"; force=true)
            p = start_driver(dir, repl; threads)
            @test timedwait(() -> isfile("started"), 300) === :ok
            sleep(0.3)
            ccall(:kill, Cint, (Cint, Cint), -getpid(p), 2)
            @test timedwait(() -> process_exited(p), 10) === :ok
            process_exited(p) || kill(p, 9)
            sleep(0.5)
            @test !alive("sleep 309") && !isfile("finished")
            @test occursin("InterruptException", read(joinpath(dir, "stderr.txt"), String))
        end
    end
end

SP.restart_guard!()
