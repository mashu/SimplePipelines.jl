using SimplePipelines
using Test

@testset "SimplePipelines" begin
    
    @testset "Step creation" begin
        # From Cmd
        s1 = Step(`echo hello`)
        @test s1.work == `echo hello`
        
        # Named step
        s2 = Step(:mystep, `echo world`)
        @test s2.name == :mystep
        
        # With dependencies
        s3 = Step(:process, `cat input.txt`, ["input.txt"], ["output.txt"])
        @test s3.inputs == ["input.txt"]
        @test s3.outputs == ["output.txt"]
        
        # From function
        f = () -> 42
        s4 = Step(f)
        @test s4.work() == 42
    end
    
    @testset "@step macro" begin
        # Anonymous
        s1 = @step `echo test`
        @test s1.work == `echo test`
        
        # Named
        s2 = @step mystep = `echo named`
        @test s2.name == :mystep
        
        # With dependencies
        s3 = @step process("in.txt" => "out.txt") = `cat in.txt`
        @test s3.name == :process
        @test s3.inputs == ["in.txt"]
        @test s3.outputs == ["out.txt"]
        
        # Function step
        g() = "result"
        s4 = @step compute = g
        @test s4.name == :compute
    end
    
    @testset "Sequence operator >>" begin
        s1 = @step a = `echo a`
        s2 = @step b = `echo b`
        s3 = @step c = `echo c`
        
        # Basic sequence
        seq = s1 >> s2
        @test seq isa Sequence
        @test length(seq.nodes) == 2
        
        # Chaining
        seq3 = s1 >> s2 >> s3
        @test length(seq3.nodes) == 3
        
        # Direct Cmd chaining
        seq_cmd = `echo 1` >> `echo 2`
        @test seq_cmd isa Sequence
    end
    
    @testset "Parallel operator &" begin
        s1 = @step a = `echo a`
        s2 = @step b = `echo b`
        s3 = @step c = `echo c`
        
        # Basic parallel
        par = s1 & s2
        @test par isa Parallel
        @test length(par.nodes) == 2
        
        # Chaining
        par3 = s1 & s2 & s3
        @test length(par3.nodes) == 3
    end
    
    @testset "Complex DAG" begin
        # Diamond pattern: a -> (b & c) -> d
        a = @step a = `echo a`
        b = @step b = `echo b`
        c = @step c = `echo c`
        d = @step d = `echo d`
        
        dag = a >> (b & c) >> d
        @test dag isa Sequence
        @test dag.nodes[1] isa Step
        @test dag.nodes[2] isa Parallel
        @test dag.nodes[3] isa Step
    end
    
    @testset "Pipeline" begin
        s1 = @step `echo hello`
        s2 = @step `echo world`
        
        p = Pipeline(s1 >> s2, name="test")
        @test p.name == "test"
        @test count_steps(p.root) == 2
    end
    
    @testset "Step execution" begin
        # Simple echo command
        s = @step test = `echo "hello world"`
        results = run_pipeline(s, verbose=false)
        
        @test length(results) == 1
        @test results[1].success == true
        @test contains(results[1].output, "hello world")
    end
    
    @testset "Sequence execution" begin
        seq = `echo first` >> `echo second`
        results = run_pipeline(seq, verbose=false)
        
        @test length(results) == 2
        @test all(r -> r.success, results)
    end
    
    @testset "Parallel execution" begin
        # Verify parallel structure executes correctly
        par = `echo "a"` & `echo "b"` & `echo "c"`
        
        results = run_pipeline(par, verbose=false)
        
        @test length(results) == 3
        @test all(r -> r.success, results)
    end
    
    @testset "Julia function steps" begin
        counter = Ref(0)
        f = () -> (counter[] += 1; "done")
        
        s = Step(:increment, f)
        results = run_pipeline(s, verbose=false)
        
        @test results[1].success
        @test counter[] == 1
    end
    
    @testset "Utility functions" begin
        a = @step a = `echo a`
        b = @step b = `echo b`
        c = @step c = `echo c`
        
        dag = a >> (b & c)
        
        @test count_steps(dag) == 3
        @test length(steps(dag)) == 3
    end
    
    @testset "Dry run" begin
        dag = `echo a` >> (`echo b` & `echo c`) >> `echo d`
        
        # Should not error, just print structure
        results = run_pipeline(dag, dry_run=true, verbose=false)
        @test isempty(results)
    end
    
    @testset "Type stability" begin
        # Verify operations produce concrete types
        a = @step a = `echo a`
        b = @step b = `echo b`
        
        seq = a >> b
        @test isconcretetype(typeof(seq))
        
        par = a & b
        @test isconcretetype(typeof(par))
        
        # Pipeline should be concrete
        p = Pipeline(seq, name="test")
        @test isconcretetype(typeof(p))
    end
end
