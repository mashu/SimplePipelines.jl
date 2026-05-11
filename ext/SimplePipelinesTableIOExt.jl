module SimplePipelinesTableIOExt

using CSV
using DataFrames: DataFrame
using SimplePipelines

function SimplePipelines.materialize_table(fp::FilePath; kwargs...)
    CSV.read(fp.path, DataFrame; kwargs...)
end

end
