# Composition operators. Every method involves at least one of the package's own
# types, so Base behaviour for Cmd, Function, String, … is left untouched.

@nospecialize   # see runtime.jl

import Base: >>, &, |, ^

const Liftable = Union{AbstractNode,Shell,Rule}
const Operand = Union{Liftable,Base.AbstractCmd,Function,AbstractString}

>>(a::AbstractNode, b::AbstractNode) = Sequence(AbstractNode[a, b])
>>(a::Sequence, b::AbstractNode) = Sequence(AbstractNode[a.nodes; b])
>>(a::AbstractNode, b::Sequence) = Sequence(AbstractNode[a; b.nodes])
>>(a::Sequence, b::Sequence) = Sequence(AbstractNode[a.nodes; b.nodes])
>>(a::Liftable, b::Operand) = as_node(a) >> as_node(b)
>>(a::Operand, b::Liftable) = as_node(a) >> as_node(b)
>>(a::Liftable, b::Liftable) = as_node(a) >> as_node(b)

(&)(a::AbstractNode, b::AbstractNode) = Parallel(AbstractNode[a, b])
(&)(a::Parallel, b::AbstractNode) = Parallel(AbstractNode[a.nodes; b])
(&)(a::AbstractNode, b::Parallel) = Parallel(AbstractNode[a; b.nodes])
(&)(a::Parallel, b::Parallel) = Parallel(AbstractNode[a.nodes; b.nodes])
(&)(a::Liftable, b::Operand) = as_node(a) & as_node(b)
(&)(a::Operand, b::Liftable) = as_node(a) & as_node(b)
(&)(a::Liftable, b::Liftable) = as_node(a) & as_node(b)

# Rules carry no order: `>>` and `&` between rules merge them into one Workflow, which
# derives the dependencies from the file patterns.
const RuleSet = Union{Rule,Workflow}
>>(a::RuleSet, b::RuleSet) = merge_workflows(a, b)
>>(a::Workflow, b::Workflow) = merge_workflows(a, b)
(&)(a::RuleSet, b::RuleSet) = merge_workflows(a, b)
(&)(a::Workflow, b::Workflow) = merge_workflows(a, b)

(|)(a::AbstractNode, b::AbstractNode) = Fallback(a, b)
(|)(a::Liftable, b::Operand) = Fallback(as_node(a), as_node(b))
(|)(a::Operand, b::Liftable) = Fallback(as_node(a), as_node(b))
(|)(a::Liftable, b::Liftable) = Fallback(as_node(a), as_node(b))

(^)(a::Liftable, n::Integer) = Retry(as_node(a), n)

@specialize
