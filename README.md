## Shredder

To compile and run:
```
sbt run
```

Organization of `src/main/scala/shredding`:
* `common`: types, operators, and variable definitions that are shared across the pipeline
* `nrc`: soure nrc, target nrc, nrc extensions for shredding (dictionaries, labels), shredding, and linearization  
* `runtime`: runtime specifics for nrc and shredded nrc 
* `wmcc`: weighted monad comprehension calculus (wmcc), which algebra operators (select, reduce, join, etc.).
* `generator`: code generators, currently just native scala
* `loader`: functionality to load data for executing queries, currently csv readers used for reading tpch from a file
* `queries`: a directory of queries that are used for examples and experiments
