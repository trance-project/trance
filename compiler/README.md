### Compilation Framework

This package contains the compilation framework for the standard and shredded pipeline. 

Organization of `src/main/scala/shredding` :
* `common`: types, operators, and variable definitions that are shared across the pipeline
* `nrc`: nrc and relevant extensions for shredding (dictionaries, labels), shredding, materialization
* `runtime`: runtime specifics for nrc
* `plans`: translates nrc into plans via unnesting
* `generator`: code generators, active development for Spark Datasets
* `loader`: functionality to load data for executing queries, currently csv readers used for reading tpch from a file
* `examples`: a directory of example queries, including the nested TPC-H benchmark queries
* `utils`: utility methods used throughout
