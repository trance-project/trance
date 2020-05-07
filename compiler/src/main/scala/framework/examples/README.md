### Example Queries

Each query directory contains a set of input relations in various formats with their respective types, 
as well as flat and nested queries that use these relations. 

* `tpch`: TPCH queries from Slender paper. Since TPCH loads data from a CSV file, there are TPCH loader specifics in this directory.
* `simple`: simple example queries used for testing
* `normalize`: queries that trigger normalization rules
* `optimize`: queries specific to exploring the domain-based optimization
* `genomic`: basic GWAS example queries

### Writing a query

For now, NRC queries are described natively in Scala using the NRC language defined in `src/main/scala/framework/nrc/NRC.scala`. 
A newly defined query should extend the Query trait (see Query.scala) to leverage various support functions for executing the 
stages of the pipeline. 
