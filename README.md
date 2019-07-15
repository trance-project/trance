## Shredder

To compile and run:
```
sbt run
```

To compile and run generated code for Spark application:
```
CLASSPATH="target/scala-2.12/shredder_2.12-0.1.jar:$SPARK_HOME/jars/spark-core_2.12-2.4.0.jar:$SPARK_HOME/jars/spark-sql_2.12-2.4.0.jar"

scalac -classpath $CLASSPATH -d tests src/test/scala/shredding/examples/$1/$2.Scala

spark-submit --class experiments.$2 \
  --master "local[*]" \
  --driver-class-path tests:target/scala-2.12/shredder_2.12-0.1.jar target/scala-2.12/shredder_2.12-0.1.jar
```


Organization of `src/main/scala/shredding`:
* `common`: types, operators, and variable definitions that are shared across the pipeline
* `nrc`: soure nrc, target nrc, nrc extensions for shredding (dictionaries, labels), shredding, and linearization  
* `runtime`: runtime specifics for nrc and shredded nrc 
* `wmcc`: weighted monad comprehension calculus (wmcc), which algebra operators (select, reduce, join, etc.).
* `generator`: code generators, currently just native scala
* `loader`: functionality to load data for executing queries, currently csv readers used for reading tpch from a file
* `examples`: a directory of example queries 
* `utils`: utility methods used throughout
