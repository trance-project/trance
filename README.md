## Shredder

To compile and run:
```
sbt run
```

To build an application jar to run with Spark submit:
```
SPARK_HOME=/path/to/spark
CLASSPATH="target/scala-2.12/shredder_2.12-0.1.jar:$SPARK_HOME/jars/spark-core_2.12-2.4.2.jar:$SPARK_HOME/jars/spark-sql_2.12-2.4.2.jar:spark/target/scala-2.12/sprkloader_2.12-0.1.jar"

scalac -classpath $CLASSPATH /path/to/generated/code/Query1.Scala -d Query1.jar
```

To run spark submit (example executor configuration):
```
  spark-submit --class experiments.Query1 \
      --master "spark://master-ip:7077" \
      --executor-cores 4 \
      --num-executors 10 \
      --executor-memory 24G \
      --driver-memory 32G \
      --jars spark/target/scala-2.12/sprkloader_2.12-0.1.jar \
      --driver-class-path spark/target/scala-2.12/sprkloader_2.12-0.1.jar Query1.jar
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
