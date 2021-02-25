### Execute generated Spark Code

Generated Spark code should be written to `src/main/scala/sparkutils/generated/`. 

Compile the executor with `sbt package`. 

### TPCH Example

To run an application that uses the TPCH inputs do, ie. Test2Spark:

```
spark-submit --class sparkutils.generated.Test2Spark \
      --master "spark://<master-ip>:7077" \
      --executor-cores 1 \
      --num-executors 2 \
      --executor-memory 1G \
      --driver-memory 2G target/scala-2.12/sparkutils_2.12-0.1.jar
```

### VCF (Genomic) Example

To run queries on VCF files you will need to point your application to two additional jars. 
Download the following, making sure you reference their full path in the `--jars` command.
* [hadoop-bam-7.8.0.jar](https://repo1.maven.org/maven2/org/seqdoop/hadoop-bam/7.8.0/hadoop-bam-7.8.0.jar)
* [htsjdk-2.9.1.jar](https://repo1.maven.org/maven2/com/github/samtools/htsjdk/2.9.1/htsjdk-2.9.1.jar)

```
spark-submit --class sparkutils.generated.Test2Spark \
      --master "spark://<master-ip>:7077" \
      --executor-cores 1 \
      --num-executors 2 \
      --executor-memory 1G \
      --jars /absolute/path/to/hadoop-bam-7.8.0.jar,/absolute/path/to/htsjdk-2.9.1.jar \ 
      --driver-memory 2G target/scala-2.12/sparkutils_2.12-0.1.jar
```

### Burden Covariance Example
This query (source code [here](https://github.com/jacmarjorie/trance/blob/burden/executor/spark/src/main/scala/sparkutils/generated/GeneBurdenCovariance.scala)) is only compatible with Scala 2.12 and [Spark 3.0.1](https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz). Start a master and a slave. To start a slave with one core and 32G of memory:
```
sh start-slave.sh spark://<master-ip>:7077 -c 1 -m 32G
```

Download the jars required to manipulate VCF files:
* [hadoop-bam-7.8.0.jar](https://repo1.maven.org/maven2/org/seqdoop/hadoop-bam/7.8.0/hadoop-bam-7.8.0.jar)
* [htsjdk-2.9.1.jar](https://repo1.maven.org/maven2/com/github/samtools/htsjdk/2.9.1/htsjdk-2.9.1.jar)

Change the hard coded paths at lines 42 and 48. Package it up: `sbt package`, and run as follows:

```
spark-submit --class sparkutils.generated. \
      --master "spark://<master-ip>:7077" \
      --executor-cores 1 \
      --num-executors 1 \
      --executor-memory 32G \
      --jars /absolute/path/to/hadoop-bam-7.8.0.jar,/absolute/path/to/htsjdk-2.9.1.jar \ 
      target/scala-2.12/sparkutils_2.12-0.1.jar >> out
```
The covariance matrix and runtime will be written to `out`.
