# TraNCE
### TRAnslating Nested Collections Efficiently: a compilation framework for processing nested collection queries

The framework is organized into two subfolders:
* `compiler` contains all the components for running the standard and shredded pipeline, both skew-unaware and skew-aware. This goes from NRC to generated code.
* `executor` contains plan operator implementations specific to the targets of the code generator. Currently, we support Spark 2.4.2. This organization avoids running the compiler with Spark-specific dependencies.

### Supplementary Materials

Supplementary materials can be found in the base directory `supplementary.pdf`. Also in the base 
directory, `external.tar.gz` contains the source files for TPC-H benchmark queries written in external systems. 

### System Requirements

The framework has been tested with `Scala 2.12` and `Spark 2.4.2`, pre-built with Scala 2.12 support. See [spark.apache.org](https://spark.apache.org/releases/spark-release-2-4-2.html) for more details. 

### Example Usage

This example runs the skew-unaware and skew-aware shredded pipeline for the running example:

```
For c in COP Union
  {( c_name := c.c_name,
     c_orders := For o in c.c_orders Union
    {( o_orderdate := o.o_orderdate,
       o_parts := ReduceByKey([p_name], [l_quantity],
      For l in o.o_parts Union
        For p in P Union
          If (l.l_partkey = p.p_partkey) Then
            {( p_name := p.p_name,
               l_quantity := l.l_quantity )}
```

NRC queries can currently be described directly in Scala. The above is the running example as defined 
in Scala [here](https://github.com/jacmarjorie/shredder/blob/master/compiler/src/main/scala/framework/examples/tpch/NestedToNested.scala#L286-L309). `compilers/src/main/scala/framework/examples/` contains more examples on how to do this.

This example writes the Spark application code into the `executor/spark` package, 
which will then be compiled into an application 
jar along with additional Spark-specific dependencies. This is because it will need to 
access the same `spark.sql.implicits` as the functions inside this package. 

Run sbt in the `compiler` directory:

```
cd compiler
sbt run
```

Select to run the first application `[1] framework.generator.spark.App`. This will generate two files:
* skew-unaware, shredded pipeline: `../executor/spark/src/main/scala/sparkutils/generated/ShredTest2NNLUnshredSpark.scala`
* skew-aware, shredded pipeline: `../executor/spark/src/main/scala/sparkutils/generated/ShredTest2NNLUnshredSkewSpark.scala`

Navigate to `executor/spark`. This example assumes that the application 
will be ran from this directory; if you execute from any other directory, 
update the `datapath` configuration variable in `data.flat` 
to point to `executor/spark/data/tpch` directory. See [below](#additional-configurations) 
for further details on config parameters.

To continue, compile the package:

```
cd executor/spark
echo "datapath=$PWD/data/tpch" > data.flat
sbt package
```

Once compiled, the application can be compiled with spark-submit. For example, to run the application 
corresponding to `ShredTest2NNLUnshredSkewSpark.scala` as a local spark job do:

```
spark-submit --class sparkutils.generated.ShredTest2NNLUnshredSkewSpark \
  --master "local[*]" target/scala-2.12/sparkutils_2.12-0.1.jar
```

If you would like to print to console, you can edit the generated 
application file - just uncomment the line `Test2NNL.print` at the 
bottom of the application file, then redirect to stdout: 

```
spark-submit --class sparkutils.generated.ShredTest2NNLUnshredSkewSpark \
  --master "local[*]" target/scala-2.12/sparkutils_2.12-0.1.jar > out
```

### Additional Configurations

The file `data.flat` is read by `executor/spark/src/main/scala/sparkutils/Config.scala`. 
The below table defines the additional config parameters.

| Parameter | Type | Description | Default |
| ---- | ---- | --- | --- |
| datapath | String | Path to TPC-H `.tbl` files. Only required for TPC-H queries. | `/path/to/executor/spark/data/tpch` |
| minPartitions | Int | The minimum number of partitions. | 400 |
| maxPartitions | Int | The maximum number of partitions. Also used for `spark.sql.shuffle`. | 1000 |
| heavyKeyStrategy | String | The strategy for heavy key calculuation. Can be either `sample` or `slice`. | `sample` |
| sample | Int | The number of values to slice in the `slice` procedure. | 1000 |
| threshold | Double | Threshold used for heavy key procedure. For `sample` this is the percent of values that should be reached to be considered heavy. For `slice` this is the scale above average to be reached to be considered heavy. | .0025 |


