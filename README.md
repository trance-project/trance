## NRC Compilation Framework 

The framework is organized into two subfolders:
* `compiler` contains all the components for running the standard and shredded pipeline, both skew-unaware and skew-aware. This goes from NRC to generated code.
* `executor` contains plan operator implementations specific to the targets of the code generator. Currently, we support Spark 2.4.2. This organization avoids running the compiler with Spark-specific dependencies.

### System Requirements

The framework has been tested with `Scala 2.12` and `Spark 2.4.2`, pre-built with Scala 2.12 support. See [here](https://spark.apache.org/releases/spark-release-2-4-2.html) for more details. 

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

This example will write the Spark application code into the `executor/spark` package, which will then be compiled into an application 
jar along with additional Spark-specific dependencies. This is because it will need to 
access the same `spark.sql.implicits` as the functions inside this package. 

Run the following in your terminal:

```
mkdir -p executor/spark/src/main/scala/sparkutils/generated/
cd compilers
sbt run
```

Select to run the first application [1]. This will generate two files:
* skew-unaware, shredded pipeline: `../executor/spark/src/main/scala/sparkutils/generated/ShredTest2NNLUnshredSpark.scala`
* skew-aware, shredded pipeline: `../executor/spark/src/main/scala/sparkutils/generated/ShredTest2NNLUnshredSkewSpark.scala`

Navigate to `executor/spark`. Update `data.flat` with your configuration details, particularly replace datapath=`/path/to/shredder...` with the appropriate location of the cloned repo and compile the package:

```
cd executors/spark
sbt package
```

You can run the application with spark-submit. For example, to run the application 
corresponding to the application defined in `ShredTest2NNLUnshredSkewSpark.scala` as a local spark job do:

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
