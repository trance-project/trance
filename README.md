## NRC Compilation Framework 

The framework is organized into two subfolders:
* `compiler` contains all the components for running the standard and shredded pipeline, both skew-unaware and skew-aware. This goes from NRC to generated code.
* `executor` contains plan operator implementations specific to the targets of the code generator. Currently, we support Spark 2.4.2. This organization avoids running the compiler with Spark-specific dependencies.

### System Requirements

The framework has been tested with `Scala 2.12` and `Spark 2.4.2`, pre-built with Scala 2.12 support. See [here](https://spark.apache.org/releases/spark-release-2-4-2.html) for more details. 

### Example Usage

NRC queries can currently be described directly in Scala, see `compilers/src/main/scala/framework/examples/`
for examples on how to do this. Here we describe how to generate and 
execute code for an example from the TPC-H benchmark.

This example generates code into the `executor/spark` package which will then be compiled into an application 
jar along with additional Spark-specific dependencies.

To start run the following in your terminal:

```
mkdir -p executor/spark/src/main/scala/sparkutils/generated/
cd compilers
sbt run
```

Select to run the first application [1]. This will generate two files:
* standard pipeline: `../executor/spark/src/main/scala/sparkutils/generated/Test2Spark.scala`
* shredded pipeline:`../executor/spark/src/main/scala/sparkutils/generated/ShredTest2UnshredSpark.scala`

Note that these files are generated inside the executor package. This is because it will need to 
access the same `spark.sql.implicits` as the functions inside this package. 

Navigate to `executor/spark`. Update `data.flat` with your configuration details, particularly replace datapath=`/path/to/shredder...` with the appropriate location of the cloned repo and compile the package:

```
cd executors/spark
sbt package
```

The application can now be ran with spark-submit. For example, to run the 
application defined in `Test2Spark.scala` as a local spark job do:

```
spark-submit --class sparkutils.generated.Test2Spark \
  --master "local[*]" target/scala-2.12/sparkutils_2.12-0.1.jar
```

If you would like your output to print to console, you can edit the generated 
application jar to do so. Just uncomment the line `Test2.print`, then redirect to 
stdout: 

```
spark-submit --class sparkutils.generated.Test2Spark \
  --master "local[*]" target/scala-2.12/sparkutils_2.12-0.1.jar > out
```
