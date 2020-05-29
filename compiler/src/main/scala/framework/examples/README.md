### Example Queries

Each query directory contains a set of input relations in various formats with their respective types, 
as well as flat and nested queries that use these relations. 

* `tpch`: TPCH queries from Slender paper. Since TPCH loads data from a CSV file, there are TPCH loader specifics in this directory.
* `simple`: simple example queries used for testing
* `normalize`: queries that trigger normalization rules
* `optimize`: queries specific to exploring the domain-based optimization
* `genomic`: basic GWAS example queries

### Writing a query

NRC queries are described natively in Scala using the NRC language defined in `src/main/scala/framework/nrc/NRC.scala`. 
A newly defined query should extend the Query trait (see Query.scala) to leverage various support functions for executing the 
stages of the pipeline. 

** Disclaimer:** This is very much a prototype, so defining queries and generating code is a bit of an involved process which is specific to writing queries and generating code for benchmarking. This section will be updated as we develop a query parser and a more automated way to generate target code.

#### Example

A Query (defined in `Query.scala`) is a trait that extends the components necessary 
to execute the pipeline for code generation. When writing a query, we will create 
an object that extends this. 

If your plan is to write a set of queries based on a ceratin set of inputs, then 
it will be best to create a base trait for your query. This is what `trait TPCHBase` 
is for queries in `examples/tpch/`. This base trait should contain the schema for 
all the inputs that will be used by TPCH queries. 

If we wanted to make one of these for genomic data, we need to reference the 
loading functions we have defined for our input, describe the type of the input, 
and then make a set of variables that we will use as reference in our queries. 
See `src/main/scala/common/Types.scala` for more NRC types.

First, make a file in src/main/scala/examples/Genomic.scala. In this file, 
define a trait that will be the base for our queries:

```
package framework.examples

import framework.common._
import framework.examples.Query

trait GenomicSchema extends Query {
  
  // these are arbitrary functions that will be deprecated
  def inputTypes(shred: Boolean = false): Map[Type, String] = Map()
  def headerTypes(shred: Boolean = false): List[String] = Nil
  def inputs(tmap: Map[String, String]): String = ""

  // this overrides a function that was previously used for tpch benchmarking
  // just define how to load your inputs here
  override def loadTables(tbls: Set[String], eval: String, shred: Boolean = false): String = {
      s"""|val basepath = "src/main/scala/Data/"
          |val path = basepath + "Variants/sub_chr22.vcf"
          |val variants = loadVCF(path, spark)
          |""".stripMargin
  }


  // define the types, which would reflect the case classes from your variant loader 
  val genoType = TupleType("sample" -> String, "call" -> IntType)
  val variantType = TupleType(
    "contig" -> StringType, 
    "start" -> IntType, 
    "reference" -> StringType, 
    "alternate" -> StringType, 
    genotypes -> BagType(genoType))

  // define references to these types
  val variants = BagVarRef("Variants", BagType(variantsType))
  val vr = TupleVarRef("v", variantType)
  val gr = TupleVarRef("g", genoType)

}
```

Now that we have a base type, we can write a query over variant information. 
In the same file, we will define an object to write out the query:

```
object GenomicQuery1 extends GenomicSchema {
  
  val name = "GenomicQuery1"

  // this query just iterates over the variant set
  val query = 
    ForeachUnion(vr, variants,                          // For v in Variants
      Singleton(Tuple("contig" -> vr("contig")          //   {( contig := v.contig,  
        "start" -> vr("start"),                         //      start := v.start, 
        "reference" -> vr("reference"),                 //       reference := v.reference,
        "alternate" -> vr("alternate"),                 //       alternate := v.alternate,
        "genotypes" ->                                  //       genotypes := 
          ForeachUnion(gr, BagProject(vr, "genotypes"), //        For g in v.genotypes union
            Singleton(Tuple("sample" -> gr("sample"),   //          {(sample := g.sample,
                            "call" -> gr("call")))      //            call := g.call )}
        ))))                                            //    )}

  // finally define the query as the input to your NRC program
  val program = Program(Assignment(name, query))

}
```

Now you have written a query. You can extend the schema to use with other inputs, and 
you can define new query objects to write out more queries. 

The next step is to write and run an application that will generate the code for the query. 

#### Code Generation

For now code generation is setup to use helper functions specific to benchmarking. See `src/main/scala/generator/spark/App.scala` for examples of how the benchmark experiments were created. Here we will define our own application to call the queries defined in the above section. 

In `src/main/scala/generator/spark/` make a file called GenomicApp.scala. This will be the application you use to generate code from the above. Write the following in the application file:

```
package framework.generator.spark

import framework.examples._

object TestApp extends App {

  val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"
 
  override def main(args: Array[String]){
    // runs the standard pipeline
    AppWriter.flatDataset(GenomicQuery1, pathout, "test")
    
    // runs the shredded pipeline
    AppWriter.shredDataset(GenomicQuery1, pathout, "test", unshred = true)
  }
}
```

Now, run `sbt run` from the `compiler` folder. This should give you four application numbers. Select the application number specific to `framework.generator.spark.TestApp`. You should see the NRC written out to the console and the corresponding plan underneath.

### Execution

After you have completed the above, the code will be written to `shredder/executor/spark/src/main/scala/sparkutils/generated/`. cd to `shredder/exectuor/spark`, compile the jar like in the example (`sbt package`). And run the jar specifying your query: 

```
spark-submit --class sparkutils.generated.GenomicQuery1 \
  --master "local[*]" target/scala-2.12/sparkutils_2.12-0.1.jar
```
