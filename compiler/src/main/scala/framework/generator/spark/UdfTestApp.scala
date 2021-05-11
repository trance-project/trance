package framework.generator.spark

import framework.examples._
import framework.examples.genomic._

object UdfTestApp extends App {

  override def main(args: Array[String]){

    // runs the standard pipeline
    AppWriter.runDatasetShred(ExampleQuery, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
      //zhost = "localhost",
      zport = 8085)

    // runs the shredded pipeline
<<<<<<< HEAD
   	AppWriter.runDatasetShred(ExampleQuery, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
      //zhost = "localhost",
      zport = 8085)
=======
   	AppWriter.runDatasetShred(ExampleQuery, "ExampleTest,standard", optLevel = 1)

>>>>>>> cb62a760f83f02a91e0f51b863607a8eb089d3eb
  }
}