package framework.generator.spark

import framework.examples._
import framework.examples.genomic._

object UdfTestApp extends App {

  override def main(args: Array[String]){

    // runs the standard pipeline
    AppWriter.runDataset(PMB, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
      //zhost = "localhost",
      zport = 8085)

    // runs the shredded pipeline
   	AppWriter.runDatasetShred(PMB, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
      //zhost = "localhost",
      zport = 8085)
  }
}