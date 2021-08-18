package framework.generator.spark

import framework.examples._
import framework.examples.genomic._

object UdfTestApp extends App {

  override def main(args: Array[String]){

    AppWriter.runDatasetShred(MultiTcga, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
      //zhost = "localhost",
      zport = 8085)
  }
}
//
object UdfDevApp extends App {

  override def main(args: Array[String]){

    // runs the shredded pipeline to file
//     AppWriter.runDatasetShred(QueryMultiTcga, "ExampleTest,standard", optLevel = 1,notebk = true, zhost = "oda-compute-0-6",
//       //zhost = "localhost",
//       zport = 8085)

    AppWriter.runDatasetShred(HintUDFExample, "ExampleTest,standard", optLevel = 2, notebk = true, zhost = "oda-compute-0-6",
    zport = 8085)
  }
}