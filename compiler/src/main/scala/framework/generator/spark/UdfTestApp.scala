package framework.generator.spark

import framework.examples._
import framework.examples.genomic._

object UdfTestApp extends App {

  override def main(args: Array[String]){

    // runs the standard pipeline
//    AppWriter.runDatasetShred(ExampleQueryImpact, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
//      //zhost = "localhost",
//      zport = 8085)
    AppWriter.runDatasetShred(ExampleQueryMultiTcga, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
      //zhost = "localhost",
      zport = 8085)
//    AppWriter.runDatasetShred(ExampleQuery, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
//      //zhost = "localhost",
//      zport = 8085)
//    AppWriter.runDatasetShred(ExampleQuery, "ExampleTest,standard", optLevel = 1, notebk = true, zhost = "oda-compute-0-6",
//      //zhost = "localhost",
//      zport = 8085)

    //AppWriter.runDatasetShred(ExampleQuery, "ExampleTest,standard", optLevel = 1)

  }
}
//
object UdfDevApp extends App {

  override def main(args: Array[String]){

    // runs the shredded pipeline to file
     AppWriter.runDatasetShred(ExampleQueryMultiTcga9, "ExampleTest,standard", optLevel = 1,notebk = true, zhost = "oda-compute-0-6",
       //zhost = "localhost",
       zport = 8085)

//    AppWriter.runDatasetShred(PivotUDFExample, "ExampleTest,standard", optLevel = 1,notebk = true, zhost = "oda-compute-0-6",
//      //zhost = "localhost",
//    zport = 8085)
  }
}