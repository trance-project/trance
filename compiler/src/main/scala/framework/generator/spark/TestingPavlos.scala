package framework.generator.spark

import framework.examples._
import framework.examples.genomic._

object TestingPavlos extends App {

  override def main(args: Array[String]){

    // runs the standard pipeline
    AppWriter.runDataset(TestQuery, "TestExample,standard", optLevel = 1)

    // runs the shredded pipeline
    AppWriter.runDatasetShred(TestQuery, "TestExample,standard", optLevel = 1)
  }
}