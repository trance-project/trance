package framework.generator.spark

import framework.examples.tpch._
import framework.examples.genomic._


/*
 * Generate Spark applications for a subset of the benchmark queries.
 */
object App {

  val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"
 
  def main(args: Array[String]){
    runFlatToNested()
    // runNestedToNested()
    // runNestedToFlat()
    // runSkewHandling()
  }

  def runFlatToNested(){
    // AppWriter.flatDataset(Test2, pathout, "Flat,0", optLevel = 0)
    // AppWriter.flatDataset(Test2, pathout, "Flat,1")
    AppWriter.flatDataset(Test2Flat, pathout, "Flat,2")
    AppWriter.shredDataset(Test2, pathout, "Shred,2", unshred=true)

  }
 
  def runNestedToNested(){

    AppWriter.runDatasetInput(Test2FullFlat, Test2NN, pathout, "Flat,2")
    AppWriter.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2", unshred=true)

  }

  def runNestedToFlat(){

    AppWriter.runDatasetInput(Test2FullFlat, Test2Agg2, pathout, "Flat,Standard,2")
    AppWriter.runDatasetInputShred(Test2Full, Test2Agg2, pathout, "Shred,Standard,2")  
  
  }

  def runSkewHandling(){

    AppWriter.runDatasetInput(Test2Flat, Test2NNL, pathout, "Flat,Standard,2")
    AppWriter.runDatasetInput(Test2Flat, Test2NNL, pathout, "Flat,Skew,2", skew = true)

    AppWriter.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Standard,2", unshred=true)
    AppWriter.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Skew,2", unshred=true, skew = true)
  
  }


}
