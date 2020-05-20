package framework.generator.spark

import framework.common._
import framework.plans._
import framework.examples.tpch._
import framework.examples.genomic._


/*
 * Generate Spark applications for benchmark queries.
 */
object App {

  val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"
 
  def main(args: Array[String]){
    // runFlatToNested()
    // runNestedToNested()
    // runNestedToFlat()
    // runSkewHandling()
    runDatasetTest()
  }

  def runFlatToNested(){

    // AppWriter.flatDataset(Test0, pathout, "Flat,0")
    // AppWriter.flatDataset(Test0Full, pathout, "Flat,0")
    // AppWriter.flatDataset(Test1, pathout, "Flat,1")
    // AppWriter.flatDataset(Test1Full, pathout, "Flat,1")
    AppWriter.flatDataset(Test2Flat, pathout, "Flat,2")
    // AppWriter.flatDataset(Test2FullFlat, pathout, "Flat,2")
    // AppWriter.flatDataset(Test3Flat, pathout, "Flat,3")
    // AppWriter.flatDataset(Test3FullFlat, pathout, "Flat,3")
    // AppWriter.flatDataset(Test4Flat, pathout, "Flat,4")
    // AppWriter.flatDataset(Test4FullFlat, pathout, "Flat,4")

    // AppWriter.shredDataset(Test0, pathout, "Shred,0")
    // AppWriter.shredDataset(Test1, pathout, "Shred,1")
    // AppWriter.shredDataset(Test2, pathout, "Shred,2")
    // AppWriter.shredDataset(Test3, pathout, "Shred,3")
    // AppWriter.shredDataset(Test4, pathout, "Shred,4")

    // AppWriter.shredDataset(Test0Full, pathout, "Shred,0")
    // AppWriter.shredDataset(Test1Full, pathout, "Shred,1")
    // AppWriter.shredDataset(Test2Full, pathout, "Shred,2")
    // AppWriter.shredDataset(Test3Full, pathout, "Shred,3")
    // AppWriter.shredDataset(Test4Full, pathout, "Shred,4")

    // AppWriter.shredDataset(Test0, pathout, "Shred,0", unshred=true)
    // AppWriter.shredDataset(Test1, pathout, "Shred,1", unshred=true)
    AppWriter.shredDataset(Test2, pathout, "Shred,2", unshred=true)
    // AppWriter.shredDataset(Test3, pathout, "Shred,3", unshred=true)
    // AppWriter.shredDataset(Test4, pathout, "Shred,4", unshred=true)

    // AppWriter.shredDataset(Test0Full, pathout, "Shred,0", unshred=true)
    // AppWriter.shredDataset(Test1Full, pathout, "Shred,1", unshred=true)
    // AppWriter.shredDataset(Test2Full, pathout, "Shred,2", unshred=true)
    // AppWriter.shredDataset(Test3Full, pathout, "Shred,3", unshred=true)
    // AppWriter.shredDataset(Test4Full, pathout, "Shred,4", unshred=true)
  }
 
  def runNestedToNested(){

    // AppWriter.runDatasetInput(Test0Full, Test0NN, pathout, "Flat,0")
    // AppWriter.runDatasetInput(Test1Full, Test1NN, pathout, "Flat,1")
    AppWriter.runDatasetInput(Test2FullFlat, Test2NN, pathout, "Flat,2")
    // AppWriter.runDatasetInput(Test3FullFlat, Test3NN, pathout, "Flat,3")
    // AppWriter.runDatasetInput(Test4FullFlat, Test4NN, pathout, "Flat,4")

    // AppWriter.runDatasetInput(Test0Full, Test0FullNN, pathout, "Flat,0")
    // AppWriter.runDatasetInput(Test1Full, Test1FullNN, pathout, "Flat,1")
    // AppWriter.runDatasetInput(Test2FullFlat, Test2FullNN, pathout, "Flat,2")
    // AppWriter.runDatasetInput(Test3FullFlat, Test3FullNN, pathout, "Flat,3")
    // AppWriter.runDatasetInput(Test4FullFlat, Test4FullNN, pathout, "Flat,4")

    // AppWriter.runDatasetInputShred(Test0Full, Test0NN, pathout, "Shred,0")
    // AppWriter.runDatasetInputShred(Test1Full, Test1NN, pathout, "Shred,1")
    // AppWriter.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2")
    // AppWriter.runDatasetInputShred(Test3Full, Test3NN, pathout, "Shred,3")
    // AppWriter.runDatasetInputShred(Test4Full, Test4NN, pathout, "Shred,4")

    // AppWriter.runDatasetInputShred(Test0Full, Test0NN, pathout, "Shred,0", unshred=true)
    // AppWriter.runDatasetInputShred(Test1Full, Test1NN, pathout, "Shred,1", unshred=true)
    AppWriter.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2", unshred=true)
    // AppWriter.runDatasetInputShred(Test3Full, Test3NN, pathout, "Shred,3", unshred=true)
    // AppWriter.runDatasetInputShred(Test4Full, Test4NN, pathout, "Shred,4", unshred=true)

    // AppWriter.runDatasetInputShred(Test0Full, Test0FullNN, pathout, "Shred,0")
    // AppWriter.runDatasetInputShred(Test1Full, Test1FullNN, pathout, "Shred,1")
    // AppWriter.runDatasetInputShred(Test2Full, Test2FullNN, pathout, "Shred,2")
    // AppWriter.runDatasetInputShred(Test3Full, Test3FullNN, pathout, "Shred,3")
    // AppWriter.runDatasetInputShred(Test4Full, Test4FullNN, pathout, "Shred,4")

    // AppWriter.runDatasetInputShred(Test0Full, Test0FullNN, pathout, "Shred,0", unshred=true)
    // AppWriter.runDatasetInputShred(Test1Full, Test1FullNN, pathout, "Shred,1", unshred=true)
    // AppWriter.runDatasetInputShred(Test2Full, Test2FullNN, pathout, "Shred,2", unshred=true)
    // AppWriter.runDatasetInputShred(Test3Full, Test3FullNN, pathout, "Shred,3", unshred=true)
    // AppWriter.runDatasetInputShred(Test4Full, Test4FullNN, pathout, "Shred,4", unshred=true)
  }

  def runNestedToFlat(){
    // AppWriter.runDatasetInput(Test2FullFlat, Test2Agg2, pathout, "Flat,Standard,2")
    // AppWriter.runDatasetInput(Test2FullFlat, Test2Agg2, pathout, "Flat,Skew,2", skew = true)

    AppWriter.runDatasetInputShred(Test2Full, Test2Agg2, pathout, "Shred,Standard,2")
    AppWriter.runDatasetInputShred(Test2Full, Test2Agg2, pathout, "Shred,Skew,2", skew = true)    
  }

  def runSkewHandling(){

    // AppWriter.runDatasetInput(Test2Flat, Test2NNL, pathout, "Flat,Standard,2")
    // AppWriter.runDatasetInput(Test2Flat, Test2NNL, pathout, "Flat,Skew,2", skew = true)

    // AppWriter.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Standard,2")
    // AppWriter.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Skew,2", skew = true)

    AppWriter.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Standard,2", unshred=true)
    AppWriter.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Skew,2", unshred=true, skew = true)
  }

  def runDatasetTest(){
    // AppWriter.flatDataset(Test2, pathout, "test")
    // AppWriter.flatDataset(OddsRatio, pathout, "test")
    AppWriter.shredDataset(OddsRatio, pathout, "test")
  }


}
