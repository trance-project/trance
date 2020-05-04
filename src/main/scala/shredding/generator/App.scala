package shredding.generator

import shredding.core._
import shredding.plans._
import shredding.examples.tpch._


/*
 * Full pipeline runner: standard and shredded
 *
 */

object App {
 
  def main(args: Array[String]){
    runFlatToNested()
    runNestedToNested()
    // TODO
    runNestedToFlat()
    // runSkewHandling()
  }

  def runFlatToNested(){
    val pathout = "spark/src/main/scala/sprkloader/experiments"

    // Utils.flatDataset(Test0, pathout, "Flat++,0")
    // Utils.flatDataset(Test0Full, pathout, "Flat++,0")
    // Utils.flatDataset(Test1, pathout, "Flat++,1")
    // Utils.flatDataset(Test1Full, pathout, "Flat++,1")
    Utils.flatDataset(Test2, pathout, "Flat++,2")
    // Utils.flatDataset(Test2Flat, pathout, "Flat++,2")
    // Utils.flatDataset(Test2FullFlat, pathout, "Flat++,2")
    // Utils.flatDataset(Test3Flat, pathout, "Flat++,3")
    // Utils.flatDataset(Test3FullFlat, pathout, "Flat++,3")
    // Utils.flatDataset(Test4Flat, pathout, "Flat++,4")
    // Utils.flatDataset(Test4FullFlat, pathout, "Flat++,4")

    // Utils.shredDataset(Test0, pathout, "Shred,0")
    // Utils.shredDataset(Test1, pathout, "Shred,1")
    // Utils.shredDataset(Test2, pathout, "Shred,2")
    // Utils.shredDataset(Test3, pathout, "Shred,3")
    // Utils.shredDataset(Test4, pathout, "Shred,4")

    // Utils.shredDataset(Test0Full, pathout, "Shred,0")
    // Utils.shredDataset(Test1Full, pathout, "Shred,1")
    // Utils.shredDataset(Test2Full, pathout, "Shred,2")
    // Utils.shredDataset(Test3Full, pathout, "Shred,3")
    // Utils.shredDataset(Test4Full, pathout, "Shred,4")

    // Utils.shredDataset(Test0, pathout, "Shred,0", unshred=true)
    // Utils.shredDataset(Test1, pathout, "Shred,1", unshred=true)
    Utils.shredDataset(Test2, pathout, "Shred,2", unshred=true)
    // Utils.shredDataset(Test3, pathout, "Shred,3", unshred=true)
    // Utils.shredDataset(Test4, pathout, "Shred,4", unshred=true)

    // Utils.shredDataset(Test0Full, pathout, "Shred,0", unshred=true)
    // Utils.shredDataset(Test1Full, pathout, "Shred,1", unshred=true)
    // Utils.shredDataset(Test2Full, pathout, "Shred,2", unshred=true)
    // Utils.shredDataset(Test3Full, pathout, "Shred,3", unshred=true)
    // Utils.shredDataset(Test4Full, pathout, "Shred,4", unshred=true)
  }
 
  def runNestedToNested(){
    val pathout = "spark/src/main/scala/sprkloader/experiments"

    // Utils.runDatasetInput(Test0Full, Test0NN, pathout, "Flat++,0")
    // Utils.runDatasetInput(Test1Full, Test1NN, pathout, "Flat++,1")
    Utils.runDatasetInput(Test2FullFlat, Test2NN, pathout, "Flat++,2")
    // Utils.runDatasetInput(Test3FullFlat, Test3NN, pathout, "Flat++,3")
    // Utils.runDatasetInput(Test4FullFlat, Test4NN, pathout, "Flat++,4")

    // Utils.runDatasetInput(Test0Full, Test0FullNN, pathout, "Flat++,0")
    // Utils.runDatasetInput(Test1Full, Test1FullNN, pathout, "Flat++,1")
    // Utils.runDatasetInput(Test2FullFlat, Test2FullNN, pathout, "Flat++,2")
    // Utils.runDatasetInput(Test3FullFlat, Test3FullNN, pathout, "Flat++,3")
    // Utils.runDatasetInput(Test4FullFlat, Test4FullNN, pathout, "Flat++,4")

    // Utils.runDatasetInputShred(Test0Full, Test0NN, pathout, "Shred,0")
    // Utils.runDatasetInputShred(Test1Full, Test1NN, pathout, "Shred,1")
    // Utils.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2")
    // Utils.runDatasetInputShred(Test3Full, Test3NN, pathout, "Shred,3")
    // Utils.runDatasetInputShred(Test4Full, Test4NN, pathout, "Shred,4")

    // Utils.runDatasetInputShred(Test0Full, Test0NN, pathout, "Shred,0", unshred=true)
    // Utils.runDatasetInputShred(Test1Full, Test1NN, pathout, "Shred,1", unshred=true)
    Utils.runDatasetInputShred(Test2Full, Test2NN, pathout, "Shred,2", unshred=true)
    // Utils.runDatasetInputShred(Test3Full, Test3NN, pathout, "Shred,3", unshred=true)
    // Utils.runDatasetInputShred(Test4Full, Test4NN, pathout, "Shred,4", unshred=true)

    // Utils.runDatasetInputShred(Test0Full, Test0FullNN, pathout, "Shred,0")
    // Utils.runDatasetInputShred(Test1Full, Test1FullNN, pathout, "Shred,1")
    // Utils.runDatasetInputShred(Test2Full, Test2FullNN, pathout, "Shred,2")
    // Utils.runDatasetInputShred(Test3Full, Test3FullNN, pathout, "Shred,3")
    // Utils.runDatasetInputShred(Test4Full, Test4FullNN, pathout, "Shred,4")

    // Utils.runDatasetInputShred(Test0Full, Test0FullNN, pathout, "Shred,0", unshred=true)
    // Utils.runDatasetInputShred(Test1Full, Test1FullNN, pathout, "Shred,1", unshred=true)
    // Utils.runDatasetInputShred(Test2Full, Test2FullNN, pathout, "Shred,2", unshred=true)
    // Utils.runDatasetInputShred(Test3Full, Test3FullNN, pathout, "Shred,3", unshred=true)
    // Utils.runDatasetInputShred(Test4Full, Test4FullNN, pathout, "Shred,4", unshred=true)
  }

  def runNestedToFlat(){
    val pathout = "spark/src/main/scala/sprkloader/experiments"
    // Utils.runDatasetInput(Test2FullFlat, Test2Agg2, pathout, "Flat++,Standard,2")
    // Utils.runDatasetInput(Test2FullFlat, Test2Agg2, pathout, "Flat++,Skew,2", skew = true)

    Utils.runDatasetInputShred(Test2Full, Test2Agg2, pathout, "Shred,Standard,2")
    Utils.runDatasetInputShred(Test2Full, Test2Agg2, pathout, "Shred,Skew,2", skew = true)    
  }

  def runSkewHandling(){
    val pathout = "spark/src/main/scala/sprkloader/experiments"

    Utils.runDatasetInput(Test2Flat, Test2NNL, pathout, "Flat++,Standard,2")
    Utils.runDatasetInput(Test2Flat, Test2NNL, pathout, "Flat++,Skew,2", skew = true)

    Utils.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Standard,2")
    Utils.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Skew,2", skew = true)

    Utils.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Standard,2", unshred=true)
    Utils.runDatasetInputShred(Test2, Test2NNL, pathout, "Shred,Skew,2", unshred=true, skew = true)
  }

}
