package framework.generator.spark

import framework.examples.tpch._
import framework.examples.genomic._

/** Example Test Application 
  *
  */
object TestApp extends App {

  override def main(args: Array[String]){

    AppWriter.flatDataset(HybridBySample, "test")
    AppWriter.shredDataset(HybridBySample3, "test")
    // AppWriter.shredDataset(GeneConnectivity, "test")
    // AppWriter.writeLoader("Biospec", HybridBySample.biospecOtype, delimiter = "\t")
  }
}


/*
 * Generate Spark applications for a subset of the benchmark queries.
 */
object App {

  val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"
 
  def main(args: Array[String]){
    // runFlatToNested()
    // runNestedToNested()
    // runNestedToFlat()
    runSkewHandling()
  }

  def runFlatToNested(){
    
    // standard pipeline - no optimiztions
    AppWriter.flatDataset(Test2, "Flat,0", optLevel = 0)
    // standard pipeline - pushed projections only
    AppWriter.flatDataset(Test2, "Flat,1", optLevel = 1)
    // standard pipeline - all optimizations
    AppWriter.flatDataset(Test2Flat, "Flat,2")
    
    // shredded pipeline + unshredding
    AppWriter.shredDataset(Test2, "Shred,2", unshred=true)
  }
 
  def runNestedToNested(){
    
    // standard pipeline - all optimizations
    AppWriter.runDatasetInput(Test2FullFlat, Test2NN, "Flat,2")
    
    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2Full, Test2NN, "Shred,2", unshred=true)

  }

  def runNestedToFlat(){

    // standard pipeline - all optimizations
    AppWriter.runDatasetInput(Test2FullFlat, Test2Agg2, "Flat,Standard,2")

    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2Full, Test2Agg2, "Shred,Standard,2")  
  
  }

  def runSkewHandling(){

    // standard pipeline - all optimizations 
    AppWriter.runDatasetInput(Test2Flat, Test2NNL, "Flat,Standard,2")
    // standard pipeline - skew-handling - all optimizations 
    AppWriter.runDatasetInput(Test2Flat, Test2NNL, "Flat,Skew,2", skew = true)

    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2, Test2NNL, "Shred,Standard,2", unshred=true)
    // shredded pipeline + unshredding - skew-handling 
    AppWriter.runDatasetInputShred(Test2, Test2NNL, "Shred,Skew,2", unshred=true, skew = true)
  
  }


}
