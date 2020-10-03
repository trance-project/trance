package framework.generator.spark

import framework.examples.tpch._
import framework.examples.genomic._

/*
 * Generate Spark applications for the end-to-end pipeline of the biomedical benchmark.
 */
object E2EApp extends App {

  override def main(args: Array[String]){

    // standard pipeline
    AppWriter.flatDataset(HybridBySampleNew, "HybridBySampleNew,standard")
    AppWriter.flatDataset(SampleNetworkNew, "SampleNetworkNew,standard")
    AppWriter.flatDataset(EffectBySampleNew, "EffectBySampleNew,standard")
    AppWriter.flatDataset(ConnectionBySampleNew, "ConnectionBySampleNew,standard")
    AppWriter.flatDataset(GeneConnectivityNew, "GeneConnectivityNew,standard")

    // shredded pipeline
    AppWriter.shredDataset(HybridBySampleNewS, "HybridBySampleNew,shredded")
    AppWriter.shredDataset(SampleNetworkNew, "SampleNetworkNew,shredded")
    AppWriter.shredDataset(EffectBySampleNew, "EffectBySampleNew,shredded")
    AppWriter.shredDataset(ConnectionBySampleNew, "ConnectionBySampleNew,shredded")
    AppWriter.shredDataset(GeneConnectivityNew, "GeneConnectivityNew,shredded")

  }

}

/*
 * Generate Spark applications for the level 2, narrow TPC-H benchmark queries.
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
    
    // standard pipeline - all optimizations
    AppWriter.runDataset(Test2Flat, "Flat,2")
    
    // shredded pipeline + unshredding
    AppWriter.runDatasetShred(Test2, "Shred,2", unshred=true)

  }
 
  def runNestedToNested(){
    
    // standard pipeline - all optimizations
    // AppWriter.runDatasetInput(Test0Full, Test0NN, "Flat,0")
    AppWriter.runDatasetInput(Test2FullFlat, Test2NN, "Flat,2")
    
    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2Full, Test2NN, "Shred,2", unshred=true)
  }

  def runNestedToFlat(){

    // standard pipeline - all optimizations
    // AppWriter.runDatasetInput(Test1Full, Test1Agg1, "Flat,Standard,1")
    AppWriter.runDatasetInput(Test2FullFlat, Test2Agg2, "Flat,Standard,2")

    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2Full, Test2Agg2S, "Shred,Standard,2")
  
  }

  def runSkewHandling(){

    val schema = TPCHSchema.getSchema()

    // standard pipeline - all optimizations 
    // AppWriter.runDatasetInput(Test2Flat, Test2NNL, "Flat,Standard,2")
    // standard pipeline - skew-handling - all optimizations 
    AppWriter.runDatasetInput(Test2Flat, Test2NNL, "Flat,Skew,2", skew = true, schema = Some(schema))

    // shredded pipeline + unshredding
    // AppWriter.runDatasetInputShred(Test2, Test2NNL, "Shred,Standard,2", unshred=true)
    // shredded pipeline + unshredding - skew-handling 
    AppWriter.runDatasetInputShred(Test2, Test2NNL, "Shred,Skew,2", unshred = true, skew = true, schema = Some(schema))
  
  }


}
