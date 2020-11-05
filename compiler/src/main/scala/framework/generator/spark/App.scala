package framework.generator.spark

import framework.examples.tpch._
import framework.examples.genomic._

/*
 * Generate Spark applications for the end-to-end pipeline of the biomedical benchmark.
 */
object E2EApp extends App {

  override def main(args: Array[String]){

    // standard pipeline
    AppWriter.runDataset(HybridBySampleNew, "HybridBySampleNew,standard")
    AppWriter.runDataset(SampleNetworkNew, "SampleNetworkNew,standard")
    AppWriter.runDataset(EffectBySampleNew, "EffectBySampleNew,standard")
    AppWriter.runDataset(ConnectionBySampleNew, "ConnectionBySampleNew,standard")
    AppWriter.runDataset(GeneConnectivityNew, "GeneConnectivityNew,standard")

    // shredded pipeline
    AppWriter.runDatasetShred(HybridBySampleNewS, "HybridBySampleNew,shredded")
    AppWriter.runDatasetShred(SampleNetworkNew, "SampleNetworkNew,shredded")
    AppWriter.runDatasetShred(EffectBySampleNew, "EffectBySampleNew,shredded")
    AppWriter.runDatasetShred(ConnectionBySampleNew, "ConnectionBySampleNew,shredded")
    AppWriter.runDatasetShred(GeneConnectivityNew, "GeneConnectivityNew,shredded")

  }

}

/*
 * Generate Spark applications for the level 2, narrow TPC-H benchmark queries.
 */
object App {

  val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"
  val schema = TPCHSchema.getSchema()
 
  def main(args: Array[String]){
    runFlatToNested()
    runNestedToNested()
    // runNestedToFlat()
    runSkewHandling()
  }

  def runFlatToNested(){
    
    // standard pipeline - all optimizations
    AppWriter.runDataset(Test2Flat, "Flat,2", schema = schema)
    
    // shredded pipeline + unshredding
    AppWriter.runDatasetShred(Test2, "Shred,2", unshred=false, schema = schema)

  }
 
  def runNestedToNested(){
    
    // standard pipeline - all optimizations
    // AppWriter.runDatasetInput(Test0Full, Test0NN, "Flat,0")
    AppWriter.runDatasetInput(Test2FullFlat, Test2NN, "Flat,2", schema = schema)
    
    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2Full, Test2NN, "Shred,2", unshred=false, schema = schema)
  }

  def runNestedToFlat(){

    // standard pipeline - all optimizations
    // AppWriter.runDatasetInput(Test1Full, Test1Agg1, "Flat,Standard,1")
    AppWriter.runDatasetInput(Test2FullFlat, Test2Agg2, "Flat,Standard,2", schema = schema)

    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2Full, Test2Agg2S, "Shred,Standard,2", optLevel = 1, schema = schema)
  
  }

  def runSkewHandling(){

    // standard pipeline - all optimizations 
    // AppWriter.runDatasetInput(Test2Flat, Test2NNL, "Flat,Standard,2")
    // standard pipeline - skew-handling - all optimizations 
    AppWriter.runDatasetInput(Test2Flat, Test2NNL, "Flat,Skew,2", skew = true, schema = schema)

    // shredded pipeline + unshredding
    // AppWriter.runDatasetInputShred(Test2, Test2NNL, "Shred,Standard,2", unshred=true)
    // shredded pipeline + unshredding - skew-handling 
    AppWriter.runDatasetInputShred(Test2, Test2NNL, "Shred,Skew,2", unshred = true, skew = true, schema = schema)
  
  }


}
