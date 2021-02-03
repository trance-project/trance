package framework.generator.spark

import framework.examples.tpch._
import framework.examples.genomic._

object Sharing extends App {

  override def main(args: Array[String]){
    //AppWriter.runDataset(HybridTP53, "HybridTP53,standard", optLevel = 1)
    //AppWriter.runDataset(HybridBRCA, "HybridBRCA,standard", optLevel = 1)

	//AppWriter.runDataset(SequentialFilters, "SequentialFilters,standard", optLevel = 1)
    //AppWriter.runDataset(SharedFilters, "SharedFilters,standard", optLevel = 1)

	// AppWriter.runDatasetShred(HybridTP53, "HybridTP53,shredded", optLevel = 1)
 //    AppWriter.runDatasetShred(HybridBRCA, "HybridBRCA,shredded", optLevel = 1)

	// AppWriter.runDatasetShred(SequentialFilters, "SequentialFilters,shredded", optLevel = 1)
 //    AppWriter.runDatasetShred(SharedFilters, "SharedFilters,shredded", optLevel = 1)

    //   AppWriter.runDataset(HybridImpact, "HybridImpact,shredded", optLevel = 1)
    // AppWriter.runDataset(HybridScores, "HybridScores,shredded", optLevel = 1)
    //   AppWriter.runDatasetShred(HybridImpact, "HybridImpact,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(HybridScores, "HybridScores,shredded", optLevel = 1)

      AppWriter.runDataset(SequentialFilters, "SequentialFilters,standard", optLevel = 1)
    AppWriter.runDataset(SharedFilters, "SharedFilters,standard", optLevel = 1)
      AppWriter.runDatasetShred(SequentialFilters, "SequentialFilters,shredded", optLevel = 1)
    AppWriter.runDatasetShred(SharedFilters, "SharedFilters,shredded", optLevel = 1)

  }

}


/*
 * Generate Spark applications for the end-to-end pipeline of the biomedical benchmark.
 */
object E2EApp extends App {

  override def main(args: Array[String]){

    // standard pipeline
    AppWriter.runDataset(HybridBySampleNew, "HybridBySampleNew,standard", optLevel = 1)
    AppWriter.runDataset(SampleNetworkNew, "SampleNetworkNew,standard", optLevel = 1)
    AppWriter.runDataset(EffectBySampleNew, "EffectBySampleNew,standard", optLevel = 1)
    AppWriter.runDataset(ConnectionBySampleNew, "ConnectionBySampleNew,standard", optLevel = 1)
    AppWriter.runDataset(GeneConnectivityNew, "GeneConnectivityNew,standard", optLevel = 1)

    // shredded pipeline
    AppWriter.runDatasetShred(HybridBySampleNewS, "HybridBySampleNew,shredded", optLevel = 1)
    AppWriter.runDatasetShred(SampleNetworkNew, "SampleNetworkNew,shredded", optLevel = 1)
    AppWriter.runDatasetShred(EffectBySampleNew, "EffectBySampleNew,shredded", optLevel = 1)
    // known double label issue
    //AppWriter.runDatasetShred(ConnectionBySampleNew, "ConnectionBySampleNew,shredded", optLevel = 1)
    //AppWriter.runDatasetShred(GeneConnectivityNew, "GeneConnectivityNew,shredded", optLevel = 1)

    // AppWriter.runDatasetShred(ClinicalRunExample, "ClinicalRunExample,shredded", optLevel=1)
	  // AppWriter.runDataset(BuildOccurrences2, "Build,standard", optLevel = 1)
	  // AppWriter.runDatasetShred(BuildOccurrences1, "Build,shredded", optLevel = 1)
    // AppWriter.runDataset(HybridBySampleMuts, "HybridScore,standard", optLevel = 1)
    //AppWriter.runDatasetShred(HybridBySampleMuts, "HybridScore,shredded", optLevel = 1)

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
