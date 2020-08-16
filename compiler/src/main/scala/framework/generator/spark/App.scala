package framework.generator.spark

import framework.examples.tpch._
import framework.examples.genomic._

object SkewApp extends App {

  override def main(args: Array[String]){

    // AppWriter.flatDataset(SkewTest1, "SkewTest1,standard", skew = true)
    // AppWriter.flatDataset(SkewTest1, "SkewTest1,standard", skew = false)

    // AppWriter.flatDataset(SkewTest2, "SkewTest2,standard", skew = true)
    // AppWriter.flatDataset(SkewTest2, "SkewTest2,standard", skew = false)

    // AppWriter.flatDataset(SkewTest3, "SkewTest3,standard", skew = true)
    // AppWriter.flatDataset(SkewTest3, "SkewTest3,standard", skew = false)

    // AppWriter.flatDataset(SkewTest4, "SkewTest4,standard", skew = true)
    // AppWriter.flatDataset(SkewTest4, "SkewTest4,standard", skew = false)

    // AppWriter.shredDataset(SkewTest4, "SkewTest4,shredded", skew = true)
    // AppWriter.shredDataset(SkewTest4, "SkewTest4,shredded", skew = false)
    //AppWriter.flatDataset(SkewTest5, "SkewTest5,standard", skew = true)
    //AppWriter.flatDataset(SkewTest5, "SkewTest5,standard", skew = false)

    //AppWriter.shredDataset(SkewTest5, "SkewTest5,shredded", skew = true)
    //AppWriter.shredDataset(SkewTest5, "SkewTest5,shredded", skew = false)

	AppWriter.shredDataset(SkewTest5, "SkewTest5,shredded", skew = true, unshred=true)
    AppWriter.shredDataset(SkewTest5, "SkewTest5,shredded", skew = false, unshred=true)

  }

}

object OneKApp extends App {

  override def main(args: Array[String]){

    // AppWriter.flatDataset(HybridBySample, "HybridBySample,standard")
    // AppWriter.shredDataset(HybridBySample, "HybridBySample,shredded")
    // AppWriter.shredDataset(HybridBySample, "HybridBySample,shredded", unshred=true)
  
    // AppWriter.flatDataset(SampleNetworkNew100K, "SampleNetworkNew100K,standard")
    // AppWriter.shredDataset(SampleNetworkNew100K, "SampleNetworkNew100K,shredded")
    // AppWriter.shredDataset(SampleNetworkNew100K, "SampleNetworkNew100K,shredded", unshred=true)

    AppWriter.flatDataset(SampleNetworkNew100K, "SampleNetworkNew100K,standard", skew = true)
    // AppWriter.shredDataset(SampleNetworkNew100K, "SampleNetworkNew100K,shredded", skew = true)
    // AppWriter.shredDataset(SampleNetworkNew100K, "SampleNetworkNew100K,shredded", unshred=true, skew = true)
    
    // AppWriter.flatDataset(EffectBySampleNew100K, "EffectBySampleNew100K,standard")
    // AppWriter.shredDataset(EffectBySampleNew100K, "EffectBySampleNew100K,shredded")
    // AppWriter.shredDataset(EffectBySampleNew100K, "EffectBySampleNew100K,shredded", unshred=true)

    // AppWriter.flatDataset(ConnectionBySampleNew100K, "ConnectionBySampleNew100K,standard")
    // AppWriter.shredDataset(ConnectionBySampleNew100K, "ConnectionBySampleNew100K,shredded")
    // AppWriter.shredDataset(ConnectionBySampleNew100K, "ConnectionBySampleNew100K,shredded", unshred=true)

    // AppWriter.flatDataset(GeneConnectivityNew100K, "ConnectionBySampleNew100K,standard")
    // AppWriter.shredDataset(GeneConnectivityNew100K, "ConnectionBySampleNew100K,shredded")
    // AppWriter.shredDataset(GeneConnectivityNew100K, "ConnectionBySampleNew100K,shredded", unshred=true)

  }

}

object FiveKApp extends App {

  override def main(args: Array[String]){

    AppWriter.flatDataset(SampleNetworkNew, "SampleNetworkNew,standard")
    AppWriter.shredDataset(SampleNetworkNew, "SampleNetworkNew,shredded")
    AppWriter.shredDataset(SampleNetworkNew, "SampleNetworkNew,shredded", unshred=true)
    
    AppWriter.flatDataset(EffectBySampleNew, "EffectBySampleNew,standard")
    AppWriter.shredDataset(EffectBySampleNew, "EffectBySampleNew,shredded")
    AppWriter.shredDataset(EffectBySampleNew, "EffectBySampleNew,shredded", unshred=true)

    AppWriter.flatDataset(ConnectionBySampleNew, "ConnectionBySampleNew,standard")
    AppWriter.shredDataset(ConnectionBySampleNew, "ConnectionBySampleNew,shredded")
    AppWriter.shredDataset(ConnectionBySampleNew, "ConnectionBySampleNew,shredded", unshred=true)

    AppWriter.flatDataset(GeneConnectivityNew, "ConnectionBySampleNew,standard")
    AppWriter.shredDataset(GeneConnectivityNew, "ConnectionBySampleNew,shredded")
    AppWriter.shredDataset(GeneConnectivityNew, "ConnectionBySampleNew,shredded", unshred=true)

  }

}

/** Example Test Application 
  *
  */
object TestApp extends App {

  override def main(args: Array[String]){

    // AppWriter.flatDataset(OccurGroupByGene, "OccurGroupByGene,standard")
    // AppWriter.shredDataset(OccurGroupByGene, "OccurGroupByGene,shredded")
    // AppWriter.shredDataset(OccurGroupByGene, "OccurGroupByGene,shredded", unshred=true)

    // AppWriter.flatDataset(OccurGroupByCaseMid, "OccurGroupByCaseMid,standard")
    // AppWriter.shredDataset(OccurGroupByCaseMid, "OccurGroupByCaseMid,shredded")
    // AppWriter.shredDataset(OccurGroupByCaseMid, "OccurGroupByCaseMid,shredded", unshred=true)


    // AppWriter.flatDataset(OccurCNVGroupByCase, "OccurCNVGroupByCase,standardi)
    // AppWriter.shredDataset(OccurCNVGroupByCase, "OccurCNVGroupByCase,shredded")
    // AppWriter.shredDataset(OccurCNVGroupByCase, "OccurCNVGroupByCase,shredded", unshred=true)

    // AppWriter.flatDataset(OccurCNVGroupByCaseMid, "OccurCNVGroupByCaseMid,standard")
    // AppWriter.shredDataset(OccurCNVGroupByCaseMid, "OccurCNVGroupByCaseMid,shredded")
    // AppWriter.shredDataset(OccurCNVGroupByCaseMid, "OccurCNVGroupByCaseMid,shredded", unshred=true)

    // AppWriter.flatDataset(OccurCNVAggGroupByCaseMid, "OccurCNVAggGroupByCaseMid,standard")
    // AppWriter.shredDataset(OccurCNVAggGroupByCaseMid, "OccurCNVAggGroupByCaseMid,shredded")
    // AppWriter.shredDataset(OccurCNVAggGroupByCaseMid, "OccurCNVAggGroupByCaseMid,shredded", unshred=true)

    // AppWriter.flatDataset(HybridGisticCNByGene, "test")
    // AppWriter.shredDataset(HybridGisticCNByGene, "test")

    // AppWriter.flatDataset(CombineGisticCNByGene, "CombineGisticCNByGene,standard")

    // AppWriter.flatDataset(HybridBySampleV2, "HybridBySampleV2,standard")
    // AppWriter.shredDataset(HybridBySampleV2, "HybridBySampleV2,shredded")
    // AppWriter.shredDataset(HybridBySampleV2, "HybridBySampleV2,shredded", unshred=true)

    // AppWriter.flatDataset(HybridBySampleMid2, "HybridBySampleMid2,standard")
    // AppWriter.shredDataset(HybridBySampleMid2, "HybridBySampleMid2,shredded")
    // AppWriter.shredDataset(HybridBySampleMid2, "HybridBySampleMid2,shredded", unshred=true)

    // AppWriter.flatDataset(HybridPlusBySample, "test")
    // AppWriter.shredDataset(HybridPlusBySample, "test")

    // AppWriter.flatDataset(HybridBySampleNoAgg, "test")
    // AppWriter.shredDataset(HybridBySampleNoAgg, "test")

    // AppWriter.flatDataset(MappedNetwork, "MappedNetwork,standard")
    // AppWriter.shredDataset(MappedNetwork, "MappedNetwork,shredded")

    // AppWriter.flatDataset(SampleNetworkMid2, "SampleNetworkMid2,standard")
    // AppWriter.shredDataset(SampleNetworkMid2, "SampleNetworkMid2,shredded")
    // AppWriter.shredDataset(SampleNetworkMid2, "SampleNetworkMid2,shredded", unshred=true)

    // AppWriter.flatDataset(SampleNetworkMid2a, "SampleNetworkMid2a,standard")
    // AppWriter.shredDataset(SampleNetworkMid2a, "SampleNetworkMid2a,shredded")
    // AppWriter.shredDataset(SampleNetworkMid2a, "SampleNetworkMid2a,shredded", unshred=true)

    // AppWriter.flatDataset(EffectBySampleSO, "EffectBySample,standard")
    // AppWriter.shredDataset(EffectBySampleSO, "EffectBySample,shredded")
    // AppWriter.shredDataset(EffectBySampleSO, "EffectBySample,shredded", unshred=true)

    // AppWriter.flatDataset(ConnectionBySample, "ConnectionBySample,standard")
    // AppWriter.shredDataset(ConnectionBySample, "ConnectionBySample,shredded")
    // AppWriter.shredDataset(ConnectionBySample, "ConnectionBySample,shredded", unshred=true)

    // AppWriter.flatDataset(Effect2ConnectBySample, "Effect2ConnectBySample,standard")
    // AppWriter.shredDataset(Effect2ConnectBySample, "Effect2ConnectBySample,shredded")
    // AppWriter.shredDataset(Effect2ConnectBySample, "Effect2ConnectBySample,shredded", unshred=true)

    // AppWriter.flatDataset(GeneConnectivity, "GeneConnectivityAlt,standard")
    // AppWriter.shredDataset(GeneConnectivity, "GeneConnectivityAlt,shredded")
    // AppWriter.shredDataset(GeneConnectivity, "GeneConnectivityAlt,shredded", unshred=true)
  }
}

/*
 * Generate Spark applications for a subset of the benchmark queries.
 */
object App {

  val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"
 
  def main(args: Array[String]){
    // runFlatToNested()
    runNestedToNested()
    runNestedToFlat()
    //runSkewHandling()
  }

  def runFlatToNested(){
    
    // standard pipeline - no optimiztions
//    AppWriter.flatDataset(Test2, "Flat,0", optLevel = 0)
    // standard pipeline - pushed projections only
//    AppWriter.flatDataset(Test2, "Flat,1", optLevel = 1)
    // standard pipeline - all optimizations
    AppWriter.flatDataset(Test2Flat, "Flat,2")
    
    // shredded pipeline + unshredding
//    AppWriter.shredDataset(Test2, "Shred,2", unshred=true)
  }
 
  def runNestedToNested(){
    
    // standard pipeline - all optimizations
    AppWriter.runDatasetInput(Test2FullFlat, Test2NN, "Flat,2")
    
    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2Full, Test2NN, "Shred,2", unshred=true)

  }

  def runNestedToFlat(){

    // standard pipeline - all optimizations
    // AppWriter.runDatasetInput(Test0Full, Test0Agg0, "Flat,Standard,0")
    // AppWriter.runDatasetInput(Test0Full, Test0Agg0Full, "Flat,Standard,0")
    // AppWriter.runDatasetInput(Test1Full, Test1Agg1, "Flat,Standard,1")
    // AppWriter.runDatasetInput(Test1Full, Test1Agg1Full, "Flat,Standard,1")
    AppWriter.runDatasetInput(Test2FullFlat, Test2Agg2, "Flat,Standard,2")
    // AppWriter.runDatasetInput(Test2FullFlat, Test2Agg2Full, "Flat,Standard,2")
    // AppWriter.runDatasetInput(Test3FullFlat, Test3Agg3, "Flat,Standard,3")
    // AppWriter.runDatasetInput(Test3FullFlat, Test3Agg3Full, "Flat,Standard,3")
    // AppWriter.runDatasetInput(Test4FullFlat, Test4Agg4, "Flat,Standard,4")

    // shredded pipeline + unshredding
    // AppWriter.runDatasetInputShred(Test0Full, Test0Agg0, "Shred,Standard,0")
    // AppWriter.runDatasetInputShred(Test1Full, Test1Agg1S, "Shred,Projections,1")
    // AppWriter.runDatasetInputShred(Test1Full, Test1Agg1FullS, "Shred,Standard,1")
    // AppWriter.runDatasetInputShred(Test2Full, Test2Agg2S, "Shred,Standard,2")
    // AppWriter.runDatasetInputShred(Test2Full, Test2Agg2FullS, "Shred,Standard,2")
    // AppWriter.runDatasetInputShred(Test3Full, Test3Agg3S, "Shred,Projections,3")
    // AppWriter.runDatasetInputShred(Test3Full, Test3Agg3FullS, "Shred,Standard,3")
    // AppWriter.runDatasetInputShred(Test4Full, Test4Agg4S, "Shred,Projections,4") 
    // AppWriter.runDatasetInputShred(Test4Full, Test4Agg4FullS, "Shred,Standard,4")
  
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
