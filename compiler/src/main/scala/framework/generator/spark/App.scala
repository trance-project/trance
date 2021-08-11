package framework.generator.spark

import java.io._
import framework.common._
import framework.plans._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.examples.Query
import framework.loader.csv._

object SkewTest extends App {
  override def main(args: Array[String]){
    AppWriter.runDataset(SkewTest2, "SkewTest2Genes,standard", optLevel = 1)
    AppWriter.runDataset(SkewTest2, "SkewTest2Genes,standard", optLevel = 1, skew = true)
  }
}

object ScaleTest extends App {
  override def main(args: Array[String]){
    AppWriter.runDatasetShred(TestBaseQuery, "TestBaseQuery,shredded", optLevel = 1)
  }
}


object TestZeppelin extends App {
  override def main(args: Array[String]){
    AppWriter.runDataset(ExampleQuery, "ExampleQuery,standard", optLevel = 1, notebk = true,
      zhost = "localhost", zport = 8085)
  }
}

object LetExplore extends App {
  override def main(args: Array[String]){

    val dlbctest = 5000
    // val lenv = new LetTestEnv(dlbctest, shred=true)
    // val ests = lenv.estimates
    // AppWriter.runDatasetShred(LetTest5Seq, "Sequential,shredded", optLevel = 1)

    // AppWriter.runDatasetShred(LetTest5, "InlinedAgg,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(LetTest5Seq, "SequentiaAgg,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(LetTest5b, "InlinedNoAgg,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(LetTest5bSeq, "SequentialNoAgg,shredded", optLevel = 1)

    // AppWriter.runDatasetShred(LetTest7a, "InlinedNoAgg,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(LetTest7aSeq, "SequentialNoAgg,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(LetTest7b, "InlinedAgg,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(LetTest7bSeq, "SequentialAgg,shredded", optLevel = 1)

    AppWriter.runDatasetShred(LetTest8, "InlinedAgg,shredded", optLevel = 1)
    AppWriter.runDatasetShred(LetTest8Seq, "SequentialAgg,shredded", optLevel = 1)

    // AppWriter.runDatasetShred(lenv.queries.head, "Inlined,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(lenv.queries.last, "Sequential,shredded", optLevel = 1)
  }
}

object Sharing extends App {

  override def main(args: Array[String]){

    // val dlbctest = 371520
    // total is ~4500 so way underestimated
    // upper bound
    // This is the total size: 418119.25389783055
    // val dlbctest = 200000
    // in kilobytes
    val dataSizes = Map[String, Double]("clinical" -> 11.4, 
      "copynumber" -> 390100.0, 
      // "occurrences" -> 51000, 
      "odict1" -> 3000.0,
      "odict2" -> 93600.0, 
      "odict3" -> 11400.0, 
      "samples" -> 166800.0)

    /**
    val dataSizes = Map[String, Double](
      "clinical" -> 1407.9, 
      "copynumber" -> 61300000, 
      // "occurrences" -> 51000, 
      // "odict1" -> 3000.0,
      // "odict2" -> 93600.0, 
      // "odict3" -> 11400.0, 
      "samples" -> 34288.44)
    **/

    // allocate half the space to the cache
    // val cachesize = (dataSizes.foldLeft(0.0)(_+_._2)  * .5).asInstanceOf[Int]
    val cachesize = 20
    println("this is the cache size: "+cachesize)
    // val dlbctest = 4000
    val brcatest = 8000000

    // capacity in KB
    val flex = 3
    // val flex = 0
    val ptype = "dynamic"
    // val ptype = "greedy"
    // val ptype = "qlearn"
    val sgenv = new GenomicEnv(cachesize, shred = true, flex = flex, ptype = ptype)
    AppWriter.runWithCache(sgenv, "CacheTest,shredded,covers")
    AppWriter.runWithCache(sgenv, "CacheTest,shredded,covers", cache = true)
    // AppWriter.runDatasetShred(SW6, "TestCompile,shredded", optLevel = 1)



  }

}


/*
 * Generate Spark applications for the end-to-end pipeline of the biomedical benchmark.
 */
object E2EApp extends App {

  override def main(args: Array[String]){

    // standard pipeline
    // AppWriter.runDataset(HybridBySampleNew, "HybridBySampleNew,standard", optLevel = 1)
    AppWriter.runDataset(SampleNetworkNew, "SampleNetworkNew,standard", optLevel = 1)
    // AppWriter.runDataset(EffectBySampleNew, "EffectBySampleNew,standard", optLevel = 1)
    // AppWriter.runDataset(ConnectionBySampleNew, "ConnectionBySampleNew,standard", optLevel = 1)
    // AppWriter.runDataset(GeneConnectivityNew, "GeneConnectivityNew,standard", optLevel = 1)

    // AppWriter.runDatasetShred(HybridBySampleNewS, "HybridBySampleNew,shredded", optLevel = 1)
    AppWriter.runDatasetShred(SampleNetworkNew, "SampleNetworkNew,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(EffectBySampleNew, "EffectBySampleNew,shredded", optLevel = 1)
    // known double label issue
    //AppWriter.runDatasetShred(ConnectionBySampleNew, "ConnectionBySampleNew,shredded", optLevel = 1)
    //AppWriter.runDatasetShred(GeneConnectivityNew, "GeneConnectivityNew,shredded", optLevel = 1)

    // AppWriter.runDatasetShred(ClinicalRunExample, "ClinicalRunExample,shredded", optLevel=1)
    // AppWriter.runDataset(BuildOccurrences2, "Build,standard", optLevel = 1)
    // AppWriter.runDatasetShred(BuildOccurrences1, "Build,shredded", optLevel = 1)
    // AppWriter.runDataset(HybridBySampleMuts, "HybridScore,standard", optLevel = 1)
    //AppWriter.runDatasetShred(HybridBySampleMuts, "HybridScore,shredded", optLevel = 1)
    //AppWriter.runDataset(ClinicalRunExample, "Likelihood,standard", optLevel = 1)
    // AppWriter.runDatasetShred(ClinicalRunExample, "Likelihood,shredded", optLevel = 1)
    // AppWriter.runDatasetShred(OccurCNVAggGroupByCaseMid, "Test,shredded", optLevel = 1)

  }

}

/*
 * Generate Spark applications for the level 2, narrow TPC-H benchmark queries.
 */
object App {

  val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"
  val schema = TPCHSchema.getSchema()

  def main(args: Array[String]){
    AppWriter.runDatasetShred(MultiOmicsProstate, "bugfix", optLevel = 2, schema = schema, unshred=true)
    // runFlatToNested()
    // runNestedToNested()
    // runNestedToFlat()
    // runSkewHandling()
  }

  def runFlatToNested(){

    // standard pipeline - all optimizations
    AppWriter.runDataset(Test2Flat, "Flat,2", schema = schema)
    
    // shredded pipeline + unshredding
    AppWriter.runDatasetShred(Test2, "Shred,2", unshred=true, schema = schema)

  }

  def runNestedToNested(){

    // standard pipeline - all optimizations
    AppWriter.runDatasetInput(Test2FullFlat, Test2NN, "Flat,2", schema = schema)

    // shredded pipeline + unshredding
    AppWriter.runDatasetInputShred(Test2Full, Test2NN, "Shred,2", unshred=true, schema = schema)
  }

  def runNestedToFlat(){

    // standard pipeline - all optimizations
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
