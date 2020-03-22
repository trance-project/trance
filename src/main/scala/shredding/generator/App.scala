package shredding.generator

import shredding.core._
import shredding.wmcc._
import shredding.examples.genomic._
import shredding.examples.simple._
import shredding.examples.tpch._

/**
  * Generates Scala code for a provided query
  */

object App {
 
  def main(args: Array[String]){
    // runExperiment1FN()
    runExperiment1NN()
    // runExperiment2Agg()
    // runExperiment1Joins()
  }

  def runExperiment1FN(){
    val pathout = "experiments/exp1.1/"

    Utils.flat(Test0, pathout, "Flat,0")
    Utils.flat(Test0Full, pathout, "Flat,0")
    Utils.flat(Test1, pathout, "Flat,1")
    Utils.flat(Test1Full, pathout, "Flat,1")
    Utils.flat(Test2, pathout, "Flat,2")
    Utils.flat(Test2Full, pathout, "Flat,2")
    Utils.flat(Test3, pathout, "Flat,3")
    Utils.flat(Test3Full, pathout, "Flat,3")
    Utils.flat(Test4, pathout, "Flat,4")
    Utils.flat(Test4Full, pathout, "Flat,4")

    Utils.flatProj(Test0, pathout, "Flat+,0")
    Utils.flatProj(Test0Full, pathout, "Flat+,0")
    Utils.flatProj(Test1, pathout, "Flat+,1")
    Utils.flatProj(Test1Full, pathout, "Flat+,1")
    Utils.flatProj(Test2, pathout, "Flat+,2")
    Utils.flatProj(Test2Full, pathout, "Flat+,2")
    Utils.flatProj(Test3, pathout, "Flat+,3")
    Utils.flatProj(Test3Full, pathout, "Flat+,3")
    Utils.flatProj(Test4, pathout, "Flat+,4")
    Utils.flatProj(Test4Full, pathout, "Flat+,4")

    Utils.flatOpt(Test0, pathout, "Flat++,0")
    Utils.flatOpt(Test0Full, pathout, "Flat++,0")
    Utils.flatOpt(Test1, pathout, "Flat++,1")
    Utils.flatOpt(Test1Full, pathout, "Flat++,1")
    Utils.flatOpt(Test2Flat, pathout, "Flat++,2")
    Utils.flatOpt(Test2FullFlat, pathout, "Flat++,2")
    Utils.flatOpt(Test3Flat, pathout, "Flat++,3")
    Utils.flatOpt(Test3FullFlat, pathout, "Flat++,3")
    Utils.flatOpt(Test4Flat, pathout, "Flat++,4")
    Utils.flatOpt(Test4FullFlat, pathout, "Flat++,4")

    Utils.shredDomains(Test0, pathout, "Shred,0")
    Utils.shredDomains(Test1, pathout, "Shred,1")
    Utils.shredDomains(Test2, pathout, "Shred,2")
    Utils.shredDomains(Test3, pathout, "Shred,3")
    Utils.shredDomains(Test4, pathout, "Shred,4")

    Utils.shredDomains(Test0Full, pathout, "Shred,0")
    Utils.shredDomains(Test1Full, pathout, "Shred,1")
    Utils.shredDomains(Test2Full, pathout, "Shred,2")
    Utils.shredDomains(Test3Full, pathout, "Shred,3")
    Utils.shredDomains(Test4Full, pathout, "Shred,4")

    Utils.unshredDomains(Test0, pathout, "Shred,0")
    Utils.unshredDomains(Test1, pathout, "Shred,1")
    Utils.unshredDomains(Test2, pathout, "Shred,2")
    Utils.unshredDomains(Test3, pathout, "Shred,3")
    Utils.unshredDomains(Test4, pathout, "Shred,4")

    Utils.unshredDomains(Test0Full, pathout, "Shred,0")
    Utils.unshredDomains(Test1Full, pathout, "Shred,1")
    Utils.unshredDomains(Test2Full, pathout, "Shred,2")
    Utils.unshredDomains(Test3Full, pathout, "Shred,3")
    Utils.unshredDomains(Test4Full, pathout, "Shred,4")

  }
 
  def runExperiment1NN(){
    val pathout = "experiments/exp1.2"

    // Utils.flatInput(Test0, Test0NN, pathout, "Flat,0")
    // Utils.flatInput(Test1, Test1NN, pathout, "Flat,1")
    // Utils.flatInput(Test2Flat, Test2NN, pathout, "Flat,2")
    // Utils.flatInput(Test3Flat, Test3NN, pathout, "Flat,3")
    // Utils.flatInput(Test4Flat, Test4NN, pathout, "Flat,4")

    // Utils.flatProjInput(Test0, Test0NN, pathout, "Flat+,0")
    // Utils.flatProjInput(Test1, Test1NN, pathout, "Flat+,1")
    // Utils.flatProjInput(Test2Flat, Test2NN, pathout, "Flat+,2")
    // Utils.flatProjInput(Test3Flat, Test3NN, pathout, "Flat+,3")
    // Utils.flatProjInput(Test4Flat, Test4NN, pathout, "Flat+,4")

    // Utils.flatOptInput(Test0, Test0NN, pathout, "Flat++,0")
    // Utils.flatOptInput(Test1, Test1NN, pathout, "Flat++,1")
    // Utils.flatOptInput(Test2Flat, Test2NN, pathout, "Flat++,2")
    // Utils.flatOptInput(Test3Flat, Test3NN, pathout, "Flat++,3")
    // Utils.flatOptInput(Test4Flat, Test4NN, pathout, "Flat++,4")

    Utils.shredDomains(Test0NN, pathout, "Shred,0")
    Utils.shredDomains(Test1NN, pathout, "Shred,1")
    Utils.shredDomains(Test2NN, pathout, "Shred,2")
    // Utils.shredDomains(Test3NN, pathout, "Shred,3")
    // Utils.shredDomains(Test4NN, pathout, "Shred,4")

    // Utils.unshredDomains(Test0NN, pathout, "Shred,0")
    // Utils.unshredDomains(Test1NN, pathout, "Shred,1")
    // Utils.unshredDomains(Test2NN, pathout, "Shred,2")
    // Utils.unshredDomains(Test3NN, pathout, "Shred,3")
    // Utils.unshredDomains(Test4NN, pathout, "Shred,4")
  }

  def runExperiment2Agg(){
    val pathout = "experiments/exp2.1"
    // Utils.flatOptInput(Test0, Test0Agg, pathout, "Flat++,0")
    // Utils.flatOptInput(Test1, Test1Agg, pathout, "Flat++,1")
    // Utils.flatOptInput(Test2Flat, Test2NN, pathout, "Flat++,2")
    // Utils.flatOptInput(Test3Flat, Test3NN, pathout, "Flat++,3")
    // Utils.flatOptInput(Test4Flat, Test4NN, pathout, "Flat++,4")

    // Utils.shredDomainsInput(Test0, Test0Agg, pathout, "ShredDom,0")
    // Utils.shredDomainsInput(Test1, Test1Agg, pathout, "ShredDom,1")
    // Utils.shredDomainsInput(Test2, Test2NN, pathout, "ShredDom,2")
    // Utils.shredDomainsInput(Test3, Test3NN, pathout, "ShredDom,3")
    // Utils.shredDomainsInput(Test4, Test4NN, pathout, "ShredDom,4")
  }

  def runExperiment1Joins(){
    val pathout = "experiments/exp1.3"

    Utils.flat(Test0Join, pathout, "Flat,0")
    Utils.flat(Test1Join, pathout, "Flat,1")
    Utils.flat(Test2Join, pathout, "Flat,2")
    Utils.flat(Test3Join, pathout, "Flat,3")
    Utils.flat(Test4Join, pathout, "Flat,4")

    Utils.flatProj(Test0Join, pathout, "Flat+,0")
    Utils.flatProj(Test1Join, pathout, "Flat+,1")
    Utils.flatProj(Test2Join, pathout, "Flat+,2")
    Utils.flatProj(Test3Join, pathout, "Flat+,3")
    Utils.flatProj(Test4Join, pathout, "Flat+,4")

    Utils.flatOpt(Test0Join, pathout, "Flat++,0")
    Utils.flatOpt(Test1Join, pathout, "Flat++,1")
    Utils.flatOpt(Test2JoinFlat, pathout, "Flat++,2")
    Utils.flatOpt(Test3JoinFlat, pathout, "Flat++,3")
    Utils.flatOpt(Test4JoinFlat, pathout, "Flat++,4")

    Utils.shredDomains(Test0Join, pathout, "ShredDom,0")
    Utils.shredDomains(Test1Join, pathout, "ShredDom,1")
    Utils.shredDomains(Test2Join, pathout, "ShredDom,2")
    Utils.shredDomains(Test3Join, pathout, "ShredDom,3")
    Utils.shredDomains(Test4Join, pathout, "ShredDom,4")

    Utils.unshredDomains(Test0Join, pathout, "ShredDom,0")
    Utils.unshredDomains(Test1Join, pathout, "ShredDom,1")
    Utils.unshredDomains(Test2Join, pathout, "ShredDom,2")
    Utils.unshredDomains(Test3Join, pathout, "ShredDom,3")
    Utils.unshredDomains(Test4Join, pathout, "ShredDom,4")
  }
  
  def runTPCH1(){   

    // Flattning, no shredding
    //Utils.runSparkNoDomains(TPCHQuery1Full, false, true)
    // Utils.runSparkNoDomains(Test2a, false, false)

    // Run shred without domains, cannot do unshredding
    // this has a conflict with some changes in the unnesting algorithm
    // that needs fixed
    // Utils.runSparkInputNoDomains(TPCHQuery1Full, TPCHQuery4FullAgg)

    // Run shred with domains
    // the true adds unshredding
    //Utils.runSparkDomains(TPCHQuery1Full, true, true)
    // Utils.runSparkDomains(Test2, true, false)

    // Run shred with domains when a query is used as input for another
    // the true adds unshredding
    //Utils.runSparkInputDomains(TPCHQuery1Full, TPCHQuery4FullAgg, false, true)
    // Utils.runSparkInputDomains(TPCHQuery1Full, TPCHQuery4FullAgg, false, false)
  }

  /**
    Examples that generate queries that generate a non-spark scala program
  **/

  val runner = new PipelineRunner{}
  val translator = new NRCTranslator{}
  val normalizer = new Finalizer(new BaseNormalizer{})
  val tpchInputM = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
  val tpchShredM = tpchInputM ++ TPCHSchema.tpchShredInputs
 

  def run1Calc(){
    
    println("---------------------------- Query 1  ----------------------------")  
    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val qinfo = (q1.asInstanceOf[CExpr], TPCHQueries.q1name, TPCHQueries.q1data)
    Utils.runCalc(qinfo, tpchInputM)

    println("---------------------------- Query 1 Shred ----------------------------")  
    val sq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val sqinfo = (sq1.asInstanceOf[CExpr], "Shred"+TPCHQueries.q1name, TPCHQueries.sq1data)
    Utils.runCalc(sqinfo, tpchShredM)

  }
 
  def run1(){
    
    println("---------------------------- Query 1 Unnest ----------------------------")  
    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val qinfo = (q1.asInstanceOf[CExpr], TPCHQueries.q1name, TPCHQueries.q1data)
    Utils.run(qinfo, tpchInputM)

    println("---------------------------- Query 1 Shred Unnest ----------------------------")
    val sq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val sqinfo = (sq1.asInstanceOf[CExpr], "Shred"+TPCHQueries.q1name, TPCHQueries.sq1data)
    Utils.run(sqinfo, tpchShredM)

  }


}
