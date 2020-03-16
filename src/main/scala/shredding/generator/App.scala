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
    // runExperiment1()
    runExperiment1Joins()
  }

  def runExperiment1(){
    val pathout = "experiments/exp1.1/"

    Utils.flat(Test0, pathout, "Flat,0")
    Utils.flat(Test1, pathout, "Flat,1")
    Utils.flat(Test2, pathout, "Flat,2")
    Utils.flat(Test3, pathout, "Flat,3")
    Utils.flat(Test4, pathout, "Flat,4")

    Utils.flatProj(Test0, pathout, "Flat+,0")
    Utils.flatProj(Test1, pathout, "Flat+,1")
    Utils.flatProj(Test2, pathout, "Flat+,2")
    Utils.flatProj(Test3, pathout, "Flat+,3")
    Utils.flatProj(Test4, pathout, "Flat+,4")

    Utils.flatOpt(Test0, pathout, "Flat++,0")
    Utils.flatOpt(Test1, pathout, "Flat++,1")
    Utils.flatOpt(Test2Flat, pathout, "Flat++,2")
    Utils.flatOpt(Test3Flat, pathout, "Flat++,3")
    Utils.flatOpt(Test4Flat, pathout, "Flat++,4")

    Utils.shredDomains(Test0, pathout, "ShredDom,0")
    Utils.shredDomains(Test1, pathout, "ShredDom,1")
    Utils.shredDomains(Test2, pathout, "ShredDom,2")
    Utils.shredDomains(Test3, pathout, "ShredDom,3")
    Utils.shredDomains(Test4, pathout, "ShredDom,4")

    Utils.unshredDomains(Test0, pathout, "ShredDom,0")
    Utils.unshredDomains(Test1, pathout, "ShredDom,1")
    Utils.unshredDomains(Test2, pathout, "ShredDom,2")
    Utils.unshredDomains(Test3, pathout, "ShredDom,3")
    Utils.unshredDomains(Test4, pathout, "ShredDom,4")

  }

  def runExperiment1Joins(){
    val pathout = "experiments/exp1.2"
    
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
