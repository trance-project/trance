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
    Utils.runSparkNoDomains(Test1, pathout, false, false)
    Utils.runSparkNoDomains(Test1, pathout, true, false)
    Utils.runSparkDomains(Test1, pathout, false, false)
    Utils.runSparkDomains(Test1, pathout, true, false)
    Utils.runSparkNoDomains(Test2Flat, pathout, false, false)
    Utils.runSparkNoDomains(Test2, pathout, true, false)
    Utils.runSparkDomains(Test2, pathout, false, false)
    Utils.runSparkDomains(Test2, pathout, true, false)
    Utils.runSparkNoDomains(Test3Flat, pathout, false, false)
    Utils.runSparkDomains(Test3, pathout, false, false)
    Utils.runSparkDomains(Test3, pathout, true, false)
    Utils.runSparkNoDomains(Test4Flat, pathout, false, false)
    Utils.runSparkDomains(Test4, pathout, false, false)
    Utils.runSparkDomains(Test4, pathout, true, false)
  }

  def runExperiment1Joins(){
    val pathout = "experiments/exp1.2"
    Utils.runSparkNoDomains(Test1JoinFlat, pathout, false, false)
    Utils.runSparkNoDomains(Test1Join, pathout, true, false)
    Utils.runSparkDomains(Test1Join, pathout, false, false)
    Utils.runSparkDomains(Test1Join, pathout, true, false)
    Utils.runSparkNoDomains(Test2JoinFlat, pathout, false, false)
    Utils.runSparkNoDomains(Test2Join, pathout, true, false)
    Utils.runSparkDomains(Test2Join, pathout, false, false)
    Utils.runSparkDomains(Test2Join, pathout, true, false)
    Utils.runSparkNoDomains(Test3JoinFlat, pathout, false, false)
    Utils.runSparkDomains(Test3Join, pathout, false, false)
    Utils.runSparkDomains(Test3Join, pathout, true, false)
    Utils.runSparkNoDomains(Test4JoinFlat, pathout, false, false)
    Utils.runSparkDomains(Test4Join, pathout, false, false)
    Utils.runSparkDomains(Test4Join, pathout, true, false)
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
