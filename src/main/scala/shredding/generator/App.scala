package shredding.generator

import shredding.wmcc._
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}

/**
  * Generates Scala code for a provided query
  */

object App {
  
  def main(args: Array[String]){
    run1Calc()
    run1()
    run3Calc()
    run3()
    run4Calc()
    run4()
    run5Calc()
    run5()
    run7Calc()
    run7()
  }
  
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

  def run3Calc(){
    println("---------------------------- Query 3 ----------------------------")
    val q3 = translator.translate(TPCHQueries.query3.asInstanceOf[translator.Expr])
    val qinfo = (q3.asInstanceOf[CExpr], TPCHQueries.q3name, TPCHQueries.q3data)
    Utils.runCalc(qinfo, tpchInputM)

    println("---------------------------- Query 3 Shred ----------------------------")
    val sq3 = runner.shredPipeline(TPCHQueries.query3.asInstanceOf[runner.Expr])
    val sqinfo = (sq3.asInstanceOf[CExpr], "Shred"+TPCHQueries.q3name, TPCHQueries.sq3data)
    Utils.runCalc(sqinfo, tpchShredM)
  }
 
  def run3(){
    println("---------------------------- Query 3 Unnest ----------------------------")
    val q3 = translator.translate(TPCHQueries.query3.asInstanceOf[translator.Expr])
    val qinfo = (q3.asInstanceOf[CExpr], TPCHQueries.q3name, TPCHQueries.q3data)
    Utils.run(qinfo, tpchInputM)

    println("---------------------------- Query 3 Shred Unnest ----------------------------")
    val sq3 = runner.shredPipeline(TPCHQueries.query3.asInstanceOf[runner.Expr])
    val sqinfo = (sq3.asInstanceOf[CExpr], "Shred"+TPCHQueries.q3name, TPCHQueries.sq3data)
    Utils.run(sqinfo, tpchShredM)
  }

  def run4Calc(){
    
    println("---------------------------- Query 4 ----------------------------")  
    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val q4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val q1info = (q1.asInstanceOf[CExpr], TPCHQueries.q1name, TPCHQueries.q1data)
    val q4info = (q4.asInstanceOf[CExpr], TPCHQueries.q4name, "")
    Utils.runCalc(q1info, tpchInputM, q4info)

    println("---------------------------- Query 4 Shred ----------------------------")  
    val sq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val sq4 = runner.shredPipeline(TPCHQueries.query4.asInstanceOf[runner.Expr])
    val sq1info = (sq1.asInstanceOf[CExpr], "Shred"+TPCHQueries.q1name, TPCHQueries.sq1data)
    val sq4info = (sq4.asInstanceOf[CExpr], "Shred"+TPCHQueries.q4name, TPCHQueries.sq4data)
    Utils.runCalc(sq1info, tpchShredM, sq4info)

  }

  def run4(){
    
    println("---------------------------- Query 4 Unnest ----------------------------")  
    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val q4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val q1info = (q1.asInstanceOf[CExpr], TPCHQueries.q1name, TPCHQueries.q1data)
    val q4info = (q4.asInstanceOf[CExpr], TPCHQueries.q4name, "")
    Utils.run(q1info, tpchInputM, q4info)

    println("---------------------------- Query 4 Shred Unnest ----------------------------")  
    val sq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val sq4 = runner.shredPipeline(TPCHQueries.query4.asInstanceOf[runner.Expr])
    val sq1info = (sq1.asInstanceOf[CExpr], "Shred"+TPCHQueries.q1name, TPCHQueries.sq1data)
    val sq4info = (sq4.asInstanceOf[CExpr], "Shred"+TPCHQueries.q4name, TPCHQueries.sq4data)
    Utils.run(sq1info, tpchShredM, sq4info)

  }

  def run5Calc(){
    println("---------------------------- Query 5 ----------------------------")
    val q3 = translator.translate(TPCHQueries.query3.asInstanceOf[translator.Expr])
    val q5 = translator.translate(TPCHQueries.query5.asInstanceOf[translator.Expr])
    val q3info = (q3.asInstanceOf[CExpr], TPCHQueries.q3name, TPCHQueries.q3data)
    val q5info = (q5.asInstanceOf[CExpr], TPCHQueries.q5name, "")
    Utils.runCalc(q3info, tpchInputM, q5info)

    println("---------------------------- Query 5 Shred ----------------------------")
    val sq3 = runner.shredPipeline(TPCHQueries.query3.asInstanceOf[runner.Expr])
    val sq5 = runner.shredPipeline(TPCHQueries.query5.asInstanceOf[runner.Expr])
    val sq3info = (sq3.asInstanceOf[CExpr], "Shred"+TPCHQueries.q3name, TPCHQueries.sq3data)
    val sq5info = (sq5.asInstanceOf[CExpr], "Shred"+TPCHQueries.q5name, TPCHQueries.sq5data)
    Utils.runCalc(sq3info, tpchShredM, sq5info)
  }
 
  def run5(){
    println("---------------------------- Query 5 Unnest ----------------------------")
    val q3 = translator.translate(TPCHQueries.query3.asInstanceOf[translator.Expr])
    val q5 = translator.translate(TPCHQueries.query5.asInstanceOf[translator.Expr])
    val q3info = (q3.asInstanceOf[CExpr], TPCHQueries.q3name, TPCHQueries.q3data)
    val q5info = (q5.asInstanceOf[CExpr], TPCHQueries.q5name, "")
    Utils.run(q3info, tpchInputM, q5info)

    println("---------------------------- Query 5 Shred Unnest ----------------------------")
    val sq3 = runner.shredPipeline(TPCHQueries.query3.asInstanceOf[runner.Expr])
    val sq5 = runner.shredPipeline(TPCHQueries.query5.asInstanceOf[runner.Expr])
    val sq3info = (sq3.asInstanceOf[CExpr], "Shred"+TPCHQueries.q3name, TPCHQueries.sq3data)
    val sq5info = (sq5.asInstanceOf[CExpr], "Shred"+TPCHQueries.q5name, TPCHQueries.sq5data)
    Utils.run(sq3info, tpchShredM, sq5info)
  }

  def run7Calc(){
    println("---------------------------- Query 7 ----------------------------")
    val q3 = translator.translate(TPCHQueries.query3.asInstanceOf[translator.Expr])
    val q7 = translator.translate(TPCHQueries.query7.asInstanceOf[translator.Expr])
    val q3info = (q3.asInstanceOf[CExpr], TPCHQueries.q3name, TPCHQueries.q3data)
    val q7info = (q7.asInstanceOf[CExpr], TPCHQueries.q7name, TPCHQueries.q7data)
    Utils.runCalc(q3info, tpchInputM, q7info)

    println("---------------------------- Query 7 Shred ----------------------------")
    val sq3 = runner.shredPipeline(TPCHQueries.query3.asInstanceOf[runner.Expr])
    val sq7 = runner.shredPipeline(TPCHQueries.query7.asInstanceOf[runner.Expr])
    val sq3info = (sq3.asInstanceOf[CExpr], "Shred"+TPCHQueries.q3name, TPCHQueries.sq3data)
    val sq7info = (sq7.asInstanceOf[CExpr], "Shred"+TPCHQueries.q7name, TPCHQueries.sq7data)
    Utils.runCalc(sq3info, tpchShredM, sq7info)
  }
 
  def run7(){
    println("---------------------------- Query 7 Unnest ----------------------------")
    val q3 = translator.translate(TPCHQueries.query3.asInstanceOf[translator.Expr])
    val q7 = translator.translate(TPCHQueries.query7.asInstanceOf[translator.Expr])
    val q3info = (q3.asInstanceOf[CExpr], TPCHQueries.q3name, TPCHQueries.q3data)
    val q7info = (q7.asInstanceOf[CExpr], TPCHQueries.q7name, TPCHQueries.q7data)
    Utils.run(q3info, tpchInputM, q7info)

    println("---------------------------- Query 7 Shred Unnest ----------------------------")
    val sq3 = runner.shredPipeline(TPCHQueries.query3.asInstanceOf[runner.Expr])
    val sq7 = runner.shredPipeline(TPCHQueries.query7.asInstanceOf[runner.Expr])
    val sq3info = (sq3.asInstanceOf[CExpr], "Shred"+TPCHQueries.q3name, TPCHQueries.sq3data)
    val sq7info = (sq7.asInstanceOf[CExpr], "Shred"+TPCHQueries.q7name, TPCHQueries.sq7data)
    Utils.run(sq3info, tpchShredM, sq7info)
  }

}
