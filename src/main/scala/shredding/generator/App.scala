package shredding.generator

import shredding.core._
import shredding.wmcc._
import shredding.examples.genomic.{GenomicTests, GenomicRelations}
import shredding.examples.simple.{FlatTests, NestedTests, FlatRelations, NestedRelations}
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}

/**
  * Generates Scala code for a provided query
  */

object App {
  
  def runSparkCalc(){
    println(" ------------------------------- Spark Test Query -------------------------- ")
    val q1 = translator.translate(NestedTests.q10.asInstanceOf[translator.Expr])
    val qinfo = (q1.asInstanceOf[CExpr], NestedTests.q10name, NestedRelations.format4Spark)
    Utils.runCalcSpark(qinfo, NestedRelations.q10inputs)
  }

  def runTPCH(){
    println("---------------------------- TPCH Query 1 Unnest ----------------------------")  
    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val qinfo = (q1.asInstanceOf[CExpr], TPCHQueries.q1name, TPCHQueries.q1spark)
    Utils.runSpark(qinfo, tpchInputM)

    println("---------------------------- TPCH Query 1 Shred Unnest ----------------------------")
    val sq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val sqinfo = (sq1.asInstanceOf[CExpr], "Shred"+TPCHQueries.q1name, TPCHQueries.sq1spark)
    Utils.runSpark(sqinfo, tpchShredM)

    println("---------------------------- TPCH Query 4 Unnest ----------------------------")  
    val q4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val q4info = (q4.asInstanceOf[CExpr], TPCHQueries.q4name, "")
    Utils.runSpark(qinfo, tpchInputM, q4info)

    println("---------------------------- TPCH Query 4 Shred Unnest ----------------------------")
    val sq4 = runner.shredPipeline(TPCHQueries.query4.asInstanceOf[runner.Expr])
    val sq4info = (sq4.asInstanceOf[CExpr], "Shred"+TPCHQueries.q4name, TPCHQueries.sq4spark)
    Utils.runSpark(sqinfo, tpchShredM, sq4info)

    println("---------------------------- TPCH Query 3 Unnest ----------------------------")  
    val q3 = translator.translate(TPCHQueries.query3.asInstanceOf[translator.Expr])
    val q3info = (q3.asInstanceOf[CExpr], TPCHQueries.q3name, TPCHQueries.q3spark)
    Utils.runSpark(q3info, tpchInputM)

    println("---------------------------- TPCH Query 3 Shred Unnest ----------------------------")
    val sq3 = runner.shredPipeline(TPCHQueries.query3.asInstanceOf[runner.Expr])
    val sq3info = (sq3.asInstanceOf[CExpr], "Shred"+TPCHQueries.q3name, TPCHQueries.sq3spark)
    Utils.runSpark(sq3info, tpchShredM)

    println("---------------------------- TPCH Query 5 Unnest ----------------------------")  
    val q5 = translator.translate(TPCHQueries.query5.asInstanceOf[translator.Expr])
    val q5info = (q5.asInstanceOf[CExpr], TPCHQueries.q5name, "")
    Utils.runSpark(q3info, tpchInputM, q5info)

    println("---------------------------- TPCH Query 5 Shred Unnest ----------------------------")
    val sq5 = runner.shredPipeline(TPCHQueries.query5.asInstanceOf[runner.Expr])
    val sq5info = (sq5.asInstanceOf[CExpr], "Shred"+TPCHQueries.q5name, TPCHQueries.sq5spark)
    Utils.runSpark(sq3info, tpchShredM, sq5info)

  }

  def runSpark(){
    println(" ------------------------------- Spark Test Query -------------------------- ")
    val q1 = translator.translate(FlatTests.q1.asInstanceOf[translator.Expr])
    val qinfo = (q1.asInstanceOf[CExpr], "FlatTest1", FlatRelations.format1Spark)
    Utils.runSpark(qinfo, Map[Type, String]())

    val sq1 = runner.shredPipeline(FlatTests.q1.asInstanceOf[runner.Expr])
    val sqinfo = (sq1.asInstanceOf[CExpr], "FlatShredTest1", FlatRelations.format2Spark)
    Utils.runSpark(sqinfo, Map[Type, String]())
    
    val q2 = translator.translate(FlatTests.q3.asInstanceOf[translator.Expr])
    val q2info = (q2.asInstanceOf[CExpr], "FlatTest2", FlatRelations.format1Spark)
    Utils.runSpark(q2info, Map[Type, String]())

    val sq2 = runner.shredPipeline(FlatTests.q3.asInstanceOf[runner.Expr])
    val sq2info = (sq2.asInstanceOf[CExpr], "FlatShredTest2", FlatRelations.format2Spark)
    Utils.runSpark(sq2info, Map[Type, String]())
  
    val q3 = translator.translate(FlatTests.q4.asInstanceOf[translator.Expr])
    val q3info = (q3.asInstanceOf[CExpr], "FlatTest3", FlatRelations.format1Spark)
    Utils.runSpark(q3info, Map[Type, String]())

    val sq3 = runner.shredPipeline(FlatTests.q4.asInstanceOf[runner.Expr])
    val sq3info = (sq3.asInstanceOf[CExpr], "FlatShredTest3", FlatRelations.format2Spark)
    Utils.runSpark(sq3info, Map[Type, String]())

    val q4 = translator.translate(NestedTests.q2.asInstanceOf[translator.Expr])
    val q4info = (q4.asInstanceOf[CExpr], "NestedTest1", NestedRelations.format1Spark)
    Utils.runSpark(q4info, Map[Type, String]())

    val sq4 = runner.shredPipeline(NestedTests.q2.asInstanceOf[runner.Expr])
    val sq4info = (sq4.asInstanceOf[CExpr], "NestedShredTest1", NestedRelations.format2Spark)
    Utils.runSpark(sq4info, Map[Type, String]())

    val q5 = translator.translate(NestedTests.q8.asInstanceOf[translator.Expr])
    val q5info = (q5.asInstanceOf[CExpr], "NestedTest2", NestedRelations.format2aSpark)
    Utils.runSpark(q5info, Map[Type, String](RecordCType("c" -> IntType) -> "InputRB2"))

    val sq5 = runner.shredPipeline(NestedTests.q8.asInstanceOf[runner.Expr])
    val sq5info = (sq5.asInstanceOf[CExpr], "NestedShredTest2", NestedRelations.format2bSpark)
    Utils.runSpark(sq5info, Map[Type, String](RecordCType("c" -> IntType) -> "InputRB2"))

    val q6 = translator.translate(NestedTests.q1.asInstanceOf[translator.Expr])
    val q6info = (q6.asInstanceOf[CExpr], "NestedTest3", NestedRelations.format1Spark)
    Utils.runSpark(q6info, Map[Type, String](RecordCType("n" -> IntType) -> "InputR3",
                                             RecordCType("m" -> IntType, "n" -> IntType, "k" -> BagCType(RecordCType("n" -> IntType))) -> "InputR2",
                                             RecordCType("h" -> IntType, "j" -> 
                                              BagCType(RecordCType("m" -> IntType, "n" -> IntType, "k" -> BagCType(RecordCType("n" -> IntType))))) -> "InputR1"))

    val sq6 = runner.shredPipeline(NestedTests.q1.asInstanceOf[runner.Expr])
    val sq6info = (sq6.asInstanceOf[CExpr], "NestedShredTest3", NestedRelations.format2Spark)
    Utils.runSpark(sq6info, Map[Type, String]())

  }

  def runBio(){
    println(" ------------------------------- Spark Bio Query Plan -------------------------- ")
    /**val q1 = translator.translate(GenomicTests.q1.asInstanceOf[translator.Expr])
    val q1info = (q1.asInstanceOf[CExpr], "AlleleCounts", GenomicRelations.format1Spark)
    Utils.runSpark(q1info, GenomicRelations.q1inputs.map(r => translator.translate(r._1) -> r._2))**/

    val sq1 = runner.shredPipeline(GenomicTests.q1.asInstanceOf[runner.Expr])
    val sq1info = (sq1.asInstanceOf[CExpr], "ShredAlleleCounts", GenomicRelations.format2Spark)
    Utils.runSpark(sq1info, GenomicRelations.q1inputs.map(r => translator.translate(r._1) -> r._2))

    /**val q2 = translator.translate(GenomicTests.q2.asInstanceOf[translator.Expr])
    val q2info = (q2.asInstanceOf[CExpr], "TotalGenotypes", GenomicRelations.format1Spark)
    Utils.runSpark(q2info, GenomicRelations.q1inputs.map(r => translator.translate(r._1) -> r._2))

    val q3 = translator.translate(GenomicTests.q3.asInstanceOf[translator.Expr])
    val q3info = (q3.asInstanceOf[CExpr], "CaseGenotypes", GenomicRelations.format1Spark)
    Utils.runSpark(q3info, GenomicRelations.q1inputs.map(r => translator.translate(r._1) -> r._2))

    val q4 = translator.translate(GenomicTests.q4.asInstanceOf[translator.Expr])
    val q4info = (q4.asInstanceOf[CExpr], "AltMutations", GenomicRelations.format1Spark)
    Utils.runSpark(q4info, GenomicRelations.q1inputs.map(r => translator.translate(r._1) -> r._2))

    val q5 = translator.translate(GenomicTests.q5.asInstanceOf[translator.Expr])
    val q5info = (q5.asInstanceOf[CExpr], "AltCount", GenomicRelations.format1Spark)
    Utils.runSpark(q5info, GenomicRelations.q1inputs.map(r => translator.translate(r._1) -> r._2))
    **/
  }

  def main(args: Array[String]){
    runTPCH()
     //runSparkCalc()
    //runSpark()
    //runBio()
    /**run1Calc()
    run1()
    run3Calc()
    run3()**/
    //run4Calc()
    //run4()
    /**run5Calc()
    run5()
    run7Calc()
    run7()**/
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
