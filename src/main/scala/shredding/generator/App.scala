package shredding.generator

import shredding.wmcc._
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}

/**
  * Generates Scala code for a provided query
  */

object App {
  
  def main(args: Array[String]){
    /**run1Calc()
    run1()
    run3Calc()
    run3()
    run4Calc()
    run4()
    run5Calc()**/
    run5()
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

  /**def run4Input(){
    val anfBase = new BaseANF {}

    val input4 = translator.translate(TPCHQueries.input4.asInstanceOf[translator.Expr])
    val normi4 = normalizer.finalize(input4).asInstanceOf[CExpr]

    val plani4 = Unnester.unnest(normi4)((Nil, Nil, None)).asInstanceOf[CExpr]
    val anfedqi4 = new Finalizer(anfBase).finalize(plani4)
    val anfExpi4 = anfBase.anf(anfedqi4.asInstanceOf[anfBase.Rep])

    val ng = TPCHSchema.tpchInputs.toList.map(f => f._2)
    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2) ++ 
                  normi4.asInstanceOf[LinearCSet].getTypeMap
                   
    val codegen = new ScalaNamedGenerator(inputs)   
    val gcode = codegen.generate(anfExpi4)

    println(TPCHQueries.q1data)
    println(codegen.generateHeader(ng))
    println(gcode)
    // todo figure out how to integrate this
    //val Q1__F = Q1._1.head.lbl
    //val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    val q4 = runner.shredPipeline(TPCHQueries.query4.asInstanceOf[runner.Expr])
    println(Printer.quote(q4))
    val normq4 = normalizer.finalize(q4).asInstanceOf[CExpr]
    println(Printer.quote(normq4))
    val plan4 = Unnester.unnest(normq4)((Nil, Nil, None)).asInstanceOf[CExpr]
    println(Printer.quote(plan4))
    anfBase.reset
    val anfedq4 = new Finalizer(anfBase).finalize(plan4)
    val anfExp4 = anfBase.anf(anfedq4.asInstanceOf[anfBase.Rep])
    val gcode4 = codegen.generate(anfExp4)
    val header4 = codegen.generateHeader(ng)

    var sout = s"src/test/scala/shredding/examples/tpch/Shred${TPCHQueries.q4name}.Scala"
    val sprinter = new PrintWriter(new FileOutputStream(new File(sout), false))
    val cc = TPCHQueries.sq4cclass("RecM_flat2", "RecM_flat3")
    val dd = TPCHQueries.sq4data("Q1")
    val sfinalc = write2(s"Shred${TPCHQueries.q4name}", s"${TPCHQueries.sq1data}", s"$header4\n$cc", gcode1, "Q1", gcode4, dd)
    sprinter.println(sfinalc)
    sprinter.close
    
  }**/

}
