package shredding.generator

import java.io._
import shredding.core._
import shredding.wmcc._
import shredding.utils.ShredPipelineRunner
import shredding.examples.simple.{FlatTests, NestedTests}
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}

/**
  * This App is just a scratch space to quickly test components in this 
  * directory. 
  */

object App {

  def write(n: String, i: String, h: String, q: String): String = s"""
      |package experiments
      |/** Generated code **/
      |object $n {
      | $i
      | $h
      | def main(args: Array[String]){ 
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {
      |      var start = System.currentTimeMillis()
      |      println(f)
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    val avg = (time.sum/5)
      |    println(avg)
      | }
      | 
      | def f(){
      |   $q
      | }
      |}""".stripMargin

  def runShred(){
    
    val translator = new NRCTranslator{}
    val shredder = new ShredPipelineRunner{}
    val str = Printer
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    
    /**val exp1 = shredder.shredPipeline(FlatTests.query1.asInstanceOf[shredder.Expr])
    val exp2 = shredder.shredPipeline(NestedTests.query2.asInstanceOf[shredder.Expr])
    val exp3 = shredder.shredPipeline(NestedTests.query3.asInstanceOf[shredder.Expr])
    val exp4 = shredder.shredPipeline(NestedTests.query4.asInstanceOf[shredder.Expr]) 
    val tpch1 = shredder.shredPipeline(TPCHQueries.query1.asInstanceOf[shredder.Expr])

    val beval = new BaseScalaInterp{}
    val eval = new Finalizer(beval)
    val anfBase = new BaseANF {}**/

    /**println("\nTranslated:")
    println(str.quote(exp1))
    println("\nNormalized:")
    val nexp1 = norm.finalize(exp1).asInstanceOf[CExpr]
    println(str.quote(nexp1))
    beval.ctx("RF") = FlatTest.rF
    beval.ctx("RD") = FlatTest.rDc
    println("\n\nEvaluated:\n")
    eval.finalize(nexp1).asInstanceOf[List[_]].foreach(println(_))

    println("")
    val anfed = new Finalizer(anfBase).finalize(nexp1)
    val anfExp = anfBase.anf(anfed.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp))
    println(ScalaNamedGenerator.generate(anfExp))
    println(ScalaNamedGenerator.generateHeader())

    println("")
    beval.ctx.clear
    beval.ctx("RF") = NestedTest.rF
    beval.ctx("RD") = NestedTest.rDc**/

    /**println("\nTranslated:")
    println(str.quote(tpch1))
    
    println("\nNormalized:")
    val nquery1 = norm.finalize(tpch1).asInstanceOf[CExpr]
    //println("\nEvaluated:\n")
    //eval.finalize(nexp2)
 
    println("")
    val anfed1 = new Finalizer(anfBase).finalize(nquery1)
    val anfExp1 = anfBase.anf(anfed1.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp1))
    val inputtypes = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
    val sgen = new ScalaNamedGenerator(inputtypes)
    val ccode = sgen.generate(anfExp1)
    val header = sgen.generateHeader()
 
    var out = "src/test/scala/shredding/queries/tpch/ShredQuery1.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write("ShredQuery1", TPCHQueries.q1shreddata, header, ccode)
    printer.println(finalc)
    printer.close **/

  }

  def runBase(){
    val translator = new NRCTranslator{}

    /**val exp0 = translator.translate(NestedTests.query2.asInstanceOf[translator.Expr])
    val exp1 = translator.translate(NestedTests.query2a.asInstanceOf[translator.Expr])
    val exp2 = translator.translate(NestedTests.query1.asInstanceOf[translator.Expr])
    val exp3 = translator.translate(FlatTests.query2.asInstanceOf[translator.Expr])
    val exp4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val exp5 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val exp6 = translator.translate(FlatTests.query1.asInstanceOf[translator.Expr])

    val str = Printer
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    val beval = new BaseScalaInterp{}
    beval.ctx("R") = FlatTest.relationRValues3
    val eval = new Finalizer(beval)
    val anfBase = new BaseANF {}

    // exp0
    val normalized0 = norm.finalize(exp0).asInstanceOf[CExpr]
    //println(str.quote(normalized0))

    val normalized1 = norm.finalize(exp2).asInstanceOf[CExpr]

    val normalized4 = norm.finalize(exp4).asInstanceOf[CExpr]
    //println(str.quote(normalized4))
    
    println("")
    val anfed = new Finalizer(anfBase).finalize(normalized1)
    val anfExp = anfBase.anf(anfed.asInstanceOf[anfBase.Rep])
    //val inputtypes = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
    //val inputtypes = Map(translator.translate(FlatTests.dtype) -> "InputR")
    val inputtypes = NestedTests.nestedInputs.map(f => translator.translate(f._1) -> f._2)
    val sgenn = new ScalaNamedGenerator(inputtypes)
    val ccode = sgenn.generate(anfExp)
    val header = sgenn.generateHeader()

    var out = "src/test/scala/shredding/queries/simple/NestedQuery1.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    //val finalc = write("Query1", TPCHQueries.query1data, header, ccode)
    val finalc = write("Query1", NestedTests.header, header, ccode)
    printer.println(finalc)
    printer.close**/
    
  }

  def main(args: Array[String]){
    runShred()
    runBase()
  }


}
