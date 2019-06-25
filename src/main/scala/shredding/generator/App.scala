package shredding.generator

import java.io._
import shredding.core._
import shredding.wmcc._
import shredding.examples.simple.{FlatTests, FlatRelations}
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}

/**
  * Generates Scala code for a provided query
  */

object App {
    
  def write(n: String, i: String, h: String, q: String): String = { 
    s"""
      |package experiments
      |/** Generated code **/
      |object $n {
      | $h
      | $i
      | def main(args: Array[String]){ 
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {
      |      var start = System.currentTimeMillis()
      |      f
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    val avg = (time.sum/5)
      |    println(avg)
      | }
      | def f(){
      |  $q
      | }
      |}""".stripMargin
  }
     
  def write2(n: String, i1: String, h: String, q1: String, i2: String, q2: String): String = {
    s"""
      |package experiments
      |/** Generated code **/
      |object $n {
      | $h
      | $i1
      | def main(args: Array[String]){ 
      |    var start0 = System.currentTimeMillis()
      |    val $i2 = { $q1 }
      |    var end0 = System.currentTimeMillis() - start0
      |    println("setup time: "+end0)
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {  
      |     var start = System.currentTimeMillis()
      |      f($i2)
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    val avg = (time.sum/5)
      |    println(avg)
      | }
      | def f($i2: List[${i2}Out]){
      |  $q2
      | }
      |}""".stripMargin
  }
 
  def run(){
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val optimizer = new Finalizer(new BasePlanOptimizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)
    val inputs = Map(FlatRelations.type1b.asInstanceOf[Type] -> "InputR")
    val codegen = new ScalaNamedGenerator(inputs)
    val eval = new BaseScalaInterp{}
    val evaluator = new Finalizer(eval)

    val q1 = translator.translate(FlatTests.q4.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1.asInstanceOf[CExpr]))
    //eval.ctx("R") = FlatRelations.format1a
    //println(evaluator.finalize(normq1.asInstanceOf[CExpr]))
    val codegen1 = new ScalaNamedGenerator(inputs)
    val gcode11 = codegen1.generate(normq1)
    val header11 = codegen1.generateHeader()
    var out1 = s"src/test/scala/shredding/examples/simple/Query1Calc.Scala"
    val printer1 = new PrintWriter(new FileOutputStream(new File(out1), false))
    val finalc1 = write("Query1Calc", FlatRelations.format1c, header11, gcode11)
    printer1.println(finalc1)
    printer1.close

    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    //println(evaluator.finalize(plan1))
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExp1.asInstanceOf[CExpr]))
    val gcode1 = codegen.generate(anfExp1)
    val header1 = codegen.generateHeader()

    var out = s"src/test/scala/shredding/examples/simple/Query1.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write("Query1", FlatRelations.format1c, header1, gcode1)
    printer.println(finalc)
    printer.close
 
  }

  def run1(){
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val optimizer = new Finalizer(new BasePlanOptimizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)
    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
    val ng = TPCHSchema.tpchInputs.toList.map(f => f._2)
     
    val codegen = new ScalaNamedGenerator(inputs)   

    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    println(Printer.quote(q1))
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1))
    val anfedq1c = anfer.finalize(normq1)
    val anfExp1c = anfBase.anf(anfedq1c.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExp1c.asInstanceOf[CExpr]))
    val codegen1 = new ScalaNamedGenerator(inputs)
    val gcode11 = codegen1.generate(anfExp1c)
    val header11 = codegen1.generateHeader(ng)
    var out1 = s"src/test/scala/shredding/examples/tpch/${TPCHQueries.q1name}Calc.Scala"
    val printer1 = new PrintWriter(new FileOutputStream(new File(out1), false))
    val finalc1 = write(s"${TPCHQueries.q1name}Calc", TPCHQueries.q1data, header11, gcode11)
    printer1.println(finalc1)
    printer1.close

    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None)
    println(Printer.quote(plan1))
    anfBase.reset
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExp1.asInstanceOf[CExpr]))
    val gcode1 = codegen.generate(anfExp1)
    val header1 = codegen.generateHeader(ng)

    var out = s"src/test/scala/shredding/examples/tpch/${TPCHQueries.q1name}.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write(TPCHQueries.q1name, TPCHQueries.q1data, header1, gcode1)
    printer.println(finalc)
    printer.close
  }

  def run4Calc(){
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    val anfedq1c = anfer.finalize(normq1)
    val anfExp1c = anfBase.anf(anfedq1c.asInstanceOf[anfBase.Rep])

    println(Printer.quote(anfExp1c.asInstanceOf[CExpr]))

    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2) ++ 
                  Map(normq1.tp.asInstanceOf[BagCType].tp -> "Q1Out")
    val ng = TPCHSchema.tpchInputs.toList.map(f => f._2)
    val codegen = new ScalaNamedGenerator(inputs)   
    val gcode1 = codegen.generate(anfExp1c)

    val q4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val normq4 = normalizer.finalize(q4).asInstanceOf[CExpr]
    val gcode4 = codegen.generate(normq4)
    val header4 = codegen.generateHeader(ng)

    var out = s"src/test/scala/shredding/examples/tpch/${TPCHQueries.q4name}Calc.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write2(s"${TPCHQueries.q4name}Calc", TPCHQueries.q1data, header4, gcode1, "Q1", gcode4)
    printer.println(finalc)
    printer.close
  }

  def run4(){
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}

    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1)

    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2) ++ 
                  Map(normq1.asInstanceOf[CExpr].tp.asInstanceOf[BagCType].tp -> "Q1Out")
    val ng = TPCHSchema.tpchInputs.toList.map(f => f._2)
    val codegen = new ScalaNamedGenerator(inputs)   

    val anfedq1 = new Finalizer(anfBase).finalize(normq1.asInstanceOf[CExpr])
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    val gcode1 = codegen.generate(anfExp1)

    val q4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val normq4 = normalizer.finalize(q4)
    anfBase.reset
    val anfedq4 = new Finalizer(anfBase).finalize(normq4.asInstanceOf[CExpr])
    val anfExp4 = anfBase.anf(anfedq4.asInstanceOf[anfBase.Rep])
    val gcode4 = codegen.generate(anfExp4)
    val header4 = codegen.generateHeader(ng)

    var out = s"src/test/scala/shredding/examples/tpch/${TPCHQueries.q4name}.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write2(TPCHQueries.q4name, TPCHQueries.q1data, header4, gcode1, "Q1", gcode4)
    printer.println(finalc)
    printer.close
  }

   def run1ShredCalc(){
    // shredded pipeline
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val nq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val nnormq1 = normalizer.finalize(nq1).asInstanceOf[CExpr]
    val anfedq1 = anfer.finalize(nnormq1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])

    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2) ++ 
                  nnormq1.asInstanceOf[LinearCSet].getTypeMap
    val ng = TPCHSchema.tpchInputs.toList.map(f => f._2)

    val scodegen = new ScalaNamedGenerator(inputs)   
    val sgcode = scodegen.generate(anfExp1)
    val sheader = scodegen.generateHeader(ng)

    val query = TPCHQueries.query1.asInstanceOf[translator.Expr]
    val qname = TPCHQueries.q1name
    val qdata = TPCHQueries.sq1data

    var sout = s"src/test/scala/shredding/examples/tpch/Shred${qname}Calc.Scala"
    val sprinter = new PrintWriter(new FileOutputStream(new File(sout), false))
    val sfinalc = write(s"Shred${qname}Calc", qdata, sheader, sgcode)
    sprinter.println(sfinalc)
    sprinter.close
  }
 
  
  def run1Shred(){
    // shredded pipeline
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val nq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val nnormq1 = normalizer.finalize(nq1)
    
    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2) ++ 
                  nnormq1.asInstanceOf[LinearCSet].getTypeMap
    val ng = TPCHSchema.tpchInputs.toList.map(f => f._2)
    val scodegen = new ScalaNamedGenerator(inputs)   
    
    val anfednq1 = anfer.finalize(nnormq1.asInstanceOf[CExpr])
    val anfExpn1 = anfBase.anf(anfednq1.asInstanceOf[anfBase.Rep])
    val sgcode = scodegen.generate(anfExpn1)
    val sheader = scodegen.generateHeader(ng)

    val query = TPCHQueries.query1.asInstanceOf[translator.Expr]
    val qname = TPCHQueries.q1name
    val qdata = TPCHQueries.sq1data

    var sout = s"src/test/scala/shredding/examples/tpch/Shred${qname}.Scala"
    val sprinter = new PrintWriter(new FileOutputStream(new File(sout), false))
    val sfinalc = write("Shred"+qname, qdata, sheader, sgcode)
    sprinter.println(sfinalc)
    sprinter.close
  }

  def run4Shred(){
    // shredded pipeline
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)
    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2) ++
                  Map(RecordCType("Q1__F" -> IntType) -> "Q1Flat2")
    val ng = TPCHSchema.tpchInputs.toList.map(f => f._2)
    val codegen = new ScalaNamedGenerator(inputs)

    val q1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val normq1 = normalizer.finalize(q1)
    val anfedq1 = new Finalizer(anfBase).finalize(normq1.asInstanceOf[CExpr])
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    val gcode1 = codegen.generate(anfExp1)

    val q4 = runner.shredPipeline(TPCHQueries.query4.asInstanceOf[runner.Expr])
    val normq4 = normalizer.finalize(q4)
    anfBase.reset
    val anfedq4 = new Finalizer(anfBase).finalize(normq4.asInstanceOf[CExpr])
    val anfExp4 = anfBase.anf(anfedq4.asInstanceOf[anfBase.Rep])
    val gcode4 = codegen.generate(anfExp4)
    val header4 = codegen.generateHeader(ng)

    var sout = s"src/test/scala/shredding/examples/tpch/Shred${TPCHQueries.q4name}.Scala"
    val sprinter = new PrintWriter(new FileOutputStream(new File(sout), false))
    // TODO change Foo and Goo to appropriate types
    val sfinalc = write2("Shred"+TPCHQueries.q4name, TPCHQueries.sq1data, header4, gcode1, "foo", gcode4)
    //TPCHQueries.sq4data("Record442", "Record430"), gcode4)
    sprinter.println(sfinalc)
    sprinter.close
  }

  def main(args: Array[String]){
    run()
    run1()
    run1Shred()
    run1ShredCalc()
    run4Calc()
    run4()
    run4Shred()
  }
}
