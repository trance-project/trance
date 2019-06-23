package shredding.generator

import java.io._
import shredding.core._
import shredding.wmcc._
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
      | $i
      | $h
      | def main(args: Array[String]){ 
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {
      |      var start = System.currentTimeMillis()
      |      f
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    println(time)
      |    val avg = (time.sum/5)
      |    println(avg)
      | }
      | def f(){
      |  $q
      | }
      |}""".stripMargin
  }
     
  def write2(n: String, i1: String, h: String, q1: String, i2: String => String, q2: String): String = {
    s"""
      |package experiments
      |/** Generated code **/
      |object $n {
      | $i1
      | $h
      | def main(args: Array[String]){ 
      |    var start0 = System.currentTimeMillis()
      |    $q1
      |    ${i2(q1.split("\n").last)}
      |    var end0 = System.currentTimeMillis() - start0
      |    println("setup time: "+end0)
      |    var time = List[Long]()
      |    for (i <- 1 to 5) {  
      |     var start = System.currentTimeMillis()
      |      f
      |      var end = System.currentTimeMillis() - start
      |      time = time :+ end
      |    }
      |    println(time)
      |    val avg = (time.sum/5)
      |    println(avg)
      | }
      | def f(){
      |  $q2
      | }
      |}""".stripMargin
  }
 
  def run1(){
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val optimizer = new Finalizer(new BasePlanOptimizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)
    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
     
    val codegen = new ScalaNamedGenerator(inputs)   

    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    println(Printer.quote(q1))
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1))
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None)
    println(Printer.quote(plan1))
    val anfedq1 = anfer.finalize(normq1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExp1.asInstanceOf[CExpr]))
    val gcode1 = codegen.generate(anfExp1)
    val header1 = codegen.generateHeader()

    var out = s"src/test/scala/shredding/examples/tpch/${TPCHQueries.q1name}.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write(TPCHQueries.q1name, TPCHQueries.q1data, header1, gcode1)
    printer.println(finalc)
    printer.close
  }

  def run4(){
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}
    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
 
    val codegen = new ScalaNamedGenerator(inputs)   

    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1)
    val anfedq1 = new Finalizer(anfBase).finalize(normq1.asInstanceOf[CExpr])
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    val gcode1 = codegen.generate(anfExp1)

    val q4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val normq4 = normalizer.finalize(q4)
    anfBase.reset
    val anfedq4 = new Finalizer(anfBase).finalize(normq4.asInstanceOf[CExpr])
    val anfExp4 = anfBase.anf(anfedq4.asInstanceOf[anfBase.Rep])
    val gcode4 = codegen.generate(anfExp4)
    val header4 = codegen.generateHeader()

    var out = s"src/test/scala/shredding/examples/tpch/${TPCHQueries.q4name}.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write2(TPCHQueries.q4name, TPCHQueries.q1data, header4, gcode1, (v: String) => s"val Q1 = $v", gcode4)
    printer.println(finalc)
    printer.close
  }

  def run1Shred(){
    // shredded pipeline
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)
    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
    val scodegen = new ScalaNamedGenerator(inputs)
    val nq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val nnormq1 = normalizer.finalize(nq1)
    val anfednq1 = anfer.finalize(nnormq1.asInstanceOf[CExpr])
    val anfExpn1 = anfBase.anf(anfednq1.asInstanceOf[anfBase.Rep])
    val sgcode = scodegen.generate(anfExpn1)
    val sheader = scodegen.generateHeader()

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
    val header4 = codegen.generateHeader()

    var sout = s"src/test/scala/shredding/examples/tpch/Shred${TPCHQueries.q4name}.Scala"
    val sprinter = new PrintWriter(new FileOutputStream(new File(sout), false))
    // TODO change Foo and Goo to appropriate types
    val sfinalc = write2("Shred"+TPCHQueries.q4name, TPCHQueries.sq1data, header4, gcode1, TPCHQueries.sq4data("Record442", "Record430"), gcode4)
    sprinter.println(sfinalc)
    sprinter.close
  }

  def main(args: Array[String]){
    run1()
    run1Shred()
    run4()
    run4Shred()
  }
}
