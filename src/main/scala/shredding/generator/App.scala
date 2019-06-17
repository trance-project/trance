package shredding.generator

import java.io._
import shredding.core._
import shredding.wmcc._
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}

/**
  * Generates Scala code for a provided query
  */

object App {
  
  def wflat(q: String): String = s"""
    |def f(){
    | $q
    |}
    """
  def wnest(q1: String, q2: String): String = s"""
    |val $q1 = f.asInstanceOf[List[_]]
    |def f2(){
    | $q2
    |}
    """
  
  def write(n: String, i: String, h: String, q: String): String = { 
    //val f = if (q2 == ""){"f"} else {"f2"}
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
      |    val avg = (time.sum/5)
      |    println(avg)
      | }
      | def f(){
      |  $q
      | }
      |}""".stripMargin
  }
  
  def run(){
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)
    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
 
    val codegen = new ScalaNamedGenerator(inputs)   

    //val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    //val normq1 = normalizer.finalize(q1)
    //val anfedq1 = anfer.finalize(normq1.asInstanceOf[CExpr])
    //val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    //println(Printer.quote(anfExp1.asInstanceOf[CExpr]))
    //val gcode1 = codegen.generate(anfExp1)
    //val header1 = codegen.generateHeader()

    val q4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val normq4 = normalizer.finalize(q4)
    val anfedq4 = anfer.finalize(normq4.asInstanceOf[CExpr])
    val anfExp4 = anfBase.anf(anfedq4.asInstanceOf[anfBase.Rep])
    val gcode4 = codegen.generate(anfExp4)
    val header4 = codegen.generateHeader()

    var out = s"src/test/scala/shredding/examples/tpch/${TPCHQueries.q4name}.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write(TPCHQueries.q4name, TPCHQueries.q1data, header4, gcode4)
    printer.println(finalc)
    printer.close
  }

  def runShred(){
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

    val query = TPCHQueries.query4.asInstanceOf[translator.Expr]
    val qname = TPCHQueries.q4name
    val qdata = TPCHQueries.q1data

    var sout = s"src/test/scala/shredding/examples/tpch/Shred${qname}.Scala"
    val sprinter = new PrintWriter(new FileOutputStream(new File(sout), false))
    val sfinalc = write("Shred"+qname, qdata, sheader, sgcode)
    sprinter.println(sfinalc)
    sprinter.close
  }

  def main(args: Array[String]){
    run()
    //runShred()
  }
}
