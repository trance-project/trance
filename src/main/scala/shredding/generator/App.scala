package shredding.generator

import java.io._
import shredding.core._
import shredding.wmcc._
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

  def main(args: Array[String]){
    
    val runner = new PipelineRunner{}
    val translator = new NRCTranslator{}
    val normalizer = new Finalizer(new BaseNormalizer{})
    val anfBase = new BaseANF {}
    val anfer = new Finalizer(anfBase)

    val inputs = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
    // standard pipeline
    val codegen = new ScalaNamedGenerator(inputs)
    val q1 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1)
    val anfedq1 = anfer.finalize(normq1.asInstanceOf[CExpr])
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    val gcode = codegen.generate(anfExp1)
    val header = codegen.generateHeader()
    var out = s"src/test/scala/shredding/examples/tpch/${TPCHQueries.q1name}.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write(TPCHQueries.q1name, TPCHQueries.q1data, header, gcode)
    printer.println(finalc)
    printer.close

    // shredded pipeline
    val scodegen = new ScalaNamedGenerator(inputs)
    val nq1 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val nnormq1 = normalizer.finalize(nq1)
    val anfednq1 = anfer.finalize(nnormq1.asInstanceOf[CExpr])
    val anfExpn1 = anfBase.anf(anfednq1.asInstanceOf[anfBase.Rep])
    val sgcode = scodegen.generate(anfExpn1)
    val sheader = scodegen.generateHeader()

    var sout = s"src/test/scala/shredding/examples/tpch/Shred${TPCHQueries.q1name}.Scala"
    val sprinter = new PrintWriter(new FileOutputStream(new File(sout), false))
    val sfinalc = write("Shred"+TPCHQueries.q1name, TPCHQueries.sq1data, sheader, sgcode)
    sprinter.println(sfinalc)
    sprinter.close

  }


}
