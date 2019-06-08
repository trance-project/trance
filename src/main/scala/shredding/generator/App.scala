package shredding.generator

import java.io._
import shredding.core._
import shredding.wmcc._
import shredding.utils.PipelineRunner
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

  val runner = new PipelineRunner{}

  implicit def toExpr(e: TPCHQueries.nrc.Expr): runner.Expr = e.asInstanceOf[runner.Expr]

  def main(args: Array[String]){
    val translator = new NRCTranslator{}
    val codegen = new ScalaNamedGenerator(TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2))
    val gcode = codegen.generate(runner.toAnf(TPCHQueries.query1))
    val header = codegen.generateHeader()
    
    var out = s"src/test/scala/shredding/queries/tpch/${TPCHQueries.q1name}.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    val finalc = write(TPCHQueries.q1name, TPCHQueries.q1data, header, gcode)
    printer.println(finalc)
    printer.close
  }


}
