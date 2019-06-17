package shredding.wmcc

import shredding.examples.simple.FlatTests
import shredding.examples.simple.NestedTests

object App {
    
  val translator = new NRCTranslator{}
  val normalizer = new Finalizer(new BaseNormalizer{})

  def main(args: Array[String]){
    val q1 = translator.translate(FlatTests.q1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1)
    println(Printer.quote(normq1.asInstanceOf[CExpr]))
    // val evaluator = new Finalizer(new BaseScalaInter {})
    // println(evaluator.finalize(normq1.asInstanceOf[CExpr]))
  }


}
