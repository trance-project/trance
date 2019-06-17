package shredding.wmcc

import shredding.examples.simple.{FlatTests, FlatRelations}
import shredding.examples.simple.NestedTests

object App {
    
  val translator = new NRCTranslator{}
  val normalizer = new Finalizer(new BaseNormalizer{})

  def main(args: Array[String]){
    val q1 = translator.translate(FlatTests.q1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1)
    println(Printer.quote(normq1.asInstanceOf[CExpr]))
    val eval = new BaseScalaInterp{}
    val evaluator = new Finalizer(eval)
    eval.ctx("R") = FlatRelations.format1a
    println(evaluator.finalize(normq1.asInstanceOf[CExpr]))
  }


}
