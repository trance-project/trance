package shredding.wmcc

import shredding.utils.PipelineRunner
import shredding.examples.simple.FlatTests
import shredding.examples.simple.NestedTests

object App {
    
  val runner = new PipelineRunner{}

  implicit def toExpr(e: FlatTests.nrc.Expr): runner.Expr = e.asInstanceOf[runner.Expr]
  implicit def toExprN(e: NestedTests.nrc.Expr): runner.Expr = e.asInstanceOf[runner.Expr]

  def main(args: Array[String]){
    runner.toNormalizedCalc(FlatTests.q1)
    runner.toNormalizedCalc(NestedTests.q1)
    runner.toNormalizedCalc(NestedTests.q2)
    runner.toNormalizedCalc(NestedTests.q3)
    runner.toNormalizedCalc(NestedTests.q4)
    runner.toNormalizedCalc(NestedTests.q5)
    runner.toNormalizedCalc(NestedTests.q6)
    runner.toNormalizedCalc(NestedTests.q7)
    runner.toNormalizedCalc(NestedTests.q8)
    runner.toNormalizedCalc(NestedTests.q9)
  }


}
