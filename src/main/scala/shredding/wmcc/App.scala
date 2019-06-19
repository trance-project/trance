package shredding.wmcc

import shredding.core._
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}
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

    /**val q2 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val normq2 = normalizer.finalize(q2)
    println(Printer.quote(normq2.asInstanceOf[CExpr]))
    eval.ctx("C") = TPCHLoader.loadCustomer.toList 
    eval.ctx("O") = TPCHLoader.loadOrders.toList 
    eval.ctx("L") = TPCHLoader.loadLineitem.toList 
    eval.ctx("P") = TPCHLoader.loadPart.toList 
    println(evaluator.finalize(normq2.asInstanceOf[CExpr]))**/

    val q3 = {    
      import translator._
      val tuple_x = TupleType("a" -> IntType, "c" -> IntType)
      val tuple_y = TupleType("a" -> IntType, "c" -> IntType)
      val relationR = BagVarRef(VarDef("R", BagType(tuple_x)))

      val xdef = VarDef("x", tuple_x)
      val ydef = VarDef("y", tuple_y)
      val xref = TupleVarRef(xdef)

      val yref = TupleVarRef(ydef)
      translate(Tuple("a" -> xref("c"), "b" -> ForeachUnion(ydef, relationR,
        IfThenElse(
          Cmp(OpEq, yref("c"), xref("c")),
          Singleton(Tuple("b'" -> yref("a")))))))
    }
    val normq3 = normalizer.finalize(q3)
    println(Printer.quote(normq3.asInstanceOf[CExpr]))
    eval.ctx("R") = List(
      RecordValue("a" -> 1, "c" -> 11),
      RecordValue("a" -> 2, "c" -> 22),
      RecordValue("a" -> 3, "c" -> 33),
      RecordValue("a" -> 4, "c" -> 44)
    )
    eval.ctx("x") =  RecordValue("a" -> 4, "c" -> 44)
    println(evaluator.finalize(normq3.asInstanceOf[CExpr]))

  }


}
