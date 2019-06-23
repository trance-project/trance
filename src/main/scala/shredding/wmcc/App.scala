package shredding.wmcc

import shredding.core._
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}
import shredding.examples.simple._

object App {
    
  val translator = new NRCTranslator{}
  val normalizer = new Finalizer(new BaseNormalizer{})
  val runner = new PipelineRunner{}
  
  def main(args: Array[String]){
    val eval = new BaseScalaInterp{}
    val evaluator = new Finalizer(eval)
    
    /**val q1 = translator.translate(FlatTests.q1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1.asInstanceOf[CExpr]))
    eval.ctx("R") = FlatRelations.format1a
    println(evaluator.finalize(normq1.asInstanceOf[CExpr]))
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    println(evaluator.finalize(plan1))**/

    val q2 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])
    val sq2 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
    val snormq2 = normalizer.finalize(sq2).asInstanceOf[CExpr]
    println("")
    println(Printer.quote(normq2))
    println("")
    println(Printer.quote(snormq2))
    eval.ctx("C") = TPCHLoader.loadCustomer.toList 
    eval.ctx("O") = TPCHLoader.loadOrders.toList 
    eval.ctx("L") = TPCHLoader.loadLineitem.toList 
    eval.ctx("P") = TPCHLoader.loadPart.toList 
    eval.ctx("C__F") = 1
    eval.ctx("C__D") = (List((1, TPCHLoader.loadCustomer.toList)), ())
    eval.ctx("O__F") = 2
    eval.ctx("O__D") = (List((2, TPCHLoader.loadOrders.toList)), ())
    eval.ctx("L__F") = 3
    eval.ctx("L__D") = (List((3, TPCHLoader.loadLineitem.toList)), ())
    eval.ctx("P__F") = 4
    eval.ctx("P__D") = (List((4, TPCHLoader.loadPart.toList)), ())
    println("")
    println(evaluator.finalize(normq2))

    val plan2 = Unnester.unnest(normq2)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nPlan")
    println(Printer.quote(plan2))
    println(evaluator.finalize(plan2))

    val splan2 = Unnester.unnest(snormq2)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nShredPlan")
    println(Printer.quote(splan2))
    println(evaluator.finalize(splan2))

    /**val q3 = {    
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
          Singleton(Tuple("b'" -> yref("a"))), 
          Singleton(Tuple("b'" -> Const(-1, IntType)))).asInstanceOf[BagExpr]
      )))
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
    **/

    // bug in this one
    /**val q4 = translator.translate(NestedTests.q6.asInstanceOf[translator.Expr])
    val sq4 = runner.shredPipeline(NestedTests.q6.asInstanceOf[runner.Expr])
    val normq4 = normalizer.finalize(q4).asInstanceOf[CExpr]
    val snormq4 = normalizer.finalize(sq4).asInstanceOf[CExpr]
    println(Printer.quote(normq4.asInstanceOf[CExpr]))
    eval.ctx.clear
    eval.ctx("R") = NestedRelations.format2a
    println(evaluator.finalize(normq4.asInstanceOf[CExpr]))
    val plan4 = Unnester.unnest(normq4)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan4))
    println(evaluator.finalize(plan4))

    eval.ctx("R__F") = 1
    eval.ctx("R__D") = NestedRelations.format2Dd
    println(Printer.quote(snormq4.asInstanceOf[CExpr]))
    val splan4 = Unnester.unnest(snormq4)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(splan4))
    println(evaluator.finalize(splan4))**/

  }


}
