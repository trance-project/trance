package shredding.wmcc

import shredding.core._
import shredding.examples.tpch._//{TPCHQueries, TPCHSchema, TPCHLoader}
import shredding.examples.simple._

object App {
    
  val translator = new NRCTranslator{}
  val normalizer = new Finalizer(new BaseNormalizer{})
  val runner = new PipelineRunner{}
  val anfBase = new BaseANF {}
  val anfer = new Finalizer(anfBase)
  
  def main(args: Array[String]){
    val eval = new BaseScalaInterp{}
    val evaluator = new Finalizer(eval)

    val q1 = translator.translate(NestedTests.q10.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1.asInstanceOf[CExpr]))
    eval.ctx("R") = NestedRelations.format4a
    println(evaluator.finalize(normq1.asInstanceOf[CExpr]))
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    println(evaluator.finalize(plan1))
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp1.asInstanceOf[CExpr]))

    val sq1 = runner.shredPipeline(NestedTests.q10.asInstanceOf[runner.Expr])
    val snormq1 = normalizer.finalize(sq1).asInstanceOf[CExpr]
    println(Printer.quote(snormq1.asInstanceOf[CExpr]))
    eval.ctx.clear
    eval.ctx("R__F") = 1
    eval.ctx("R__D") = NestedRelations.sformat4a
    println(evaluator.finalize(snormq1.asInstanceOf[CExpr]))
    val splan1 = Unnester.unnest(snormq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(splan1))
    println(evaluator.finalize(splan1))
    anfBase.reset
    val sanfedq1 = anfer.finalize(splan1)
    val sanfExp1 = anfBase.anf(sanfedq1.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(sanfExp1.asInstanceOf[CExpr]))

    /**val q1 = translator.translate(FlatTests.q1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1.asInstanceOf[CExpr]))
    eval.ctx("R") = FlatRelations.format1a
    println(evaluator.finalize(normq1.asInstanceOf[CExpr]))
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    println(evaluator.finalize(plan1))
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp1.asInstanceOf[CExpr]))**/

    /**val q2 = translator.translate(translator.Named(VarDef("Query5", TPCHQueries.query3.tp), 
              TPCHQueries.query3.asInstanceOf[translator.Expr]))
    val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
    println("")
    println(Printer.quote(normq2))
    eval.ctx("C") = TPCHLoader.loadCustomer[Customer].toList 
    eval.ctx("O") = TPCHLoader.loadOrders[Orders].toList 
    eval.ctx("L") = TPCHLoader.loadLineitem[Lineitem].toList 
    eval.ctx("P") = TPCHLoader.loadPart[Part].toList 
    eval.ctx("PS") = TPCHLoader.loadPartSupp[PartSupp].toList 
    eval.ctx("S") = TPCHLoader.loadSupplier[Supplier].toList 
    println("")
    println(evaluator.finalize(normq2))
    
    val plan2 = Unnester.unnest(normq2)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nPlan")
    println(Printer.quote(plan2))
    println(evaluator.finalize(plan2))
    anfBase.reset
    val anfedq2 = anfer.finalize(plan2)
    val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp2.asInstanceOf[CExpr]))
 
    val q5 = translator.translate(TPCHQueries.query5.asInstanceOf[translator.Expr])
    val normq5 = normalizer.finalize(q5).asInstanceOf[CExpr]
    println("")
    println(Printer.quote(normq5))
    println("")
    println(evaluator.finalize(normq5))
    
    val plan5 = Unnester.unnest(normq5)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nPlan")
    println(Printer.quote(plan5))
    println(evaluator.finalize(plan5))
    anfBase.reset
    val anfedq5 = anfer.finalize(plan5)
    val anfExp5 = anfBase.anf(anfedq5.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp5.asInstanceOf[CExpr]))

    val sq2 = runner.shredPipeline(TPCHQueries.query3.asInstanceOf[runner.Expr])
    val snormq2 = normalizer.finalize(sq2).asInstanceOf[CExpr]
    println("")
    println(Printer.quote(snormq2))
    eval.ctx.clear

    eval.ctx("C__F") = 1
    eval.ctx("C__D") = (List((1, TPCHLoader.loadCustomer[Customer].toList)), ())
    eval.ctx("O__F") = 2
    eval.ctx("O__D") = (List((2, TPCHLoader.loadOrders[Orders].toList)), ())
    eval.ctx("L__F") = 3
    eval.ctx("L__D") = (List((3, TPCHLoader.loadLineitem[Lineitem].toList)), ())
    eval.ctx("P__F") = 4
    eval.ctx("P__D") = (List((4, TPCHLoader.loadPart[Part].toList)), ())
    eval.ctx("PS__F") = 5
    eval.ctx("PS__D") = (List((5, TPCHLoader.loadPartSupp[PartSupp].toList)), ())
    eval.ctx("S__F") = 6
    eval.ctx("S__D") = (List((6, TPCHLoader.loadSupplier[Supplier].toList)), ())
    
    println(evaluator.finalize(snormq2))
    
    val splan2 = Unnester.unnest(snormq2)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nShredPlan")
    println(Printer.quote(splan2))
    println(evaluator.finalize(splan2))
    anfBase.reset
    val anfedqs2 = anfer.finalize(splan2)
    val anfExps2 = anfBase.anf(anfedqs2.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExps2))
    println(evaluator.finalize(anfExps2.asInstanceOf[CExpr]))

    eval.ctx("Query5__F") = eval.ctx("M_ctx1").asInstanceOf[List[_]].head.asInstanceOf[RecordValue]
    eval.ctx("Query5__D") = (eval.ctx("M_flat1"), (RecordValue("customers" -> (eval.ctx("M_flat3"), ()), "suppliers" -> (eval.ctx("M_flat2"), ()))))


    val sq5 = runner.shredPipeline(TPCHQueries.query5.asInstanceOf[runner.Expr])
    val snormq5 = normalizer.finalize(sq5).asInstanceOf[CExpr]
    println("")
    println(Printer.quote(snormq5))
    println(evaluator.finalize(snormq5))

    val splan5 = Unnester.unnest(snormq5)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nShredPlan")
    println(Printer.quote(splan5))
    println(evaluator.finalize(splan5))
    anfBase.reset
    val anfedqs5 = anfer.finalize(splan5)
    val anfExps5 = anfBase.anf(anfedqs5.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExps5))
    println(evaluator.finalize(anfExps5.asInstanceOf[CExpr]))**/


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
