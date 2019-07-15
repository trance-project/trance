package shredding.examples.simple

import org.scalatest.FunSuite
import shredding.core._
import shredding.examples.simple.{FlatRelations, FlatTests}
import shredding.wmcc.{BaseNormalizer, BaseScalaInterp, CExpr, Finalizer, NRCTranslator, PipelineRunner, Printer}
import shredding.examples.tpch.TPCHQueries.{c, cr, l, lr, o, or, p, pr, ps, psr, relC, relL, relO, relP, relPS, relS, s, sr}
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TestData}

class ShreddingTest extends FunSuite {

  val translator = new NRCTranslator{}
  val normalizer = new Finalizer(new BaseNormalizer{})
  val runner = new PipelineRunner{}

  test("testQ7V2 shredded") {
    println("")
    val q7 = {
      import translator._

      // QUERY 3 - this is the same as in TPCHQuery query3, but it also returns the keys.
      val query3 = ForeachUnion(p, relP.asInstanceOf[BagExpr],
        Singleton(Tuple("p_name" -> PrimitiveProject(pr.asInstanceOf[TupleExpr],"p_name"), "suppliers" -> ForeachUnion(ps, relPS.asInstanceOf[BagExpr],
          IfThenElse(Cmp(OpEq, PrimitiveProject(psr.asInstanceOf[TupleExpr],"ps_partkey"), PrimitiveProject(pr.asInstanceOf[TupleExpr],"p_partkey")),
            ForeachUnion(s, relS.asInstanceOf[BagExpr],
              IfThenElse(Cmp(OpEq, PrimitiveProject(sr.asInstanceOf[TupleExpr],"s_suppkey"), PrimitiveProject(psr.asInstanceOf[TupleExpr],"ps_suppkey")),
                Singleton(Tuple("s_name" -> PrimitiveProject(sr.asInstanceOf[TupleExpr],"s_name"),
                  "s_nationkey" -> PrimitiveProject(sr.asInstanceOf[TupleExpr],"s_nationkey")
                )))))),
          "customers" -> ForeachUnion(l, relL.asInstanceOf[BagExpr],
            IfThenElse(Cmp(OpEq, PrimitiveProject(lr.asInstanceOf[TupleExpr],"l_partkey"), PrimitiveProject(pr.asInstanceOf[TupleExpr],"p_partkey")),
              ForeachUnion(o, relO.asInstanceOf[BagExpr],
                IfThenElse(Cmp(OpEq, PrimitiveProject(or.asInstanceOf[TupleExpr],"o_orderkey"), PrimitiveProject(lr.asInstanceOf[TupleExpr],"l_orderkey")),
                  ForeachUnion(c, relC.asInstanceOf[BagExpr],
                    IfThenElse(Cmp(OpEq, PrimitiveProject(cr.asInstanceOf[TupleExpr],"c_custkey"), PrimitiveProject(or.asInstanceOf[TupleExpr],"o_custkey")),
                      Singleton(Tuple(
                        "c_name" -> PrimitiveProject(cr.asInstanceOf[TupleExpr],"c_name"),
                        "c_nationkey" -> PrimitiveProject(cr.asInstanceOf[TupleExpr],"c_nationkey"),
                      )))))))))))


      println("[TEST_Q7]" + query3.tp)
      // QUERY 7 : for each country of origin list the parts that are supplied locally but bought by foreign customers.
      //      For n in N Union
      //      Sng((n_name := n.n_name, part_names := For q3 in Query3 Union
      //        For s in q3.suppliers Union
      //        If (s.s_nationkey = n.n_nationkey AND Total(For c in q3.customers Union
      //          If (c.c_nationkey = n.n_nationkey)
      //          Then Sng((count := 1))) = 0)
      //        Then Sng((p_name := q3.p_name))))

      val ndef = VarDef("n", TPCHSchema.nationtype.tp)
      val relN = BagVarRef(VarDef("N", TPCHSchema.nationtype))
      val q3Type = BagType(TupleType(Map(
        "p_name" -> StringType,
        "suppliers" -> BagType(TupleType(Map("s_name" -> StringType, "s_nationkey" -> IntType))),
        "customers" -> BagType(TupleType(Map("c_name" -> StringType, "c_nationkey" -> IntType))))))
      val xdef2 = VarDef("x", q3Type.tp)

      val q3ref = TupleVarRef(xdef2)
      val nref = TupleVarRef(ndef)

      val sdef = VarDef("s", TupleType(Map("s_name" -> StringType, "s_nationkey" -> IntType)))
      val sref = TupleVarRef(sdef)

      val suppliersCond1 = ForeachUnion(sdef, q3ref("suppliers").asInstanceOf[BagExpr],
        IfThenElse(
          Cmp(OpEq, sref("s_nationkey"), nref("n_nationkey")),
          Singleton(Tuple("count" -> Const(1, IntType)))
        ).asInstanceOf[BagExpr])
      val cdef = VarDef("c", TupleType(Map("c_name" -> StringType, "c_nationkey" -> IntType)))
      val cref = TupleVarRef(cdef)
      val customersCond1 = ForeachUnion(cdef, q3ref("customers").asInstanceOf[BagExpr],
        IfThenElse(
          Cmp(OpNe, cref("c_nationkey"), nref("n_nationkey")),
          Singleton(Tuple("count" -> Const(1, IntType)))
        ).asInstanceOf[BagExpr])


      val customersCond1_new = ForeachUnion(cdef, q3ref("customers").asInstanceOf[BagExpr],
        IfThenElse(Cmp(OpEq, cref("c_nationkey"), nref("n_nationkey")),
          Singleton(Tuple("count" -> Const(1, IntType)))).asInstanceOf[BagExpr])

      val query7_old = ForeachUnion(ndef, relN,
        Singleton(Tuple(
          "n_name" -> nref("n_name"),
          "part_names" -> ForeachUnion(xdef2, query3,
            IfThenElse(And(Cmp(OpGt, Total(suppliersCond1), Const(0, IntType)),
              Cmp(OpNe, Total(customersCond1), Const(0, IntType))),
              Singleton(Tuple(
                "p_name" -> q3ref("p_name"))
              )))
        )))
      val query7 =  ForeachUnion(ndef, relN,
        Singleton(Tuple("n_name" -> nref("n_name"),
          "part_names" -> ForeachUnion(xdef2, query3,
            ForeachUnion(sdef,  q3ref("suppliers").asInstanceOf[BagExpr],
              IfThenElse(And(Cmp(OpEq, sref("s_nationkey"), nref("n_nationkey")),
                Cmp(OpEq, Total(customersCond1_new), Const(0, IntType))),
                Singleton(Tuple("p_name" ->  q3ref("p_name")))))))))

      // PRINT Q7
//      val printer = new Printer {}
//      println("query: \n" + printer.quote(query7.asInstanceOf[printer.Expr]))
//      val normalizer = new Finalizer(new BaseNormalizer {})
      val qTranslated = translator.translate(query7.asInstanceOf[translator.Expr])
      val normq1 = normalizer.finalize(qTranslated)
      println("translated: \n" + Printer.quote(normq1.asInstanceOf[CExpr]))



      val shreddedQ7 = runner.shredPipeline(query7.asInstanceOf[runner.Expr])
      val snormq7 = normalizer.finalize(shreddedQ7)
      println("Shredded q7: " + Printer.quote(snormq7.asInstanceOf[CExpr]))
      // EXECUTE Q7
      val eval = new BaseScalaInterp {}
      val evaluator = new Finalizer(eval)
      eval.ctx("N__F") = 1
      eval.ctx("N__D") = (List((1,TestData.nation)), ())


      eval.ctx("S__F") = 1
      eval.ctx("S__D") = (List((1,TestData.supplier)), ())
      eval.ctx("PS__F") = 1
      eval.ctx("PS__D") = (List((1,TestData.partsupp)), ())
      eval.ctx("C__F") = 1
      eval.ctx("C__D") = (List((1,TestData.customers)), ())
      eval.ctx("L__F") = 1
      eval.ctx("L__D") = (List((1,TestData.lineitem)), ())
      eval.ctx("O__F") = 1
      eval.ctx("O__D") = (List((1,TestData.orders)), ())
      eval.ctx("P__F") = 1
      eval.ctx("P__D") = (List((1,TestData.part)), ())

      //shredding

      // EXECUTE Q7
//      val res = evaluator.finalize(normq1.asInstanceOf[CExpr])
//      println("Q7 results old:\n" + res)
      println("Q7 shredded q results:\n"+evaluator.finalize(snormq7.asInstanceOf[CExpr]))


    }
  }
}
