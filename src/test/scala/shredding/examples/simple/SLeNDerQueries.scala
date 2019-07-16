package shredding.examples.simple

import org.scalatest.FunSuite
import shredding.core._
import shredding.examples.tpch.TPCHQueries._
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TestData}
import shredding.wmcc._


class SLeNDerQueries extends FunSuite {

  test("testQ6") {
    import TPCHQueries.nrc._
    import shredding.nrc.Printer
    println("")
    println("[TEST_Q6]")
    val q2Type = BagType(TupleType(Map("s_name" -> StringType, "customers2" -> BagType(TupleType(Map("c_name2" -> StringType))))))
    val xdef = VarDef("x", q2Type.tp)
    val xref = TupleVarRef(xdef)
    val cz = VarDef("cz", TupleType(Map("c_name2" -> StringType)))
    val czr = TupleVarRef(cz)
    val translator = new NRCTranslator {}
    val normalizer = new Finalizer(new BaseNormalizer {})
    val eval = new BaseScalaInterp {}
    val evaluator = new Finalizer(eval)
    eval.ctx("N") = TestData.nation
    eval.ctx("S") = TestData.supplier
    eval.ctx("PS") = TestData.partsupp
    eval.ctx("C") = TestData.customers
    eval.ctx("L") = TestData.lineitem
    eval.ctx("O") = TestData.orders
    eval.ctx("P") = TestData.part

    val query2 = ForeachUnion(s, relS,
      Singleton(Tuple("s_name" -> sr("s_name"), "customers2" -> ForeachUnion(l, relL,
        IfThenElse(Cmp(OpEq, sr("s_suppkey"), lr("l_suppkey")),
          ForeachUnion(o, relO,
            IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
              ForeachUnion(c, relC,
                IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                  Singleton(Tuple("c_name2" -> cr("c_name"))))))))))))
    val Q2 = VarDef("Q2", q2Type)
    val q2Translated = translator.translate(query2.asInstanceOf[translator.Expr])
    val normq2 = normalizer.finalize(q2Translated)
    println("translated: \n" + Printer.quote(normq2.asInstanceOf[CExpr]))

    val res2 = evaluator.finalize(normq2.asInstanceOf[CExpr])
    eval.ctx("Q2") = res2

    val query6 = ForeachUnion(TPCHQueries.c, TPCHQueries.relC,
      Singleton(
        Tuple("cname" -> TPCHQueries.cr("c_name"),
          "customers" -> ForeachUnion(xdef, BagVarRef(Q2),
            ForeachUnion(
              cz,
              BagProject(xref, "customers2"),
              IfThenElse(Cmp(OpEq, TPCHQueries.cr("c_name"), czr("c_name2")),
                Singleton(Tuple("s_name" -> xref("s_name")) //sng
                ))
            )
          )
        ))
    )
    val printer = new Printer {}
    println("query: \n" + printer.quote(query6.asInstanceOf[printer.Expr]))
    val qTranslated = translator.translate(query6.asInstanceOf[translator.Expr])
    val normq6 = normalizer.finalize(qTranslated)
    println("translated: \n" + Printer.quote(normq6.asInstanceOf[CExpr]))

    val res = evaluator.finalize(normq6.asInstanceOf[CExpr])
    println("Q6 results:\n" + res)

    val plan1 = Unnester.unnest(normq6.asInstanceOf[CExpr])((Nil, Nil, None))
    println("plan: \n" + Printer.quote(plan1.asInstanceOf[CExpr]))
    //eval.doteq = true
    println("Q6 shredded q results of the plan:\n"+evaluator.finalize(plan1.asInstanceOf[CExpr]))


  }


  // QUERY 7 : From the tpch database answer the following query:
  // for each country of origin list the parts that are supplied locally but bought by foreign customers.
  test("testQ7") {
    println("")
    val q7 = {
      import TPCHQueries.nrc._
      import shredding.nrc.Printer

      // QUERY 3 - this is the same as in TPCHQuery query3, but it also returns the keys.
      val query3 = ForeachUnion(p, relP,
        Singleton(Tuple("p_name" -> pr("p_name"), "suppliers" -> ForeachUnion(ps, relPS,
          IfThenElse(Cmp(OpEq, psr("ps_partkey"), pr("p_partkey")),
            ForeachUnion(s, relS,
              IfThenElse(Cmp(OpEq, sr("s_suppkey"), psr("ps_suppkey")),
                Singleton(Tuple("s_name" -> sr("s_name"),
                  "s_nationkey" -> sr("s_nationkey")
                )))))),
          "customers" -> ForeachUnion(l, relL,
            IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
              ForeachUnion(o, relO,
                IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                  ForeachUnion(c, relC,
                    IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                      Singleton(Tuple(
                        "c_name" -> cr("c_name"),
                        "c_nationkey" -> cr("c_nationkey"),
                      )))))))))))


      println("[TEST_Q7]" + query3.tp)
      // QUERY 7 : for each country of origin list the parts that are supplied locally but bought by foreign customers.

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
      val query7 = ForeachUnion(ndef, relN,
        Singleton(Tuple(
          "n_name" -> nref("n_name"),
          "part_names" -> ForeachUnion(xdef2, query3,
            IfThenElse(And(Cmp(OpGt, Total(suppliersCond1), Const(0, IntType)),
              Cmp(OpNe, Total(customersCond1), Const(0, IntType))),
              Singleton(Tuple(
                "p_name" -> q3ref("p_name"))
              )))
        )))

      // PRINT Q7
      val printer = new Printer {}
      println("query: \n" + printer.quote(query7.asInstanceOf[printer.Expr]))

      val translator = new NRCTranslator {}

      val normalizer = new Finalizer(new BaseNormalizer {})
      val qTranslated = translator.translate(query7.asInstanceOf[translator.Expr])
      val normq1 = normalizer.finalize(qTranslated)
      println("translated: \n" + Printer.quote(normq1.asInstanceOf[CExpr]))
      // EXECUTE Q7
      val eval = new BaseScalaInterp {}
      val evaluator = new Finalizer(eval)
      eval.ctx("N") = TestData.nation
      eval.ctx("S") = TestData.supplier
      eval.ctx("PS") = TestData.partsupp
      eval.ctx("C") = TestData.customers
      eval.ctx("L") = TestData.lineitem
      eval.ctx("O") = TestData.orders
      eval.ctx("P") = TestData.part
      val res = evaluator.finalize(normq1.asInstanceOf[CExpr])
      println("Q7 results:\n" + res)
      val head = res.asInstanceOf[List[Rec]].head
      assert(Rec("n_name" -> "Country1", "part_names" -> List()).equals(head))

      val customers = List(
        Rec("c_custkey" -> 1, "c_name" -> "Test Customer1", "c_nationkey" -> 1),
        Rec("c_custkey" -> 2, "c_name" -> "Test Customer2", "c_nationkey" -> 1),
        Rec("c_custkey" -> 3, "c_name" -> "Test Customer3", "c_nationkey" -> 2),
        Rec("c_custkey" -> 4, "c_name" -> "Test Customer4", "c_nationkey" -> 2),
        Rec("c_custkey" -> 5, "c_name" -> "Test Customer5", "c_nationkey" -> 3),
        Rec("c_custkey" -> 6, "c_name" -> "Test Customer6", "c_nationkey" -> 3)
      )
      eval.ctx("C") = customers
      val res2 = evaluator.finalize(normq1.asInstanceOf[CExpr])
      println("Q7 results with new context :\n" + res2)
      val head2 = res2.asInstanceOf[List[Rec]].head
//      assert(Rec("n_name" -> "Country1", "part_names" -> List(List(Rec("p_name" -> "part 5")))).equals(head2))
    }
  }



  // QUERY 7 : uses two different approach for Q7, executes both and prints the results.
  test("testQ7V2") {
    println("")
    val q7 = {
      import TPCHQueries.nrc._
      import shredding.nrc.Printer

      // QUERY 3 - this is the same as in TPCHQuery query3, but it also returns the keys.
      val query3 = ForeachUnion(p, relP,
        Singleton(Tuple("p_name" -> pr("p_name"), "suppliers" -> ForeachUnion(ps, relPS,
          IfThenElse(Cmp(OpEq, psr("ps_partkey"), pr("p_partkey")),
            ForeachUnion(s, relS,
              IfThenElse(Cmp(OpEq, sr("s_suppkey"), psr("ps_suppkey")),
                Singleton(Tuple("s_name" -> sr("s_name"),
                  "s_nationkey" -> sr("s_nationkey")
                )))))),
          "customers" -> ForeachUnion(l, relL,
            IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
              ForeachUnion(o, relO,
                IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                  ForeachUnion(c, relC,
                    IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                      Singleton(Tuple(
                        "c_name" -> cr("c_name"),
                        "c_nationkey" -> cr("c_nationkey"),
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
      val printer = new Printer {}
      println("query: \n" + printer.quote(query7.asInstanceOf[printer.Expr]))
      println("query_old: \n" + printer.quote(query7_old.asInstanceOf[printer.Expr]))
      val translator = new NRCTranslator {}
      val normalizer = new Finalizer(new BaseNormalizer {})
      val qTranslated = translator.translate(query7.asInstanceOf[translator.Expr])
      val qTranslated_old = translator.translate(query7_old.asInstanceOf[translator.Expr])
      val normq1 = normalizer.finalize(qTranslated)
      val normq1_old = normalizer.finalize(qTranslated_old)
      println("translated: \n" + Printer.quote(normq1.asInstanceOf[CExpr]))
      println("translated_old: \n" + Printer.quote(normq1_old.asInstanceOf[CExpr]))
      // EXECUTE Q7
      val eval = new BaseScalaInterp {}
      val evaluator = new Finalizer(eval)
      eval.ctx("N") = TestData.nation
      eval.ctx("S") = TestData.supplier
      eval.ctx("PS") = TestData.partsupp
      eval.ctx("C") = TestData.customers
      eval.ctx("L") = TestData.lineitem
      eval.ctx("O") = TestData.orders
      eval.ctx("P") = TestData.part



      // EXECUTE Q7
      val res_old = evaluator.finalize(normq1_old.asInstanceOf[CExpr])
      val res = evaluator.finalize(normq1.asInstanceOf[CExpr])
      println("Q7 results_old:\n" + res_old)
      println("Q7 results:\n" + res)

    }
  }
}
