package shredding.examples.simple

import org.scalatest.FunSuite
import shredding.core._
import shredding.examples.tpch.TPCHQueries.{c, cr, l, lr, o, or, p, pr, ps, psr, relC, relL, relO, relP, relPS, relS, s, sr}
import shredding.examples.tpch.{TPCHQueries, TPCHSchema, TestData}
import shredding.wmcc._

class SLeNDerQueries extends FunSuite {

  test("testQ6") {
    println("")
    println("[TEST_Q6]")

    val q6a = {
      import TPCHQueries.nrc._
      import shredding.nrc.Printer
      val q2Type = BagType(TupleType(Map("s_name" -> StringType, "customers2" -> BagType(TupleType(Map("c_name2" -> StringType))))))
      val xdef = VarDef("x", q2Type.tp)
      val xref = TupleVarRef(xdef)
      val cz = VarDef("cz", TupleType(Map("c_name2" -> StringType)))
      val czr = TupleVarRef(cz)
      val query6 = ForeachUnion(TPCHQueries.c, TPCHQueries.relC,
        Singleton(
          Tuple("cname" -> TPCHQueries.cr("c_name"),
            "customers" -> ForeachUnion(xdef, TPCHQueries.query2,
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
      val translator = new NRCTranslator {}
      val normalizer = new Finalizer(new BaseNormalizer {})
      val qTranslated = translator.translate(query6.asInstanceOf[translator.Expr])
      val normq1 = normalizer.finalize(qTranslated)
      println("translated: \n" + Printer.quote(normq1.asInstanceOf[CExpr]))

    }
  }



  test("testQ7") {
    println("")
    val q7 = {
      import TPCHQueries.nrc._
      import shredding.nrc.Printer

// QUERY 3
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
// QUERY 7

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
          Singleton(Tuple("count" -> Const(1,IntType))) /*,
          Singleton(Tuple("count" -> Const(0,IntType)))*/
        ).asInstanceOf[BagExpr])
      val cdef = VarDef("c", TupleType(Map("c_name" -> StringType, "c_nationkey" -> IntType)))
      val cref = TupleVarRef(cdef)
      val customersCond1 = ForeachUnion(cdef, q3ref("customers").asInstanceOf[BagExpr],
        IfThenElse(
          Cmp(OpEq, cref("c_nationkey"), nref("n_nationkey")),
          Singleton(Tuple("count" -> Const(1,IntType)))/*,
          Singleton(Tuple("count" -> Const(0,IntType)))*/
          ).asInstanceOf[BagExpr])
      val query7 = ForeachUnion(ndef, relN,
        Singleton(Tuple(
          "n_name" -> nref("n_name"),
          "part_names" -> ForeachUnion(xdef2, query3,
            IfThenElse(And(Cmp(OpGe,Total(suppliersCond1), Const(0,IntType)),
              Cmp(OpGt,Total(customersCond1), Const(0,IntType))),
            Singleton(Tuple(
            "p_name" -> q3ref("p_name"))
          )))
        )))

      // PRINT Q7
      val printer = new Printer {}
      println("query: \n" + printer.quote(query7.asInstanceOf[printer.Expr]))

      val translator = new NRCTranslator {}

      val normalizer = new Finalizer(new BaseNormalizer {})

//
//      { { (n_name := x8.n_name,part_names := { if ({ { { 1 | x15 <-
//          if (x11.s_nationkey == x8.n_nationkey) then { { (count := 1) } } } |
//        x11 <- S, x11.s_suppkey == x10.ps_suppkey } | x10 <- PS, x10.ps_partkey == x9.p_partkey }
//        >= 0, { { { { 1 | x16 <- if (x14.c_nationkey == x8.n_nationkey) then { { (count := 1) } } }
//        | x14 <- C, x14.c_custkey == x13.o_custkey } | x13 <- O, x13.o_orderkey == x12.l_orderkey }
//        | x12 <- L, x12.l_partkey == x9.p_partkey } > 0) then { { (p_name := x9.p_name) } } | x9 <- P }) } | x8 <- N }




      //val qTranslated = translator.translate(query3.asInstanceOf[translator.Expr])
      val qTranslated = translator.translate(query7.asInstanceOf[translator.Expr])

//      List(
//        Rec(p_name    :part 1,
//            suppliers :List(Rec(s_name:supplier A,s_nationkey:600)),
//            customers :List(Rec(c_name:Test Customer1,c_nationkey:1), Rec(c_name:Test Customer1,c_nationkey:1))),
//        Rec(p_name    :part 2,
//            suppliers :List(Rec(s_name:supplier A,s_nationkey:600)),
//            customers :List(Rec(c_name:Test Customer1,c_nationkey:1), Rec(c_name:Test Customer2,c_nationkey:1))),
//        Rec(p_name    :part 3,
//            suppliers :List(Rec(s_name:supplier B,s_nationkey:600)),
//            customers :List()),
//        Rec(p_name    :part 4,
//            suppliers :List(Rec(s_name:supplier C,s_nationkey:601)),
//            customers :List()),
//        Rec(p_name    :part 5,
//            suppliers :List(Rec(s_name:supplier C,s_nationkey:601)),
//            customers:List()))

      val normq1 = normalizer.finalize(qTranslated)
      println("translated: \n" + Printer.quote(normq1.asInstanceOf[CExpr]))

      // EXECUTE Q7
      val eval = new BaseScalaInterp{}
      val evaluator = new Finalizer(eval)

      eval.ctx("N") = TestData.nation
      eval.ctx("S") = TestData.supplier
      eval.ctx("PS") = TestData.partsupp
      eval.ctx("C") = TestData.customers
      eval.ctx("L") = TestData.lineitem
      eval.ctx("O") = TestData.orders
      eval.ctx("P") = TestData.part
      println(evaluator.finalize(normq1.asInstanceOf[CExpr]))

    }
  }

}
