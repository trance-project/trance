package framework.nrc

import framework.core._
import framework.examples.tpch._
import framework.runtime.{RuntimeContext, Evaluator, ScalaPrinter, ScalaShredding}
import framework.examples.simple._
import framework.examples.optimize._

object TestApp extends App
  with MaterializeNRC
  with Shredding
  with ScalaShredding
  with ScalaPrinter
  with Materialization
  with Printer
  with Evaluator
  with Optimizer {

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)

      val relationR = BagVarRef("R", BagType(itemTp))
      val xref = TupleVarRef("x", itemTp)
      val q1 = Program("Q1",
        ForeachUnion(xref, relationR, Singleton(Tuple("w" -> xref("b")))))

      println("[Ex1] Q1: " + quote(q1))

      val relationRValue = List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
      )
      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

      println("[Ex1] Eval")
      eval(q1, ctx)
      q1.statements.foreach { s =>
        println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
      }

      val q1shredraw = shred(q1)
      println("[Ex1] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex1] Shredded Q1 Optimized: " + quote(q1shred))

      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(inputBagName(flatName(relationR.name)), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(inputBagName(dictName(relationR.name)), shredR.flatTp), shredR.dict)
//      ctx.add(VarDef(inputBagName(relationR.name), relationR.tp), relationRValue)

      val q1lin = materialize(q1shred)
      println("[Ex1] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex1] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n\n"))

      val q1unshred = unshred(q1shred, q1lin.ctx)
      println("[Ex1] Unshredded Q1: " + quote(q1unshred))
//      println("[Ex1] Unshredded Q1 eval: " + eval(q1unshred, ctx).asInstanceOf[List[Any]].mkString("\n\n"))

      val yref = TupleVarRef("y", itemTp)
      val q2 = Program("Q2",
        ForeachUnion(xref, relationR,
          Singleton(Tuple(
            "grp" -> xref("a"),
            "bag" ->
              ForeachUnion(yref, relationR,
                IfThenElse(
                  Cmp(OpEq, xref("a"), yref("a")),
                  Singleton(Tuple("q" -> yref("b")))
                ))
          ))))

      println("[Ex1] Q2: " + quote(q2))
//      println("[Ex1] Q2 eval: " + eval(q2, ctx))

      val q2shredraw = shred(q2)
      println("[Ex1] Shredded Q2: " + quote(q2shredraw))

      val q2shred = optimize(q2shredraw)
      println("[Ex1] Shredded Q2 Optimized: " + quote(q2shred))

      val q2lin = materialize(q2shred)
      println("[Ex1] Materialized Q2: " + quote(q2lin.program))
//      println("[Ex1] Materialized Q2 eval: " + eval(q2lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

      val q2unshred = unshred(q2shred, q2lin.ctx)
      println("[Ex1] Unshredded Q2: " + quote(q2unshred))
//      println("[Ex1] Unshredded Q2 eval: " + eval(q2unshred, ctx).asInstanceOf[List[Any]].mkString("\n\n"))
    }
  }

  object Example2 {

    def run(): Unit = {

      val nested2ItemTp =
        TupleType(Map("n" -> IntType))

      val nestedItemTp = TupleType(Map(
        "m" -> StringType,
        "n" -> IntType,
        "k" -> BagType(nested2ItemTp)
      ))

      val itemTp = TupleType(Map(
        "h" -> IntType,
        "j" -> BagType(nestedItemTp)
      ))

      val relationR = BagVarRef("R", BagType(itemTp))

      val xref = TupleVarRef("x", itemTp)
      val wref = TupleVarRef("w", nestedItemTp)

      val q1 = Program("Q1",
        ForeachUnion(xref, relationR,
          Singleton(Tuple(
            "o5" -> xref("h"),
            "o6" ->
              ForeachUnion(wref, BagProject(xref, "j"),
                Singleton(Tuple("o7" -> wref("m"), "o8" -> Count(BagProject(wref, "k"))))
              )
          ))))

      println("[Ex2] Q1: " + quote(q1))

      val relationRValue = List(
        Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Milos",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Michael",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
                Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Jaclyn",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 69,
          "j" -> List(
            Map(
              "m" -> "Thomas",
              "n" -> 987,
              "k" -> List(
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 987)
              )
            )
          )
        )
      )
      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex2] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex2] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex2] Shredded Q2 Optimized: " + quote(q1shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex2] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex2] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

      // Buggy query reported by Jaclyn
      val q2 = Program("Q2",
        ForeachUnion(xref.varDef, relationR,
          Singleton(Tuple(
            "o5" -> xref("h"),
            "o6" ->
              ForeachUnion(wref.varDef, BagProject(xref, "j"),
                Singleton(Tuple("o7" -> wref("m"), "o8" -> BagProject(wref, "k")))
              )
          ))))

      println("[Ex2] Q2: " + quote(q2))
//      println("[Ex2] Q2 eval: " + eval(q2, ctx))

      val q2shredraw = shred(q2)
      println("[Ex2] Shredded Q2: " + quote(q2shredraw))

      val q2shred = optimize(q2shredraw)
      println("[Ex2] Shredded Q2 Optimized: " + quote(q2shred))

      val q2lin = materialize(q2shred)
      println("[Ex2] Materialized Q2: " + quote(q2lin.program))
//      println("[Ex2] Materialized Q2 eval: " + eval(q2lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))
    }
  }

  object Example3 {

    def run(): Unit = {

      val depTp = TupleType("dno" -> IntType, "dname" -> StringType)
      val departments = BagVarRef("Departments", BagType(depTp))

      val empTp = TupleType("dno" -> IntType, "ename" -> StringType)
      val employees = BagVarRef("Employees", BagType(empTp))

      val dr = TupleVarRef("d", depTp)
      val er = TupleVarRef("e", empTp)
      val q1 = Program("Q1",
        ForeachUnion(dr, departments,
          Singleton(Tuple(
            "D" -> dr("dno"),
            "E" -> ForeachUnion(er, employees,
              IfThenElse(
                Cmp(OpEq, er("dno"), dr("dno")),
                Singleton(er)
          ))))))

      println("[Ex3] Q1: " + quote(q1))

      val departmentsValue = List(
        Map("dno" -> 1, "dname" -> "dept_one"),
        Map("dno" -> 2, "dname" -> "dept_two"),
        Map("dno" -> 3, "dname" -> "dept_three"),
        Map("dno" -> 4, "dname" -> "dept_four")
      )
      val employeesValue = List(
        Map("dno" -> 1, "ename" -> "emp_one"),
        Map("dno" -> 2, "ename" -> "emp_two"),
        Map("dno" -> 3, "ename" -> "emp_three"),
        Map("dno" -> 1, "ename" -> "emp_four"),
        Map("dno" -> 4, "ename" -> "emp_five")
      )

      val ctx = new RuntimeContext()
      ctx.add(departments.varDef, departmentsValue)
      ctx.add(employees.varDef, employeesValue)

//      println("[Ex3] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex3] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex3] Shredded Q1 Optimized: " + quote(q1shred))

//      val shredDepartments = shred(departmentsValue, departments.tp)
//      val shredEmployees = shred(employeesValue, employees.tp)

//      ctx.add(VarDef(flatName(departments.name), shredDepartments.flatTp), shredDepartments.flat)
//      ctx.add(VarDef(dictName(departments.name), shredDepartments.dict.tp), shredDepartments.dict)
//      ctx.add(VarDef(flatName(employees.name), shredEmployees.flatTp), shredEmployees.flat)
//      ctx.add(VarDef(dictName(employees.name), shredEmployees.dict.tp), shredEmployees.dict)

      val q1lin = materialize(q1shred)
      println("[Ex3] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex3] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example4 {

    import framework.utils.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("id" -> IntType, "name" -> StringType)
      val relationR = BagVarRef("R", BagType(itemTp))

      val x0ref = TupleVarRef(Symbol.fresh(), itemTp)
      val x1ref = TupleVarRef(Symbol.fresh(), itemTp)

      val rq1 =
        ForeachUnion(x0ref, relationR,
          ForeachUnion(x1ref, relationR,
            Singleton(Tuple("w1" -> Singleton(x0ref), "w2" -> Singleton(x1ref)))))

      val x2ref = TupleVarRef(Symbol.fresh(), itemTp)
      val x3ref = TupleVarRef(Symbol.fresh(), TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp)))
      val x4ref = BagVarRef(Symbol.fresh(), BagType(TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp))))

      val q1 = Program("Q1",
        Let(x4ref, rq1,
          ForeachUnion(x3ref, x4ref,
            ForeachUnion(x2ref, relationR,
              Singleton(Tuple("w1" -> Singleton(x3ref), "w2" -> Singleton(x2ref)))))))

      println("[Ex4] Q1: " + quote(q1))

      val relationRValue = List(Map("id" -> 42, "name" -> "Milos"))

      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex4] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex4] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex4] Shredded Q1 Optimized: " + quote(q1shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex4] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex4] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example5 {

    import framework.utils.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("id" -> IntType, "name" -> StringType)
      val relationR = BagVarRef("R", BagType(itemTp))

      //
      //    For x3 in [[ For x1 in R Union
      //      Sng((w1 := For x2 in R Union
      //      Sng((w2 := x1.a, w3 := Sng(x2))))) ]] Union
      //    For x4 in x3.w1 Union
      //      Sng((w4 := x4.w2))
      //

      val x1ref = TupleVarRef(Symbol.fresh(), itemTp)
      val x2ref = TupleVarRef(Symbol.fresh(), itemTp)

      val sq1 =
        ForeachUnion(x2ref, relationR,
          Singleton(Tuple("w2" -> x1ref("id"), "w3" -> Singleton(x2ref))))
      val sq2 = ForeachUnion(x1ref, relationR, Singleton(Tuple("w1" -> sq1)))

      val x3ref = TupleVarRef(Symbol.fresh(), sq2.tp.tp)
      val x4ref = TupleVarRef(Symbol.fresh(), sq1.tp.tp)

      val q1 = Program("Q1",
        ForeachUnion(x3ref, sq2,
          ForeachUnion(x4ref, BagProject(x3ref, "w1"),
            Singleton(Tuple("w4" -> x4ref("w2"))))))

      println("[Ex5] Q1: " + quote(q1))

      val relationRValue = List(Map("id" -> 42, "name" -> "Milos"))

      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex5] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex5] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex5] Shredded Q1 Optimized: " + quote(q1shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex5] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex5] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example6 {

    import framework.utils.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType)
      val relationR = BagVarRef("R", BagType(itemTp))

      //  Q1: For x4 in For x1 in R Union
      //    Sng((w0 := x1.b, w1 := For x2 in R Union
      //    Sng((w2 := x1.a, w3 := For x3 in R Union
      //    Sng((w4 := x3.b)))))) Union
      //    For x5 in x4.w1 Union
      //    Sng((w4 := x5.w2, w5 := For x6 in x5.w3 Union
      //      Sng((w6 := x6.w4))))

      val x1ref = TupleVarRef(Symbol.fresh(), itemTp)
      val x2ref = TupleVarRef(Symbol.fresh(), itemTp)
      val x3ref = TupleVarRef(Symbol.fresh(), itemTp)

      val sq1 = ForeachUnion(x3ref, relationR,
        Singleton(Tuple("w4" -> x3ref("b"))))

      val sq2 = ForeachUnion(x2ref, relationR,
        Singleton(Tuple("w2" -> x1ref("a"), "w3" -> sq1)))

      val sq3 = ForeachUnion(x1ref, relationR,
        Singleton(Tuple("w0" -> x1ref("b"), "w1" -> sq2)))

      val x6ref = TupleVarRef(Symbol.fresh(), sq3.tp.tp)
      val x7ref = TupleVarRef(Symbol.fresh(), sq2.tp.tp)
      val x4ref = TupleVarRef(Symbol.fresh(), sq1.tp.tp)

      val q1 = Program("Q1",
        ForeachUnion(x6ref, sq3,
          ForeachUnion(x7ref, BagProject(x6ref, "w1"),
            Singleton(Tuple(
              "w4" -> x7ref("w2"),
              "w5" -> ForeachUnion(x4ref, BagProject(x7ref, "w3"),
                Singleton(Tuple("w6" -> x4ref("w4"))))
            ))
          )))

      println("[Ex6] Q1: " + quote(q1))

      val relationRValue = List(Map("a" -> 7, "b" -> 1234))

      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex6] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex6] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex6] Shredded Q1 Optimized: " + quote(q1shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex6] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex6] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example7 {

    import framework.utils.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType)
      val relationR = BagVarRef("R", BagType(itemTp))

      //  Q1: For x4 in For x1 in R Union
      //    Sng((w0 := x1.b, w1 := For x2 in R Union
      //    Sng((w2 := x1.a, w3 := For x3 in R Union
      //    Sng((w4 := x3.b)))))) Union
      //    For x5 in x4.w1 Union
      //    Sng((w4 := x5.w2, w5 := For x6 in x5.w3 Union
      //      Sng((w6 := x6.w4))))

      val x1ref = TupleVarRef(Symbol.fresh(), itemTp)
      val x2ref = TupleVarRef(Symbol.fresh(), itemTp)
      val x3ref = TupleVarRef(Symbol.fresh(), itemTp)

      val sq1 = ForeachUnion(x3ref, relationR,
        Singleton(Tuple("w4" -> x3ref("b"))))

      val sq2 = ForeachUnion(x2ref, relationR,
        Singleton(Tuple("w2" -> x1ref("a"), "w3" -> sq1)))

      val sq3 = ForeachUnion(x1ref, relationR,
        Singleton(Tuple("w0" -> x1ref("b"), "w1" -> sq2)))

      val x6ref = TupleVarRef(Symbol.fresh(), sq3.tp.tp)
      val x7ref = TupleVarRef(Symbol.fresh(), sq2.tp.tp)
      val x4ref = TupleVarRef(Symbol.fresh(), sq1.tp.tp)

      val q1 = Program("Q1",
        ForeachUnion(x6ref, sq3,
          ForeachUnion(x7ref, BagProject(x6ref, "w1"),
            ForeachUnion(x4ref, BagProject(x7ref, "w3"),
              Singleton(Tuple("w6" -> x4ref("w4")))))))

      println("[Ex7] Q1: " + quote(q1))

      val relationRValue = List(Map("a" -> 7, "b" -> 1234))

      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex7] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex7] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex7] Shredded Q1 Optimized: " + quote(q1shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex7] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex7] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example8 {

    import framework.utils.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType, "c" -> IntType)
      val relationR = BagVarRef("R", BagType(itemTp))

      // Q:
      // For x In R Union
      //   Sng((m1 := x.a, n1 :=
      //     For y In R Union If y.a == x.a Then
      //       Sng((m2 := y.b, n2 :=
      //         For z In R Union If z.a == x.a && z.b == y.b Then
      //           Sng((m3 := z.c))
      //       ))
      //   ))

      val xref = TupleVarRef(Symbol.fresh("x"), itemTp)
      val yref = TupleVarRef(Symbol.fresh("y"), itemTp)
      val zref = TupleVarRef(Symbol.fresh("z"), itemTp)

      val q1 = Program("Q1",
        ForeachUnion(xref.varDef, relationR, Singleton(Tuple(
          "m1" -> xref("a"),
          "n1" ->
            ForeachUnion(yref, relationR,
              IfThenElse(
                Cmp(OpEq, yref("a"), xref("a")),
                Singleton(Tuple(
                  "m2" -> yref("b"),
                  "n2" ->
                    ForeachUnion(zref, relationR,
                      IfThenElse(
                        Cmp(OpEq, zref("a"), xref("a")),
                        Singleton(Tuple("m3" -> zref("c")))
                      ))
                )))
            )))))

      println("[Ex8] Q1: " + quote(q1))

      val relationRValue = List(Map("a" -> 7, "b" -> 1234, "c" -> -321))

      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex8] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex8] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex8] Shredded Q1 Optimized: " + quote(q1shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex8] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex8] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))
    }
  }

  object Example9 {

    import framework.utils.Utils.Symbol

    def run(): Unit = {

      val nestedItemTp = TupleType("b" -> IntType, "c" -> IntType)
      val itemTp = TupleType("a" -> IntType, "s" -> BagType(nestedItemTp))
      val relationR = BagVarRef("R", BagType(itemTp))

      // Q = For x in R Union {<a'= x.a, s'=For y in x.s Union if y.c<5 then {y}>}

      val xref = TupleVarRef(Symbol.fresh("x"), itemTp)
      val yref = TupleVarRef(Symbol.fresh("y"), nestedItemTp)

      val q1 = Program("Q1",
        ForeachUnion(xref, relationR, Singleton(Tuple(
          "a1" -> xref("a"),
          "s1" ->
            ForeachUnion(yref, BagProject(xref, "s"),
              IfThenElse(Cmp(OpEq, yref("c"), Const(5, IntType)), Singleton(yref))
            )))))

      println("[Ex9] Q1: " + quote(q1))

      val relationRValue = List(
        Map("a" -> 10, "s" ->
          List(
            Map("b" -> 123, "c" -> -321),
            Map("b" -> 456, "c" -> 5),
            Map("b" -> 789, "c" -> 12),
          )
        ),
        Map("a" -> 20, "s" ->
          List(Map("b" -> 654, "c" -> 555))
        )
      )

      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex9] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex9] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex9] Shredded Q1 Optimized: " + quote(q1shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex9] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex9] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))
    }
  }

  object ExampleShredValue {

    def run(): Unit = {
      val nested2ItemTp =
        TupleType(Map("n" -> IntType))

      val nestedItemTp = TupleType(Map(
        "m" -> StringType,
        "n" -> IntType,
        "k" -> BagType(nested2ItemTp)
      ))

      val itemTp = TupleType(Map(
        "h" -> IntType,
        "j" -> BagType(nestedItemTp)
      ))

      val relationR = List(
        Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Milos",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Michael",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
                Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Jaclyn",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Joe",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Alice",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
                Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Bob",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 69,
          "j" -> List(
            Map(
              "m" -> "Thomas",
              "n" -> 987,
              "k" -> List(
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 987)
              )
            )
          )
        )
      )

//      val shredR = shred(relationR, BagType(itemTp))
//
//      println(quote(relationR, BagType(itemTp)))
//      println(quote(shredR))
//
//      val unshredR = unshred(shredR)
//      println(quote(unshredR, BagType(itemTp)))
//
//      println("Same as original: " + relationR.equals(unshredR))
    }
  }

  object Example10_DeDup {

    import framework.utils.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType)
      val relationR = BagVarRef("R", BagType(itemTp))

      // Q = DeDup(For x in R Union {<m = x.a, n = DeDup(For y in R Union if x.a = y.a then {<o = y.b>})>})

      val xref = TupleVarRef(Symbol.fresh("x"), itemTp)
      val yref = TupleVarRef(Symbol.fresh("y"), itemTp)

      val q1 = Program("Q1",
        DeDup(
          ForeachUnion(xref, relationR, Singleton(Tuple(
            "m" -> xref("a"),
            "n" ->
              DeDup(ForeachUnion(yref, relationR,
                IfThenElse(Cmp(OpEq, xref("a"), yref("a")), Singleton(Tuple("o" -> yref("b"))))
              ))
          )))))

      println("[Ex10] Q1: " + quote(q1))

      val relationRValue = List(
        Map("a" -> 1, "b" -> 12),
        Map("a" -> 1, "b" -> 33),
        Map("a" -> 1, "b" -> 33),
        Map("a" -> 1, "b" -> 45),
        Map("a" -> 2, "b" -> 123),
        Map("a" -> 2, "b" -> 1233)
      )

      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex10] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex10] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex10] Shredded Q1 Optimized: " + quote(q1shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex10] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex10] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))
    }
  }

  object Example11_Conditional {

    import framework.utils.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType)
      val relationR = BagVarRef("R", BagType(itemTp))

      val xref = TupleVarRef(Symbol.fresh("x"), itemTp)

      val q1 = Program("Q1",
        ForeachUnion(xref, relationR,
          IfThenElse(Cmp(OpEq, xref("a"), Const(5, IntType)), Singleton(xref))))

      val q2 = Program("Q2",
        ForeachUnion(xref, relationR,
          IfThenElse(
            Or(Cmp(OpEq, xref("a"), Const(5, IntType)), Cmp(OpEq, xref("a"), Const(2, IntType))),
            Singleton(xref))))

      val q3 = Program("Q3",
        ForeachUnion(xref, relationR,
          IfThenElse(
            And(Cmp(OpNe, xref("a"), Const(5, IntType)), Not(Cmp(OpEq, xref("a"), Const(2, IntType)))),
            Singleton(xref))))

      println("[Ex11] Q1: " + quote(q1))
      println("[Ex11] Q2: " + quote(q2))
      println("[Ex11] Q3: " + quote(q3))

      val relationRValue = List(
        Map("a" -> 1, "b" -> 12),
        Map("a" -> 2, "b" -> 33),
        Map("a" -> 3, "b" -> 33),
        Map("a" -> 4, "b" -> 45),
        Map("a" -> 5, "b" -> 123),
        Map("a" -> 6, "b" -> 1233)
      )

      val ctx = new RuntimeContext()
      ctx.add(relationR.varDef, relationRValue)

//      println("[Ex11] Q1 eval: " + eval(q1, ctx))
//      println("[Ex11] Q2 eval: " + eval(q2, ctx))
//      println("[Ex11] Q3 eval: " + eval(q3, ctx))

      val q1shredraw = shred(q1)
      println("[Ex11] Shredded Q1: " + quote(q1shredraw))

      val q2shredraw = shred(q2)
      println("[Ex11] Shredded Q2: " + quote(q2shredraw))

      val q3shredraw = shred(q3)
      println("[Ex11] Shredded Q3: " + quote(q3shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex11] Shredded Q1 Optimized: " + quote(q1shred))

      val q2shred = optimize(q2shredraw)
      println("[Ex11] Shredded Q2 Optimized: " + quote(q2shred))

      val q3shred = optimize(q3shredraw)
      println("[Ex11] Shredded Q3 Optimized: " + quote(q3shred))

//      val shredR = shred(relationRValue, relationR.tp)

//      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
//      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = materialize(q1shred)
      println("[Ex11] Materialized Q1: " + quote(q1lin.program))
//      println("[Ex11] Materialized Q1 eval: " + eval(q1lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

      val q2lin = materialize(q2shred)
      println("[Ex11] Materialized Q2: " + quote(q2lin.program))
//      println("[Ex11] Materialized Q2 eval: " + eval(q2lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))

      val q3lin = materialize(q3shred)
      println("[Ex11] Materialized Q3: " + quote(q3lin.program))
//      println("[Ex11] Materialized Q3 eval: " + eval(q3lin.program, ctx).asInstanceOf[List[Any]].mkString("\n"))
    }
  }


  object Example12_Genomic {

    import framework.examples.genomic.GenomicTests

    def run(): Unit = {

      val q1 = Program("Q1", GenomicTests.q1.asInstanceOf[Expr])

      println("[Ex12] Q1: " + quote(q1))

      val q1shredraw = shred(q1)
      println("[Ex12] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex12] Shredded Q1 Optimized: " + quote(q1shred))

      val q1lin = materialize(q1shred)
      println("[Ex12] Materialized Q1: " + quote(q1lin.program))
    }
  }

  object Example_Slender_Query1 {

    import framework.examples.tpch.TPCHQueries

    def run(): Unit = {

      val q1 = Program("Q1", TPCHQueries.query1_v2.asInstanceOf[Expr])

      println("[Ex13] Q1: " + quote(q1))

      val q1shredraw = shred(q1)
      println("[Ex13] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex13] Shredded Q1 Optimized: " + quote(q1shred))

      val q1lin = materialize(q1shred)
      println("[Ex13] Materialized Q1: " + quote(q1lin.program))
    }
  }

  object Example_Nesting_Rewrite {

    def run(): Unit = {

      val relC = BagVarRef("C", TPCHSchema.customertype)
      val cr = TupleVarRef("c", TPCHSchema.customertype.tp)

      val relO = BagVarRef("O", TPCHSchema.orderstype)
      val or = TupleVarRef("o", TPCHSchema.orderstype.tp)

      val relL = BagVarRef("L", TPCHSchema.lineittype)
      val lr = TupleVarRef("l", TPCHSchema.lineittype.tp)

      val relP = BagVarRef("P", TPCHSchema.parttype)
      val pr = TupleVarRef("p", TPCHSchema.parttype.tp)

      val q1 =
        ForeachUnion(pr, relP, IfThenElse(
          Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))

      println("[Nesting rewrite] Q1: " + quote(q1))

////      val q1opt = nestingRewrite(q1)
//
//      println("[Nesting rewrite] Q1 rewritten: " + quote(q1opt))
//
//      val q1full = TPCHQueries.query1_v2.asInstanceOf[Expr]
//
//      println("[Nesting rewrite] Full Q1: " + quote(q1full))
//
////      val q1fullopt = nestingRewrite(q1full)
////
////      println("[Nesting rewrite] Full Q1 rewritten: " + quote(q1fullopt))
//
//      val q1fullshredraw = shred(q1full)
//      println("[Nesting rewrite] Shredded Full Q1: " + quote(q1fullshredraw))
//
//      val q1fullshred = optimize(q1fullshredraw)
//      println("[Nesting rewrite] Shredded Q1 Optimized: " + quote(q1fullshred))
//
////      val q1fulllin = linearizeNoDomains(q1fullshred)
////      println("[Nesting rewrite] Linearized Q1: " + quote(q1fulllin))

    }
  }

  // DomainOptExample6 will hit an error since BagDictLet 
  // is not handled in the accessible linearizeNoDomains
  object DomainExamples{
    def run(): Unit = {

//      val q1 = DomainOptExample1.program(DomainOptExample1.name).rhs.asInstanceOf[Expr]
//      println("[Nesting rewrite] " + quote(q1))
//
//      val q1opt = nestingRewrite(q1)
//
//      println("[Nesting rewrite] rewritten: " + quote(q1opt))
//
//      val q1fullshredraw = shred(q1)
//      println("[Nesting rewrite] Shredded Full: " + quote(q1fullshredraw))
//
//      val q1fullshred = optimize(q1fullshredraw)
//      println("[Nesting rewrite] Shredded Optimized: " + quote(q1fullshred))
//
////      val q1fulllin = linearizeNoDomains(q1fullshred)
////      println("[Nesting rewrite] Linearized Q1: " + quote(q1fulllin))

    }
  }

  object ExtractExamples {
    def run(): Unit = {
      val q1 = Program("Q1", ExtractExample.query3.asInstanceOf[Expr])
      println(quote(q1))

      val sq1 = optimize(shred(q1))
      println(quote(sq1))
      val q1mat = materialize(sq1)
      println(quote(q1mat.program))
      val q1unshred = unshred(sq1, q1mat.ctx)
      println(quote(q1unshred))
    }
  }

  object Example_Unshredding {

    def run(): Unit = {
      val program = TPCHQuery1.program.asInstanceOf[Program]
      val shredded = shred(program)
      val materialized = materialize(shredded)
      println("Shredded: ")
      println(quote(shredded) + "\n")
      println("Materialized: ")
      println(quote(materialized.program) + "\n")
      val unshredded = unshred(shredded, materialized.ctx)
      println("Unshredded: ")
      println(quote(unshredded))
     
    }
  }

//  ExtractExamples.run()
//  Example_Unshredding.run()
  Example1.run()
//  Example2.run()
//  Example3.run()
//  Example4.run()
//  Example5.run()
//  Example6.run()
//  Example7.run()
//  Example8.run()
//  Example9.run()

//  ExampleShredValue.run()
//
//  Example10_DeDup.run()
//
//  Example11_Conditional.run()
//
//  Example12_Genomic.run()
//
//  Example_Slender_Query1.run()
//
//  Example_Nesting_Rewrite.run()

}

