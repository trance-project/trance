package shredding.nrc

import shredding.core._
import shredding.runtime.{Context, Evaluator, ScalaShredding, ScalaPrinter}

object TestApp extends App
  with NRC
  with ShredNRC
  with Shredding
  with ScalaShredding
  with ScalaPrinter
  with LinearizedNRC
  with Linearization
  with Printer
  with Evaluator
  with Optimizer {

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))

      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)
      val q1 = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> xref("b"))))

      println("[Ex1] Q1: " + quote(q1))

      val relationRValue = List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
      )

      val ctx = new Context()
      ctx.add(relationR.varDef, relationRValue)
      println("[Ex1] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex1] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex1] Shredded Q1 Optimized: " + quote(q1shred))

//      val q1trans = unshred(q1shred)
//      println("[Ex1] Unshredded shredded Q1: " + quote(q1trans))
//      println("[Ex1] Same as original Q1: " + q1trans.equals(q1))

      val shredR = shred(relationRValue, relationR.tp)

      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

       val q1lin = linearize(q1shred)
      println("[Ex1] Linearized Q1: " + quote(q1lin))
      println("[Ex1] Linearized Q1 eval: " + eval(q1lin, ctx).asInstanceOf[List[Any]].mkString("\n"))

      val ydef = VarDef("y", itemTp)
      val yref = TupleVarRef(ydef)
      val q2 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "grp" -> xref("a"),
          "bag" -> ForeachUnion(ydef, relationR,
            IfThenElse(
              Cond(OpEq, xref("a"), yref("a")),
              Singleton(Tuple("q" -> yref("b")))
            ))
        )))

      println("[Ex1] Q2: " + quote(q2))
      println("[Ex1] Q2 eval: " + eval(q2, ctx))

      val q2shredraw = shred(q2)
      println("[Ex1] Shredded Q2: " + quote(q2shredraw))

      val q2shred = optimize(q2shredraw)
      println("[Ex1] Shredded Q2 Optimized: " + quote(q2shred))


//      val q2trans = unshred(q2shred)
//      println("[Ex1] Unshredded shredded Q2: " + quote(q2trans))
//      println("[Ex1] Same as original Q2: " + q2trans.equals(q2))

      val q2lin = linearize(q2shred)
      println("[Ex1] Linearized Q2: " + quote(q2lin))
      println("[Ex1] Linearized Q2 eval: " + eval(q2lin, ctx).asInstanceOf[List[Any]].mkString("\n"))
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

      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))

      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)
      val wdef = VarDef("w", nestedItemTp)
      val wref = TupleVarRef(wdef)

      val q1 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "o5" -> xref("h"),
          "o6" ->
            ForeachUnion(wdef, BagProject(xref, "j"),
              Singleton(Tuple(
                "o7" -> wref("m"),
                "o8" -> Total(BagProject(wref, "k"))
              ))
            )
        )))

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

      val ctx = new Context()
      ctx.add(relationR.varDef, relationRValue)
      println("[Ex2] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex2] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex2] Shredded Q2 Optimized: " + quote(q1shred))

//      val q1trans = unshred(q1shred)
//      println("[Ex2] Unshredded shredded Q1: " + quote(q1trans))
//      println("[Ex2] Same as original Q1: " + q1trans.equals(q1))

      val shredR = shred(relationRValue, relationR.tp)

      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = linearize(q1shred)
      println("[Ex2] Linearized Q1: " + quote(q1lin))
      println("[Ex2] Linearized Q1 eval: " + eval(q1lin, ctx).asInstanceOf[List[Any]].mkString("\n"))

      // Buggy query reported by Jaclyn
      val q2 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "o5" -> xref("h"),
          "o6" ->
            ForeachUnion(wdef, BagProject(xref, "j"),
              Singleton(Tuple(
                "o7" -> wref("m"),
                "o8" -> BagProject(wref, "k")
              ))
            )
        )))

      println("[Ex2] Q2: " + quote(q2))
      println("[Ex2] Q2 eval: " + eval(q2, ctx))

      val q2shredraw = shred(q2)
      println("[Ex2] Shredded Q2: " + quote(q2shredraw))

      val q2shred = optimize(q2shredraw)
      println("[Ex2] Shredded Q2 Optimized: " + quote(q2shred))

//      val q2trans = unshred(q2shred)
//      println("[Ex2] Unshredded shredded Q2: " + quote(q2trans))
//      println("[Ex2] Same as original Q2: " + q2trans.equals(q2))

      val q2lin = linearize(q2shred)
      println("[Ex2] Linearized Q2: " + quote(q2lin))
      println("[Ex2] Linearized Q2 eval: " + eval(q2lin, ctx).asInstanceOf[List[Any]].mkString("\n"))
    }
  }

  object Example3 {

    def run(): Unit = {

      val depTp = TupleType("dno" -> IntType, "dname" -> StringType)
      val departments = BagVarRef(VarDef("Departments", BagType(depTp)))

      val empTp = TupleType("dno" -> IntType, "ename" -> StringType)
      val employees = BagVarRef(VarDef("Employees", BagType(empTp)))

      val d = VarDef("d", depTp)
      val e = VarDef("e", empTp)
      val q1 =
        ForeachUnion(d, departments,
          Singleton(Tuple(
            "D" -> TupleVarRef(d)("dno"),
            "E" -> ForeachUnion(e, employees,
              IfThenElse(
                Cond(
                  OpEq,
                  TupleVarRef(e)("dno"),
                  TupleVarRef(d)("dno")),
                Singleton(TupleVarRef(e))
          )))))

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

      val ctx = new Context()
      ctx.add(departments.varDef, departmentsValue)
      ctx.add(employees.varDef, employeesValue)

      println("[Ex3] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex3] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex3] Shredded Q1 Optimized: " + quote(q1shred))

//      val q1trans = unshred(q1shred)
//      println("[Ex3] Unshredded shredded Q1: " + quote(q1trans))
//      println("[Ex3] Same as original Q1: " + q1trans.equals(q1))

      val shredDepartments = shred(departmentsValue, departments.tp)
      val shredEmployees = shred(employeesValue, employees.tp)

      ctx.add(VarDef(flatName(departments.name), shredDepartments.flatTp), shredDepartments.flat)
      ctx.add(VarDef(dictName(departments.name), shredDepartments.dict.tp), shredDepartments.dict)
      ctx.add(VarDef(flatName(employees.name), shredEmployees.flatTp), shredEmployees.flat)
      ctx.add(VarDef(dictName(employees.name), shredEmployees.dict.tp), shredEmployees.dict)

      val q1lin = linearize(q1shred)
      println("[Ex3] Linearized Q1: " + quote(q1lin))
      println("[Ex3] Linearized Q1 eval: " + eval(q1lin, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example4 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("id" -> IntType, "name" -> StringType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))

      val x0def = VarDef(Symbol.fresh(), itemTp)
      val x1def = VarDef(Symbol.fresh(), itemTp)

      val rq1 =
        ForeachUnion(x0def, relationR,
          ForeachUnion(x1def, relationR,
            Singleton(Tuple(
              "w1" -> Singleton(TupleVarRef(x0def)),
              "w2" -> Singleton(TupleVarRef(x1def)))
            )))

      val x2def = VarDef(Symbol.fresh(), itemTp)
      val x3def = VarDef(Symbol.fresh(), TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp)))
      val x4def = VarDef(Symbol.fresh(), BagType(TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp))))

      val q1 =
        Let(x4def, rq1,
          ForeachUnion(x3def, BagVarRef(x4def),
            ForeachUnion(x2def, relationR,
              Singleton(Tuple(
                "w1" -> Singleton(TupleVarRef(x3def)),
                "w2" -> Singleton(TupleVarRef(x2def)))
              ))))

      println("[Ex4] Q1: " + quote(q1))

      val relationRValue = List(Map("id" -> 42, "name" -> "Milos"))

      val ctx = new Context()
      ctx.add(relationR.varDef, relationRValue)

      println("[Ex4] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex4] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex4] Shredded Q1 Optimized: " + quote(q1shred))

//      val q1trans = unshred(q1shred)
//      println("[Ex4] Unshredded shredded Q1: " + quote(q1trans))
//      println("[Ex4] Same as original Q1: " + q1trans.equals(q1))

      val shredR = shred(relationRValue, relationR.tp)

      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = linearize(q1shred)
      println("[Ex4] Linearized Q1: " + quote(q1lin))
      println("[Ex4] Linearized Q1 eval: " + eval(q1lin, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example5 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("id" -> IntType, "name" -> StringType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))

      //
      //    For x3 in [[ For x1 in R Union
      //      Sng((w1 := For x2 in R Union
      //      Sng((w2 := x1.a, w3 := Sng(x2))))) ]] Union
      //    For x4 in x3.w1 Union
      //      Sng((w4 := x4.w2))
      //

      val x1def = VarDef(Symbol.fresh(), itemTp)
      val x2def = VarDef(Symbol.fresh(), itemTp)

      val sq1 =
        ForeachUnion(x2def, relationR,
          Singleton(Tuple(
            "w2" -> TupleVarRef(x1def)("id"),
            "w3" -> Singleton(TupleVarRef(x2def)))))
      val sq2 = ForeachUnion(x1def, relationR, Singleton(Tuple("w1" -> sq1)))

      val x3def = VarDef(Symbol.fresh(), sq2.tp.tp)
      val x4def = VarDef(Symbol.fresh(), sq1.tp.tp)

      val q1 =
        ForeachUnion(x3def, sq2,
          ForeachUnion(x4def, BagProject(TupleVarRef(x3def), "w1"),
            Singleton(Tuple("w4" -> TupleVarRef(x4def)("w2")))))

      println("[Ex5] Q1: " + quote(q1))

      val relationRValue = List(Map("id" -> 42, "name" -> "Milos"))

      val ctx = new Context()
      ctx.add(relationR.varDef, relationRValue)

      println("[Ex5] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex5] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex5] Shredded Q1 Optimized: " + quote(q1shred))

//      val q1trans = unshred(q1shred)
//      println("[Ex5] Unshredded shredded Q1: " + quote(q1trans))
//      println("[Ex5] Same as original Q1: " + q1trans.equals(q1))

      val shredR = shred(relationRValue, relationR.tp)

      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = linearize(q1shred)
      println("[Ex5] Linearized Q1: " + quote(q1lin))
      println("[Ex5] Linearized Q1 eval: " + eval(q1lin, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example6 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))

      //  Q1: For x4 in For x1 in R Union
      //    Sng((w0 := x1.b, w1 := For x2 in R Union
      //    Sng((w2 := x1.a, w3 := For x3 in R Union
      //    Sng((w4 := x3.b)))))) Union
      //    For x5 in x4.w1 Union
      //    Sng((w4 := x5.w2, w5 := For x6 in x5.w3 Union
      //      Sng((w6 := x6.w4))))

      val x1def = VarDef(Symbol.fresh(), itemTp)
      val x2def = VarDef(Symbol.fresh(), itemTp)
      val x3def = VarDef(Symbol.fresh(), itemTp)

      val sq1 = ForeachUnion(x3def, relationR,
        Singleton(Tuple("w4" -> TupleVarRef(x3def)("b"))))

      val sq2 = ForeachUnion(x2def, relationR,
        Singleton(Tuple(
          "w2" -> TupleVarRef(x1def)("a"),
          "w3" -> sq1
        )))

      val sq3 = ForeachUnion(x1def, relationR,
        Singleton(Tuple(
          "w0" -> TupleVarRef(x1def)("b"),
          "w1" -> sq2
        )))

      val x6def = VarDef(Symbol.fresh(), sq3.tp.tp)
      val x7def = VarDef(Symbol.fresh(), sq2.tp.tp)
      val x4def = VarDef(Symbol.fresh(), sq1.tp.tp)

      val q1 =
        ForeachUnion(x6def, sq3,
          ForeachUnion(x7def, BagProject(TupleVarRef(x6def), "w1"),
            Singleton(Tuple(
              "w4" -> TupleVarRef(x7def)("w2"),
              "w5" -> ForeachUnion(x4def, BagProject(TupleVarRef(x7def), "w3"),
                Singleton(Tuple("w6" -> TupleVarRef(x4def)("w4"))))
            ))
          ))

      println("[Ex6] Q1: " + quote(q1))

      val relationRValue = List(Map("a" -> 7, "b" -> 1234))

      val ctx = new Context()
      ctx.add(relationR.varDef, relationRValue)

      println("[Ex6] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex6] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex6] Shredded Q1 Optimized: " + quote(q1shred))

      //      val q1trans = unshred(q1shred)
//      println("[Ex6] Unshredded shredded Q1: " + quote(q1trans))
//      println("[Ex6] Same as original Q1: " + q1trans.equals(q1))

      val shredR = shred(relationRValue, relationR.tp)

      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = linearize(q1shred)
      println("[Ex6] Linearized Q1: " + quote(q1lin))
      println("[Ex6] Linearized Q1 eval: " + eval(q1lin, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example7 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))

      //  Q1: For x4 in For x1 in R Union
      //    Sng((w0 := x1.b, w1 := For x2 in R Union
      //    Sng((w2 := x1.a, w3 := For x3 in R Union
      //    Sng((w4 := x3.b)))))) Union
      //    For x5 in x4.w1 Union
      //    Sng((w4 := x5.w2, w5 := For x6 in x5.w3 Union
      //      Sng((w6 := x6.w4))))

      val x1def = VarDef(Symbol.fresh(), itemTp)
      val x2def = VarDef(Symbol.fresh(), itemTp)
      val x3def = VarDef(Symbol.fresh(), itemTp)

      val sq1 = ForeachUnion(x3def, relationR,
        Singleton(Tuple("w4" -> TupleVarRef(x3def)("b"))))

      val sq2 = ForeachUnion(x2def, relationR,
        Singleton(Tuple(
          "w2" -> TupleVarRef(x1def)("a"),
          "w3" -> sq1
        )))

      val sq3 = ForeachUnion(x1def, relationR,
        Singleton(Tuple(
          "w0" -> TupleVarRef(x1def)("b"),
          "w1" -> sq2
        )))

      val x6def = VarDef(Symbol.fresh(), sq3.tp.tp)
      val x7def = VarDef(Symbol.fresh(), sq2.tp.tp)
      val x4def = VarDef(Symbol.fresh(), sq1.tp.tp)

      val q1 =
        ForeachUnion(x6def, sq3,
          ForeachUnion(x7def, BagProject(TupleVarRef(x6def), "w1"),
            ForeachUnion(x4def, BagProject(TupleVarRef(x7def), "w3"),
              Singleton(Tuple(
                "w6" -> TupleVarRef(x4def)("w4")
              ))
            )))

      println("[Ex7] Q1: " + quote(q1))

      val relationRValue = List(Map("a" -> 7, "b" -> 1234))

      val ctx = new Context()
      ctx.add(relationR.varDef, relationRValue)

      println("[Ex7] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex7] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex7] Shredded Q1 Optimized: " + quote(q1shred))

      //      val q1trans = unshred(q1shred)
//      println("[Ex7] Unshredded shredded Q1: " + quote(q1trans))
//      println("[Ex7] Same as original Q1: " + q1trans.equals(q1))

      val shredR = shred(relationRValue, relationR.tp)

      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = linearize(q1shred)
      println("[Ex7] Linearized Q1: " + quote(q1lin))
      println("[Ex7] Linearized Q1 eval: " + eval(q1lin, ctx).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example8 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType, "c" -> IntType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))

      // Q:
      // For x In R Union
      //   Sng((m1 := x.a, n1 :=
      //     For y In R Union If y.a == x.a Then
      //       Sng((m2 := y.b, n2 :=
      //         For z In R Union If z.a == x.a && z.b == y.b Then
      //           Sng((m3 := z.c))
      //       ))
      //   ))

      val xdef = VarDef(Symbol.fresh("x"), itemTp)
      val xref = TupleVarRef(xdef)
      val ydef = VarDef(Symbol.fresh("y"), itemTp)
      val yref = TupleVarRef(ydef)
      val zdef = VarDef(Symbol.fresh("z"), itemTp)
      val zref = TupleVarRef(zdef)

      val q1 =
        ForeachUnion(xdef, relationR, Singleton(Tuple(
          "m1" -> xref("a"),
          "n1" ->
            ForeachUnion(ydef, relationR,
              IfThenElse(
                Cond(OpEq, yref("a"), xref("a")),
                Singleton(Tuple(
                  "m2" -> yref("b"),
                  "n2" ->
                    ForeachUnion(zdef, relationR,
                      IfThenElse(
                        Cond(OpEq, zref("a"), xref("a")),
                        Singleton(Tuple("m3" -> zref("c")))
                      )
                    )
                ))
              )
            )
        )))

      println("[Ex8] Q1: " + quote(q1))

      val relationRValue = List(Map("a" -> 7, "b" -> 1234, "c" -> -321))

      val ctx = new Context()
      ctx.add(relationR.varDef, relationRValue)

      println("[Ex8] Q1 eval: " + eval(q1, ctx))

      val q1shredraw = shred(q1)
      println("[Ex8] Shredded Q1: " + quote(q1shredraw))

      val q1shred = optimize(q1shredraw)
      println("[Ex8] Shredded Q1 Optimized: " + quote(q1shred))

      //      val q1trans = unshred(q1shred)
      //      println("[Ex8] Unshredded shredded Q1: " + quote(q1trans))
      //      println("[Ex8] Same as original Q1: " + q1trans.equals(q1))

      val shredR = shred(relationRValue, relationR.tp)

      ctx.add(VarDef(flatName(relationR.name), shredR.flatTp), shredR.flat)
      ctx.add(VarDef(dictName(relationR.name), shredR.dict.tp), shredR.dict)

      val q1lin = linearize(q1shred)
      println("[Ex8] Linearized Q1: " + quote(q1lin))
      println("[Ex8] Linearized Q1 eval: " + eval(q1lin, ctx).asInstanceOf[List[Any]].mkString("\n"))
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

      val shredR = shred(relationR, BagType(itemTp))

      println(quote(relationR, BagType(itemTp)))
      println(quote(shredR))

      val unshredR = unshred(shredR)
      println(quote(unshredR, BagType(itemTp)))

      println("Same as original: " + relationR.equals(unshredR))
    }
  }

  Example1.run()
  Example2.run()
  Example3.run()
  Example4.run()
  Example5.run()
  Example6.run()
  Example7.run()
  Example8.run()

//  ExampleShredValue.run()
}