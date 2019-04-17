package shredding.nrc

import shredding.core._

object TestApp extends App
  with Shredding
  with ShreddedNRC
  with ShreddedPrinter
  with ShreddedEvaluator
  with Linearization {

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
      ), BagType(itemTp))

      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)
      val q1 = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> Project(xref, "b"))))

      println("Q1: " + quote(q1))
      println("Q1 eval: " + eval(q1))

      val q1shred = shred(q1)
      println("Shredded Q1: " + q1shred.quote)
      val q1trans = unshred(q1shred)
      println("Unshredded shredded Q1: " + quote(q1trans))
      println("Same as original Q1: " + q1trans.equals(q1))

      val q1lin = linearize(q1shred)
      println("Linearized Q1: " + quote(q1lin))
      println("Linearized Q1 eval: " + eval(q1lin).asInstanceOf[List[Any]].mkString("\n"))

      val ydef = VarDef("y", itemTp)
      val yref = TupleVarRef(ydef)
      val q2 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "grp" -> Project(xref, "a"),
          "bag" -> ForeachUnion(ydef, relationR,
            IfThenElse(
              Cond(OpEq, Project(xref, "a"), Project(yref, "a")),
              Singleton(Tuple("q" -> Project(yref, "b")))
            ))
        )))

      println("Q2: " + quote(q2))
      println("Q2 eval: " + eval(q2))

      val q2shred = shred(q2)
      println("Shredded Q2: " + q2shred.quote)
      val q2trans = unshred(q2shred)
      println("Unshredded shredded Q2: " + quote(q2trans))
      println("Same as original Q2: " + q2trans.equals(q2))

      val q2lin = linearize(q2shred)
      println("Linearized Q2: " + quote(q2lin))
      println("Linearized Q2 eval: " + eval(q2lin).asInstanceOf[List[Any]].mkString("\n"))
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

      val relationR = InputBag("R", List(
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
      ), BagType(itemTp))

      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)
      val wdef = VarDef("w", nestedItemTp)
      val wref = TupleVarRef(wdef)

      val q1 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "o5" -> Project(xref, "h"),
          "o6" ->
            ForeachUnion(wdef, BagProject(xref, "j"),
              Singleton(Tuple(
                "o7" -> Project(wref, "m"),
                "o8" -> Total(BagProject(wref, "k"))
              ))
            )
        )))

      println("Q1: " + quote(q1))
      println("Q1 eval: " + eval(q1))

      val q1shred = shred(q1)
      println("Shredded Q1: " + q1shred.quote)
      val q1trans = unshred(q1shred)
      println("Unshredded shredded Q1: " + quote(q1trans))
      println("Same as original Q1: " + q1trans.equals(q1))

      val q1lin = linearize(q1shred)
      println("Linearized Q1: " + quote(q1lin))
      println("Linearized Q1 eval: " + eval(q1lin).asInstanceOf[List[Any]].mkString("\n"))

      // Buggy query reported by Jaclyn
      val q2 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "o5" -> Project(xref, "h"),
          "o6" ->
            ForeachUnion(wdef, BagProject(xref, "j"),
              Singleton(Tuple(
                "o7" -> Project(wref, "m"),
                "o8" -> BagProject(wref, "k")
              ))
            )
        )))

      println("Q2: " + quote(q2))
      println("Q2 eval: " + eval(q2))

      val q2shred = shred(q2)
      println("Shredded Q2: " + q2shred.quote)
      val q2trans = unshred(q2shred)
      println("Unshredded shredded Q2: " + quote(q2trans))
      println("Same as original Q2: " + q2trans.equals(q2))

      val q2lin = linearize(q2shred)
      println("Linearized Q2: " + quote(q2lin))
      println("Linearized Q2 eval: " + eval(q2lin).asInstanceOf[List[Any]].mkString("\n"))
    }
  }

  object Example3 {

    def run(): Unit = {

      val depTp = TupleType("dno" -> IntType, "dname" -> StringType)
      val departments = InputBag("Departments",
        List(
          Map("dno" -> 1, "dname" -> "dept_one"),
          Map("dno" -> 2, "dname" -> "dept_two"),
          Map("dno" -> 3, "dname" -> "dept_three"),
          Map("dno" -> 4, "dname" -> "dept_four")
        ), BagType(depTp))

      val empTp = TupleType("dno" -> IntType, "ename" -> StringType)
      val employees = InputBag("Employees",
        List(
          Map("dno" -> 1, "ename" -> "emp_one"),
          Map("dno" -> 2, "ename" -> "emp_two"),
          Map("dno" -> 3, "ename" -> "emp_three"),
          Map("dno" -> 1, "ename" -> "emp_four"),
          Map("dno" -> 4, "ename" -> "emp_five")
        ), BagType(empTp))

      val d = VarDef("d", depTp)
      val e = VarDef("e", empTp)
      val q1 =
        ForeachUnion(d, departments,
          Singleton(Tuple(
            "D" -> Project(TupleVarRef(d), "dno"),
            "E" -> ForeachUnion(e, employees,
              IfThenElse(
                Cond(
                  OpEq,
                  Project(TupleVarRef(e), "dno"),
                  Project(TupleVarRef(d), "dno")),
                Singleton(TupleVarRef(e))
          )))))

      println("Q1: " + quote(q1))
      println("Q1 eval: " + eval(q1))

      val q1shred = shred(q1)
      println("Shredded Q1: " + q1shred.quote)
      val q1trans = unshred(q1shred)
      println("Unshredded shredded Q1: " + quote(q1trans))
      println("Same as original Q1: " + q1trans.equals(q1))

      val q1lin = linearize(q1shred)
      println("Linearized Q1: " + quote(q1lin))
      println("Linearized Q1 eval: " + eval(q1lin).asInstanceOf[List[Any]].mkString("\n"))

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
      println(shredR.quote)

      val unshredR = unshred(shredR)
      println(quote(unshredR, BagType(itemTp)))

      println("Same as original: " + relationR.equals(unshredR))
    }
  }

  object Example4 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("id" -> IntType, "name" -> StringType)
      val relationR = InputBag("R",
        List(
          Map("id" -> 42, "name" -> "Milos")
        ),
        BagType(itemTp)
      )

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

      println("Q1: " + quote(q1))
      println("Q1 eval: " + eval(q1))

      val q1shred = shred(q1)
      println("Shredded Q1: " + q1shred.quote)
//      val q1trans = unshred(q1shred)
//      println("Unshredded shredded Q1: " + quote(q1trans))
//      println("Same as original Q1: " + q1trans.equals(q1))

      val q1lin = linearize(q1shred)
      println("Linearized Q1: " + quote(q1lin))
//      println("Linearized Q1 eval: " + eval(q1lin).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example5 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("id" -> IntType, "name" -> StringType)
      val relationR = InputBag("R",
        List(
          Map("id" -> 42, "name" -> "Milos")
        ),
        BagType(itemTp)
      )

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
            "w2" -> Project(TupleVarRef(x1def), "id"),
            "w3" -> Singleton(TupleVarRef(x2def)))))
      val sq2 = ForeachUnion(x1def, relationR, Singleton(Tuple("w1" -> sq1)))

      val x3def = VarDef(Symbol.fresh(), sq2.tp.tp)
      val x4def = VarDef(Symbol.fresh(), sq1.tp.tp)

      val q1 =
        ForeachUnion(x3def, sq2,
          ForeachUnion(x4def, BagProject(TupleVarRef(x3def), "w1"),
            Singleton(Tuple("w4" -> Project(TupleVarRef(x4def), "w2")))))

      println("Q1: " + quote(q1))
      println("Q1 eval: " + eval(q1))

      val q1shred = shred(q1)
      println("Shredded Q1: " + q1shred.quote)
      val q1trans = unshred(q1shred)
      println("Unshredded shredded Q1: " + quote(q1trans))
      println("Same as original Q1: " + q1trans.equals(q1))

      val q1lin = linearize(q1shred)
      println("Linearized Q1: " + quote(q1lin))
      println("Linearized Q1 eval: " + eval(q1lin).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example6 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType)
      val relationR = InputBag("R",
        List(
          Map("a" -> 7, "b" -> 1234)
        ),
        BagType(itemTp)
      )

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
        Singleton(Tuple("w4" -> Project(TupleVarRef(x3def), "b"))))

      val sq2 = ForeachUnion(x2def, relationR,
        Singleton(Tuple(
          "w2" -> Project(TupleVarRef(x1def), "a"),
          "w3" -> sq1
        )))

      val sq3 = ForeachUnion(x1def, relationR,
        Singleton(Tuple(
          "w0" -> Project(TupleVarRef(x1def), "b"),
          "w1" -> sq2
        )))

      val x6def = VarDef(Symbol.fresh(), sq3.tp.tp)
      val x7def = VarDef(Symbol.fresh(), sq2.tp.tp)
      val x4def = VarDef(Symbol.fresh(), sq1.tp.tp)

      val q1 =
        ForeachUnion(x6def, sq3,
          ForeachUnion(x7def, BagProject(TupleVarRef(x6def), "w1"),
            Singleton(Tuple(
              "w4" -> Project(TupleVarRef(x7def), "w2"),
              "w5" -> ForeachUnion(x4def, BagProject(TupleVarRef(x7def), "w3"),
                Singleton(Tuple("w6" -> Project(TupleVarRef(x4def), "w4"))))
            ))
          ))

      println("Q1: " + quote(q1))
      println("Q1 eval: " + eval(q1))

      val q1shred = shred(q1)
      println("Shredded Q1: " + q1shred.quote)
      val q1trans = unshred(q1shred)
      println("Unshredded shredded Q1: " + quote(q1trans))
      println("Same as original Q1: " + q1trans.equals(q1))

      val q1lin = linearize(q1shred)
      println("Linearized Q1: " + quote(q1lin))
      println("Linearized Q1 eval: " + eval(q1lin).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

  object Example7 {

    import shredding.Utils.Symbol

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> IntType)
      val relationR = InputBag("R",
        List(
          Map("a" -> 7, "b" -> 1234)
        ),
        BagType(itemTp)
      )

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
        Singleton(Tuple("w4" -> Project(TupleVarRef(x3def), "b"))))

      val sq2 = ForeachUnion(x2def, relationR,
        Singleton(Tuple(
          "w2" -> Project(TupleVarRef(x1def), "a"),
          "w3" -> sq1
        )))

      val sq3 = ForeachUnion(x1def, relationR,
        Singleton(Tuple(
          "w0" -> Project(TupleVarRef(x1def), "b"),
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
                "w6" -> Project(TupleVarRef(x4def), "w4")
              ))
            )))

      println("Q1: " + quote(q1))
      println("Q1 eval: " + eval(q1))

      val q1shred = shred(q1)
      println("Shredded Q1: " + q1shred.quote)
      val q1trans = unshred(q1shred)
      println("Unshredded shredded Q1: " + quote(q1trans))
      println("Same as original Q1: " + q1trans.equals(q1))

      val q1lin = linearize(q1shred)
      println("Linearized Q1: " + quote(q1lin))
      println("Linearized Q1 eval: " + eval(q1lin).asInstanceOf[List[Any]].mkString("\n"))

    }
  }

//  Example1.run()
//  Example2.run()
//  Example3.run()
//  ExampleShredValue.run()
//  Example4.run()
//  Example5.run()
//  Example6.run()
  Example7.run()
}