package shredding.nrc2

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

  Example1.run()
  Example2.run()
  ExampleShredValue.run()
}