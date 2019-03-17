package shredding.nrc2

object TestApp extends App {

  import NRCImplicits._

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = Relation("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
      ), BagType(itemTp))

      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)

      val q1 = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> Project(xref, "b"))))

      println("Q1: " + q1.quote)
      println("Q1 eval: " + q1.eval)

      val q1shred = q1.shred
      println("Shredded Q1: " + q1shred.quote)
      println("Unshredded shredded Q1: " + Shredder.unshred(q1shred).quote)
      println("Same as original Q1: " + Shredder.unshred(q1shred).equals(q1))

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

      println("Q2: " + q2.quote)
      println("Q2 eval: " + q2.eval)

      val q2shred = q2.shred
      println("Shredded Q2: " + q2shred.quote)
      println("Unshredded shredded Q2: " + Shredder.unshred(q2shred).quote)
      println("Same as original Q2: " + Shredder.unshred(q2shred).equals(q2))
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

      val relationR = Relation("R", List(
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
            ForeachUnion(wdef, Project(xref, "j").asInstanceOf[BagExpr],
              Singleton(Tuple(
                "o7" -> Project(wref, "m"),
                "o8" -> Mult(
                  Tuple("n" -> Project(wref, "n")),
                  Project(wref, "k").asInstanceOf[BagExpr]
                )
              ))
            )
        )))

      println("Q1: " + q1.quote)
      println("Q1 eval: " + q1.eval)

      val q1shred = q1.shred
      println("Shredded Q1: " + q1shred.quote)
      println("Unshredded shredded Q1: " + Shredder.unshred(q1shred).quote)
      println("Same as original Q1: " + Shredder.unshred(q1shred).equals(q1))
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

      val shredR = Shredder.shredValue(relationR, BagType(itemTp))

      println(Printer.quote(relationR, BagType(itemTp)))
      println(shredR.quote)

      val unshredR = Shredder.unshredValue(shredR)
      println(Printer.quote(unshredR, BagType(itemTp)))

      println("Same as original: " + relationR.equals(unshredR))
    }
  }

  Example1.run()
  Example2.run()

  ExampleShredValue.run()
}