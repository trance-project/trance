package shredding.nrc2

object TestApp extends App {

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val x = TupleVarDef("x", itemTp)
      val x2 = TupleVarDef("y2", itemTp)
      val relationR = Relation("R", PhysicalBag(itemTp,
        Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType)),
        Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType)),
        Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType)),
        Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType))
      ))
      val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> VarRef(x, "b"))))

      println(Printer.quote(q1))
      val cq1 = Translator.translate(q1)
      println(Printer.quote(cq1))
      println(Evaluator.eval(q1))
      
      println(" -------------------------------------")
      println("working on transformation")
      
      val x4 = PrimitiveVarDef("x4", IntType)
      val q11 = ForeachUnion(x, Singleton(Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType))), 
                Singleton(Tuple("w" -> VarRef(x, "a"))))
      println(Printer.quote(q11))
      println(q11)
      val cq11 = Translator.translate(q11)
      println(Printer.quote(cq11))

      val q3 = ForeachUnion(x, Singleton(Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType))),
                            Singleton(Tuple("w" -> VarRef(x, "b"))))
      println(Printer.quote(q3))
      val cq3 = Translator.translate(q3)
      println(Printer.quote(cq3))
      println("")
            
      val q4 = ForeachUnion(x, relationR, ForeachUnion(x2, relationR, 
                Singleton(Tuple("w1" -> VarRef(x, "b"), "w2" -> VarRef(x2, "b")))))
      println(Printer.quote(q4))
      val cq4 = Translator.translate(q4)
      println(Printer.quote(cq4))
      println("")

      val q5 = ForeachUnion(x, ForeachUnion(x2, relationR, 
                Singleton(Tuple("a" -> VarRef(x2, "a"), "b" -> VarRef(x2, "b")))), 
                  Singleton(Tuple("w2" -> VarRef(x, "b"))))
      println(Printer.quote(q5))
      val cq5 = Translator.translate(q5)
      println(Printer.quote(cq5))
      println("")

      val q6 = ForeachUnion(x, relationR, IfThenElse(List(Cond(OpGt, VarRef(x, "a"), Const("35", IntType))), 
                Singleton(Tuple("w1" -> VarRef(x, "b"))), None)) 
      println(Printer.quote(q6))
      val cq6 = Translator.translate(q6)
      println(Printer.quote(cq6))
      println("")

      val q8 = ForeachUnion(x, relationR, IfThenElse(List(Cond(OpGt, VarRef(x, "a"), Const("35", IntType)),
                                                     Cond(OpGt, Const("45", IntType), VarRef(x, "a"))), 
                            Singleton(Tuple("w1" -> VarRef(x, "b"))), None))
      println(Printer.quote(q8))
      val cq8 = Translator.translate(q8)
      println(Printer.quote(cq8)) 
      println("")

      val x3 = TupleVarDef("x3", TupleType())
      val q7 = ForeachUnion(x3, Singleton(Tuple()), Singleton(Tuple()))
      println(Printer.quote(q7))
      val cq7 = Translator.translate(q7)
      println(Printer.quote(cq7))
      println("")

      println("--------------------------------------")

      val y = TupleVarDef("y", itemTp)
      val q2 = ForeachUnion(x, relationR,
        Singleton(Tuple(
          "grp" -> VarRef(x, "a"),
          "bag" -> ForeachUnion(y, relationR,
            IfThenElse(
              List(Cond(OpEq, VarRef(x, "a"), VarRef(y, "a"))),
              Singleton(Tuple("q" -> VarRef(y, "b")))
            ))
        )))

      println(Printer.quote(q2))
      println(Evaluator.eval(q2))

      println(Printer.quote(Shredder.shred(q2)))

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

      val relationR = Relation("R", PhysicalBag(itemTp,
        Tuple(
          "h" -> Const("42", IntType),
          "j" -> PhysicalBag(nestedItemTp,
            Tuple(
              "m" -> Const("Milos", StringType),
              "n" -> Const("123", IntType),
              "k" -> PhysicalBag(nested2ItemTp,
                Tuple("n" -> Const("123", IntType)),
                Tuple("n" -> Const("456", IntType)),
                Tuple("n" -> Const("789", IntType)),
                Tuple("n" -> Const("123", IntType))
              )
            ),
            Tuple(
              "m" -> Const("Michael", StringType),
              "n" -> Const("7", IntType),
              "k" -> PhysicalBag(nested2ItemTp,
                Tuple("n" -> Const("2", IntType)),
                Tuple("n" -> Const("9", IntType)),
                Tuple("n" -> Const("1", IntType))
              )
            ),
            Tuple(
              "m" -> Const("Jaclyn", StringType),
              "n" -> Const("12", IntType),
              "k" -> PhysicalBag(nested2ItemTp,
                Tuple("n" -> Const("14", IntType)),
                Tuple("n" -> Const("12", IntType))
              )
            )
          )
        ),
        Tuple(
          "h" -> Const("69", IntType),
          "j" -> PhysicalBag(nestedItemTp,
            Tuple(
              "m" -> Const("Thomas", StringType),
              "n" -> Const("987", IntType),
              "k" -> PhysicalBag(nested2ItemTp,
                Tuple("n" -> Const("987", IntType)),
                Tuple("n" -> Const("654", IntType)),
                Tuple("n" -> Const("987", IntType)),
                Tuple("n" -> Const("654", IntType)),
                Tuple("n" -> Const("987", IntType)),
                Tuple("n" -> Const("987", IntType))
              )
            )
          )
        )
      ))

      val x = TupleVarDef("x", itemTp)
      val w = TupleVarDef("w", nestedItemTp)

      val q1 = ForeachUnion(x, relationR,
        Singleton(Tuple(
          "o5" -> VarRef(x, "h"),
          "o6" ->
            ForeachUnion(w, VarRef(x, "j").asInstanceOf[BagExpr],
              Singleton(Tuple(
                "o7" -> VarRef(w, "m"),
                "o8" -> Mult(
                  Tuple("n" -> VarRef(w, "n")),
                  VarRef(w, "k").asInstanceOf[BagExpr]
                )
              ))
            )
        )))

      println(Printer.quote(q1))
      println(Evaluator.eval(q1))

//      println(Printer.quote(Shredder.shred(q1)))

    }
  }

  Example1.run()
  Example2.run()
}
