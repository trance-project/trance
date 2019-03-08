package shredding.nrc

object TestApp extends App {

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val x = VarDef("x", itemTp)
      val x2 = VarDef("y2", itemTp)
      val relationR = Relation("R", PhysicalBag(itemTp,
        Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType)),
        Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType)),
        Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType)),
        Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType))
      ))
      val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> VarRef(x, "b"))))

      println(Printer.quote(q1))
      val cq1 = Translator.translate(q1)
      println(cq1)
      println(Printer.quote(cq1))
      println(Evaluator.eval(q1))
      //val ncq1 = Unnester.unnest(cq1)
      //println(Printer.quote(ncq1))

      println(cq1)

      val q10 = ForeachUnion(x, relationR, 
                  ForeachUnion(x2, relationR, 
                    IfThenElse(List(Cond(OpEq, VarRef(x, "b"), VarRef(x2, "b"))), Singleton(Tuple("w1" -> VarRef(x, "a"))))))
      println(Printer.quote(q10))
      val cq10 = Translator.translate(q10)
      println(cq10)
      println(Printer.quote(cq10))
      //val ncq10 = Unnester.unnest(cq10)
      //println(Printer.quote(ncq10)) 
      
      println(" -------------------------------------")
      println("working on transformation")
      
      val x4 = VarDef("x", IntType)
      val q11 = ForeachUnion(x, Singleton(Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType))), 
                Singleton(Tuple("w" -> VarRef(x, "a"))))
      println(Printer.quote(q11))
      println(q11)
      val cq11 = Translator.translate(q11)
      println(cq11)
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
      val ncq6 = Unnester.unnest(cq6)
      println(Printer.quote(ncq6))
      println("")

      val q8 = ForeachUnion(x, relationR, IfThenElse(List(Cond(OpGt, VarRef(x, "a"), Const("35", IntType)),
                                                     Cond(OpGt, Const("45", IntType), VarRef(x, "a"))), 
                            Singleton(Tuple("w1" -> VarRef(x, "b"))), None))
      println(Printer.quote(q8))
      val cq8 = Translator.translate(q8)
      println(Printer.quote(cq8)) 
      println("")

      val x3 = VarDef("x", TupleType())
      val q7 = ForeachUnion(x3, Singleton(Tuple()), Singleton(Tuple()))
      println(Printer.quote(q7))
      val cq7 = Translator.translate(q7)
      println(Printer.quote(cq7))
      println("")

      println("--------------------------------------")

      val y = VarDef("y", itemTp)
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

      val x = VarDef("x", itemTp)
      val w = VarDef("w", nestedItemTp)

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
      //println(Evaluator.eval(q1))

//      println(Printer.quote(Shredder.shred(q1)))

    }
  }

  /**
    * Example 3 are examples that take source NRC, translate into normalized comprehension calculus,
    * and then unnest the expression into algebra operators.
    */
  
  object Example3 {

    def run(): Unit = {

      /** input relation **/ 
      val itemTp2 = TupleType("c" -> IntType)
      val itemTp = TupleType("a" -> IntType, "b" -> StringType, "c" -> BagType(itemTp2))
      val x = VarDef("x", itemTp)
      println(x)
      val y = VarDef("y", itemTp2)
      val x2 = VarDef("y", itemTp)
      val relationR = Relation("R", PhysicalBag(itemTp,
        Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType), "c" -> PhysicalBag(itemTp2,
          Tuple("c" -> Const("42", IntType)), Tuple("c" -> Const("42", IntType)), Tuple("c" -> Const("30", IntType)))),
        Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType), "c" -> PhysicalBag(itemTp2,
          Tuple("c" -> Const("100", IntType)), Tuple("c" -> Const("69", IntType)), Tuple("c" -> Const("42", IntType)))),
        Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType), "c" -> PhysicalBag(itemTp2,
          Tuple("c" -> Const("34", IntType)), Tuple("c" -> Const("100", IntType)), Tuple("c" -> Const("12", IntType)))),
        Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType), "c" -> PhysicalBag(itemTp2,
          Tuple("c" -> Const("50", IntType)), Tuple("c" -> Const("32", IntType)), Tuple("c" -> Const("42", IntType))))
      ))
      
      /** 
        * Example 1
        *
        * NRC Input:
        * For x3 in R Union
        *   sng(( w := x3.b ))
        *
        * Translated into comprehension calc:
        * { ( w := x3.b ) |  x3 <- R  }
        *
        * Unnested into algebra:
        * Reduce[ U / lambda(x3).( w := x3.b ), lambda(x3).true]
        *   Select[lambda(x3).true](R)
        */
      println("")
      val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> VarRef(x, "b"))))
      println(Printer.quote(q1))
       
      val cq1 = Translator.translate(q1)
      println(Printer.quote(cq1))

      val ncq1 = Unnester.unnest(cq1)
      println(Printer.quote(ncq1))
      println("")

      /** 
        * Example 2
        *
        * NRC Input:
        * For x3 in R Union
        *   If ((x3.a > 35))
        *   Then sng(( w1 := x3.b ))
        *
        * Translated into comprehension calc:
        * { ( w1 := x3.b ) |  x3 <- R ,  x3.a > 35  }
        *
        * Unnested into algebra:
        * Reduce[ U / lambda(x3).( w1 := x3.b ), lambda(x3).true]
        *   Select[lambda(x3). x3.a > 35 ](R)
        */

      val q2 = ForeachUnion(x, relationR, IfThenElse(List(Cond(OpGt, VarRef(x, "a"), Const("35", IntType))), 
                Singleton(Tuple("w1" -> VarRef(x, "b"))), None)) 
      println(Printer.quote(q2))
      val cq2 = Translator.translate(q2)
      println(Printer.quote(cq2))
      val ncq2 = Unnester.unnest(cq2)
      println(Printer.quote(ncq2))
      println("")

      /** 
        * Example 3
        *
        * NRC Input:
        * For x3 in R Union
        *   For y4 in x3.c Union
        *     sng(( w1 := x3.a, w2 := y4.c ))
        *
        * Translated into comprehension calc:
        * { ( w1 := x3.a, w2 := y4.c ) |  x3 <- R ,  y4 <- x3.c  }
        *
        * Unnested into algebra:
        * Reduce[ U / lambda(y4,x3).( w1 := x3.a, w2 := y4.c ), lambda(y4,x3).true]
        *   Unnest[lambda(y4,x3).x3.c, lambda(y4,x3).true]
        *     Select[lambda(x3).true](R)
        */

      val q3 = ForeachUnion(x, relationR, 
                ForeachUnion(y, VarRef(x, "c").asInstanceOf[BagExpr],
                  Singleton(Tuple("w1" -> VarRef(x, "a"), "w2" -> VarRef(y, "c")))))
      println(Printer.quote(q3))
      val cq3 = Translator.translate(q3)
      println(Printer.quote(cq3))
      val ncq3 = Unnester.unnest(cq3)
      println(Printer.quote(ncq3))
      println("")

      /** 
        * Example 4
        *
        * NRC Input:
        * For x3 in R Union
        *   For y5 in R Union
        *     sng(( w1 := x3.a, w2 := y5.b ))
        *
        * Translated into comprehension calc:
        * { ( w1 := x3.a, w2 := y5.b ) |  x3 <- R ,  y5 <- R  }
        *
        * Unnested into algebra:
        * Reduce[ U / lambda(y5,x3).( w1 := x3.a, w2 := y5.b ), lambda(y5,x3).true]
        *   Join[lambda(y5,x3).true]
        *     Select[lambda(y5).true](R)
        *     Select[lambda(x3).true](R)
        */


      val q4 = ForeachUnion(x, relationR, 
                ForeachUnion(x2, relationR,
                  Singleton(Tuple("w1" -> VarRef(x, "a"), "w2" -> VarRef(x2, "b")))))
      println(Printer.quote(q4))
      val cq4 = Translator.translate(q4)
      println(Printer.quote(cq4))
      val ncq4 = Unnester.unnest(cq4)
      println(Printer.quote(ncq4))
      println("")

      /** 
        * Example 5
        *
        * NRC Input:
        * For x3 in R Union
        *   sng(( w1 := x3.a, w2 := For y4 in x3.c Union
        *     sng(( a1 := x3.b, a2 := y4.c )) ))
        *
        * Translated into comprehension calc:
        * { ( w1 := x3.a, w2 := { ( a1 := x3.b, a2 := y4.c ) |  y4 <- x3.c  } ) |  x3 <- R  }
        *
        * Unnested into algebra:
        * Reduce[ U / lambda(v6,x3).( w1 := x3.a, w2 := v6 ), lambda(v6,x3).true]
        *   Nest[ U / lambda(y4,x3).( a1 := x3.b, a2 := y4.c ) / lambda(y4,x3).x3, lambda(y4,x3).true / lambda(y4,x3).y4]
        *     OuterUnnest[lambda(y4,x3).x3.c, lambda(y4,x3).true]
        *       Select[lambda(x3).true](R)
        */

      val q5 = ForeachUnion(x, relationR, 
                Singleton(Tuple("w1" -> VarRef(x, "a"), "w2" -> ForeachUnion(y, VarRef(x, "c").asInstanceOf[BagExpr],
                  Singleton(Tuple("a1" -> VarRef(x, "b"), "a2" -> VarRef(y, "c")))))))
      println(Printer.quote(q5))
      val cq5 = Translator.translate(q5)
      println(Printer.quote(cq5))
      val ncq5 = Unnester.unnest(cq5)
      println(Printer.quote(ncq5))
      println("")
      
      val x3 = VarDef("x", TupleType("a" -> StringType))
      val q6 = Let(x3, Tuple("a" -> Const("one", StringType)), VarRef(x3, "a"))
      println(q6)
      println(Printer.quote(q6))
      val cq6 = Translator.translate(q6)
      println(Printer.quote(cq6))

    }
  }

  Example1.run()
  //Example2.run()
  Example3.run()
}
