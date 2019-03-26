package shredding.calc

import org.scalatest.FunSuite
import shredding.core._
import shredding.nrc2._

class UnnesterShredTest extends FunSuite with CalcTranslator with CalcImplicits with NRCTranslator with NRC with NRCTransforms with ShreddingTransform with Linearization with NRCImplicits with Dictionary{

   def print(e: CompCalc) = println(calc.quote(e.asInstanceOf[calc.CompCalc]))    
   def printa(e: AlgOp) = println(calc.quote(e.asInstanceOf[calc.AlgOp]))
   
   val itemTp = TupleType("a" -> IntType, "b" -> StringType)
   val relationR = Relation("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
      ), BagType(itemTp))
  
   /**
     * Simple shred query unit test
     */
   test("UnnesterShred.unnest.ShredQ1"){

      val xdef = VarDef("x", itemTp)
      val q1 = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> Project(TupleVarRef(xdef), "b"))))
      val q1shred = q1.shred
      // Mflat8 = For l7 in EmptyCtx0 Union sng( k = l7.lbl, v = For x0 in R union sng( w = x0.b ) )
      val q1lin = Linearize(q1shred)
      // Mflat8 = { ( k = l7.lbl, v = { ( w = x0.b ) | x0 <- R } ) | l7 <- EmptyCtx0 }
      val cqs = q1lin.map(e => Translator.translate(e))
      val nqs = cqs.map(e => e match { case NamedCBag(n,b) => NamedTerm(n, Unnester.unnest(b.normalize)) })
      //nqs.foreach(e => printa(e))
   }

   test("UnnesterShred.unnest.ShredQ2"){
      
      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)
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

      //println("Q2: " + q2.quote)
      val q2shred = q2.shred
      //println("Linearized Q2")
      val q2lin = Linearize(q2shred)
      //q2lin.foreach(e => println(e.quote))

      //println("Unnested")
      val cqs = q2lin.map(e => Translator.translate(e))
      //cqs.foreach(e => print(e))

      val nqs = cqs.map(e => e match { case NamedCBag(n, b) => NamedTerm(n, Unnester.unnest(b.normalize)) })
      //nqs.foreach(e => printa(e))
   }

   test("UnnesterShred.unnest.Query3"){
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
                // this doesn't work
                //"o8" -> Project(wref, "k").asInstanceOf[BagExpr]
                )
              ))
            )
        ))

      //println("Q1: " + q1.quote)

      val q1shred = q1.shred
      //println("Shredded Q1: " + q1shred.quote)

      //println("Linearized Q1")
      val q1lin = Linearize(q1shred)
      //q1lin.foreach(e => println(e.quote))
   
      val cqs = q1lin.map(e => Translator.translate(e))
      //cqs.foreach(e => print(e))

      val nqs = cqs.map(e => e match { case NamedCBag(n, b) => NamedTerm(n, Unnester.unnest(b.normalize))})
      //nqs.foreach(e => printa(e)) 
   }
}
