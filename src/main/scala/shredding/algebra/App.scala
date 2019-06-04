package shredding.algebra

import shredding.core._
import shredding.queries.simple.{FlatTests, NestedTests}

object FlatTest{
  val compiler = new BaseCompiler{}
  import compiler._
  val cdata = input(List(record(Map("a" -> constant(42), "b" -> constant("Milos"))), 
                            record(Map("a" -> constant(49), "b" -> constant("Michael"))), 
                            record(Map("a" -> constant(34), "b" -> constant("Jaclyn"))), 
                            record(Map("a" -> constant(42), "b" -> constant("Thomas")))))

  val relationRType = BagType(TupleType("a" -> IntType, "b" -> StringType))
  val relationRValues = List(Map("a" -> 42, "b" -> "Milos"), Map("a" -> 49, "b" -> "Michael"),
                           Map("a" -> 34, "b" -> "Jaclyn"), Map("a" -> 42, "b" -> "Thomas"))
  val relationRValues2 = List(List(42, "Milos"), List(49, "Michael"),
                           List(34, "Jaclyn"), List(42, "Thomas"))

  val relationRType3 = RecordCType("a" -> IntType, "b" -> BagCType(RecordCType("c" -> IntType)))
  val relationRValues3 = List(Map("a" -> 42, "b" -> List(Map("c" -> 1), Map("c" -> 2), Map("c" -> 4))), 
                              Map("a" -> 49, "b" -> List(Map("c" -> 3), Map("c" -> 2))),
                              Map("a" -> 34, "b" -> List(Map("c" ->5))))

  val rF = Label.fresh()
  val rFType = rF.tp
  val rDc = (List((rF, relationRValues)), ())
  val rDType = BagDictCType(BagCType(TTupleType(List(rF.tp, relationRType))), EmptyDictCType)
  val rDu = relationRValues 


}

object NestedTest {
     val compiler = new BaseCompiler{} 
     import compiler._

     val cdata = input(List(record(Map("h" -> constant(42), "j" -> 
                  input(List(record(Map("m" -> constant("Milos"), "n" -> constant(123), "k" -> 
                    input(List(record(Map("n" -> constant(123))), 
                               record(Map("n" -> constant(456))), 
                               record(Map("n" -> constant(789))), 
                               record(Map("n" -> constant(123))))))),
                      record(Map("m" -> constant("Michael"), "n" -> constant(7), "k" -> 
                    input(List(record(Map("n" -> constant(2))), 
                               record(Map("n" -> constant(9))), 
                               record(Map("n" -> constant(1))))))),
                      record(Map("m" -> constant("Jaclyn"), "n" -> constant(7), "k" -> 
                    input(List(record(Map("n"-> constant(14))), 
                               record(Map("n" -> constant(12))))))))))),
                  record(Map("h" -> constant(69), "j" ->
                    input(List(record(Map("m" -> constant("Thomas"), "n" -> constant(987), "k" -> 
                      input(List(record(Map("n" -> constant(987))),
                                 record(Map("n" -> constant(654))),
                                 record(Map("n" -> constant(987))),
                                 record(Map("n" -> constant(654))),
                                 record(Map("n" -> constant(987))), 
                                 record(Map("n" -> constant(987))))))))))))) 

      val nested2ItemTp = TupleType(Map("n" -> IntType))

      val nestedItemTp = TupleType(Map(
        "m" -> StringType,
        "n" -> IntType,
        "k" -> BagType(nested2ItemTp)
      ))
      val itemTp = TupleType(Map(
        "h" -> IntType,
        "j" -> BagType(nestedItemTp)
      ))


      val inputRelationType = BagType(itemTp)
      val inputRelation = List(Map(
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

      val rF = Label.fresh()
      val rFType = rF.tp
      val rD1 = inputRelation.map{m => (m("h"), Label.fresh(), m("j").asInstanceOf[List[Map[String,Any]]]) }
      val rD2 = rD1.map{ case (h, l, j) => (Label.fresh(), j.map{ m => 
                  (m("m"), m("n"), Label.fresh(), m("k").asInstanceOf[List[_]])})}
      val rD2j1 = rD2.map{ case (l, j) => (l, j.map{ case (m, n, l2, k) => Map("m" -> m, "n" -> n, "k" -> l2) }) }  
      val rD2j2k1 = rD2.flatMap{ case (l, j) => j.map{ case (m, n, l2, k) => (l2, k) } }
      val rDflat = rD1.map{ case (h,l,j) => Map("h" -> h, "j" -> l)}

      
      val tupleDict0 = scala.collection.mutable.Map[String, (Any, Any)]()
      val tupleDict = scala.collection.mutable.Map[String, (Any, Any)]()
      tupleDict0("k") = (rD2j2k1, ())
      tupleDict("j") = (rD2j1, tupleDict0)
      val tdictt = TupleDictCType(Map("h" -> EmptyDictCType, "j" -> 
        BagDictCType(BagCType(TTupleType(List(rF.tp, 
          BagCType(RecordCType("m" -> StringType, "n" -> IntType, "j" -> rF.tp))))), 
            TupleDictCType(Map("m" -> EmptyDictCType, "n" -> EmptyDictCType, "k" -> 
              BagDictCType(BagCType(TTupleType(List(rF.tp, 
                BagCType(RecordCType("n" -> IntType))))), EmptyDictCType))))))
      val rDc = (List((rF, rDflat)), tupleDict)
      val rDType = BagDictCType(BagCType(TTupleType(List(rF.tp, BagCType(RecordCType("h" -> IntType, "j" -> rF.tp))))), tdictt)
      // not sure about this yet 
      val rDu = (rDflat, tupleDict)
}

object App {

  def runShred(){
    
    val shredder = new ShredPipeline{};
    val str = Printer
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    
    val exp1 = shredder.shredPipeline(FlatTests.query1.asInstanceOf[shredder.Expr])
    val exp2 = shredder.shredPipeline(NestedTests.query1.asInstanceOf[shredder.Expr])
    val exp3 = shredder.shredPipeline(NestedTests.query3.asInstanceOf[shredder.Expr])
    val exp4 = shredder.shredPipeline(NestedTests.query3.asInstanceOf[shredder.Expr]) 

    val beval = new BaseScalaInterp{}
    val eval = new Finalizer(beval)

    println("\nTranslated:")
    println(str.quote(exp1))
    println("\nNormalized:")
    val nexp1 = norm.finalize(exp1).asInstanceOf[CExpr]
    println(str.quote(nexp1))
    beval.ctx("RF") = FlatTest.rF
    beval.ctx("RD") = FlatTest.rDc
    println("\n\nEvaluated:\n")
    eval.finalize(nexp1).asInstanceOf[List[_]].foreach(println(_))

    val nsgen = new ScalaGenerator{}
    nsgen.ctx("RF") = FlatTest.rFType
    nsgen.ctx("RD") = FlatTest.rDType
    val sgen = new Finalizer(nsgen)

    // exp0
    println("")
    val ccode = s"""
      | import shredding.algebra.FlatTest._
      | val RF = rF
      | val RD = rDc
      ${sgen.finalize(nexp1)}""".stripMargin
    println(ccode)

    println("")
    val anfBase = new BaseANF {}
    val anfed = new Finalizer(anfBase).finalize(nexp1)
    val anfExp = anfBase.anf(anfed.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp))
    println(ScalaNamedGenerator.generate(anfExp))
    println(ScalaNamedGenerator.generateHeader())

    /**println("")
    beval.ctx.clear
    beval.ctx("RF") = NestedTest.rF
    beval.ctx("RD") = NestedTest.rDc

    println("\nTranslated:")
    println(str.quote(exp2))
    println("\nNormalized:")
    val nexp2 = norm.finalize(exp2).asInstanceOf[CExpr]
    println(str.quote(nexp2))
    println("\nEvaluated:\n")
    eval.finalize(nexp2)
 
    nsgen.ctx.clear
    nsgen.ctx("RF") = NestedTest.rFType
    nsgen.ctx("RD") = NestedTest.rDType
 
    // exp1
    println("")
    val ccode2 = s"""
      | import shredding.algebra.NestedTest._
      | var ctx = scala.collection.mutable.Map[String,Any]()
      | val RF = rF
      | val RD = rDc
      ${sgen.finalize(nexp2)}""".stripMargin
    println(ccode2)**/
 
  }

  def runBase(){
    val translator = new NRCTranslator{}

    val exp0 = translator.translate(NestedTests.query2.asInstanceOf[translator.Expr])
    val exp1 = translator.translate(NestedTests.query2a.asInstanceOf[translator.Expr])
    val exp2 = translator.translate(NestedTests.query1.asInstanceOf[translator.Expr])

    val str = Printer
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    val beval = new BaseScalaInterp{}
    beval.ctx("R") = FlatTest.relationRValues3
    val eval = new Finalizer(beval)
    val nsgen = new ScalaGenerator{}
    nsgen.ctx("R") = FlatTest.relationRType3
    val sgen = new Finalizer(nsgen)
 
    // exp0
    val normalized0 = norm.finalize(exp0).asInstanceOf[CExpr]
    println(str.quote(normalized0))

    println("")
    val ccode = s"""
      | import shredding.algebra.FlatTest._
      | val R = relationRValues3
      | ${sgen.finalize(normalized0)}""".stripMargin

    println(ccode)

    println("")
    val anfBase = new BaseANF {}
    val anfed = new Finalizer(anfBase).finalize(normalized0)
    val anfExp = anfBase.anf(anfed.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp))
    println(ScalaNamedGenerator.generate(anfExp))
    println(ScalaNamedGenerator.generateHeader())
    
  }

  def main(args: Array[String]){
    runShred()
    runBase()
  }


}
