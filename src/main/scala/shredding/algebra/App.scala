package shredding.algebra

import java.io._
import shredding.core._
import shredding.queries.simple.{FlatTests, NestedTests}
import shredding.queries.tpch.{TPCHQueries, TPCHSchema, TPCHLoader}

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
    
    val translator = new NRCTranslator{}
    val shredder = new ShredPipeline{}
    val str = Printer
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    
    val exp1 = shredder.shredPipeline(FlatTests.query1.asInstanceOf[shredder.Expr])
    val exp2 = shredder.shredPipeline(NestedTests.query2.asInstanceOf[shredder.Expr])
    val exp3 = shredder.shredPipeline(NestedTests.query3.asInstanceOf[shredder.Expr])
    val exp4 = shredder.shredPipeline(NestedTests.query4.asInstanceOf[shredder.Expr]) 
    val tpch1 = shredder.shredPipeline(TPCHQueries.query1.asInstanceOf[shredder.Expr])

    val beval = new BaseScalaInterp{}
    val eval = new Finalizer(beval)
    val anfBase = new BaseANF {}

    /**println("\nTranslated:")
    println(str.quote(exp1))
    println("\nNormalized:")
    val nexp1 = norm.finalize(exp1).asInstanceOf[CExpr]
    println(str.quote(nexp1))
    beval.ctx("RF") = FlatTest.rF
    beval.ctx("RD") = FlatTest.rDc
    println("\n\nEvaluated:\n")
    eval.finalize(nexp1).asInstanceOf[List[_]].foreach(println(_))

    println("")
    val anfed = new Finalizer(anfBase).finalize(nexp1)
    val anfExp = anfBase.anf(anfed.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp))
    println(ScalaNamedGenerator.generate(anfExp))
    println(ScalaNamedGenerator.generateHeader())

    println("")
    beval.ctx.clear
    beval.ctx("RF") = NestedTest.rF
    beval.ctx("RD") = NestedTest.rDc**/

    println("\nTranslated:")
    println(str.quote(tpch1))
    
    println("\nNormalized:")
    val nquery1 = norm.finalize(tpch1).asInstanceOf[CExpr]
    println("/** "+str.quote(nquery1)+" **/" )
    //println("\nEvaluated:\n")
    //eval.finalize(nexp2)
 
    println("")
    val anfed1 = new Finalizer(anfBase).finalize(nquery1)
    val anfExp1 = anfBase.anf(anfed1.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp1))
    val inputtypes = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
    val sgen = new ScalaNamedGenerator(inputtypes)
    val ccode = sgen.generate(anfExp1)
    val header = sgen.generateHeader()

    val finalc = s"""
      |/** Generated code **/
      |${TPCHQueries.q1shreddata}
      |${header}
      |var start = System.currentTimeMillis()
      |${ccode}
      |var end = System.currentTimeMillis() - start""".stripMargin
 
    var out = "src/test/scala/shredding/queries/tpch/ShredQ1.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    printer.println(finalc)
    printer.close 

  }

  def runBase(){
    val translator = new NRCTranslator{}

    val exp0 = translator.translate(NestedTests.query2.asInstanceOf[translator.Expr])
    val exp1 = translator.translate(NestedTests.query2a.asInstanceOf[translator.Expr])
    val exp2 = translator.translate(NestedTests.query1.asInstanceOf[translator.Expr])
    val exp3 = translator.translate(FlatTests.query2.asInstanceOf[translator.Expr])
    val exp4 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val exp5 = translator.translate(TPCHQueries.query1.asInstanceOf[translator.Expr])

    val str = Printer
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    val beval = new BaseScalaInterp{}
    beval.ctx("R") = FlatTest.relationRValues3
    val eval = new Finalizer(beval)
    val anfBase = new BaseANF {}

    // exp0
    val normalized0 = norm.finalize(exp0).asInstanceOf[CExpr]
    //println(str.quote(normalized0))

    val normalized1 = norm.finalize(exp5).asInstanceOf[CExpr]
    println("/** "+str.quote(normalized1)+"**/")

    val normalized4 = norm.finalize(exp4).asInstanceOf[CExpr]
    //println(str.quote(normalized4))
    
    println("")
    val anfed = new Finalizer(anfBase).finalize(normalized1)
    val anfExp = anfBase.anf(anfed.asInstanceOf[anfBase.Rep])
    //println(str.quote(anfExp))
    val inputtypes = TPCHSchema.tpchInputs.map(f => translator.translate(f._1) -> f._2)
    val sgenn = new ScalaNamedGenerator(inputtypes)
    val ccode = sgenn.generate(anfExp)
    val header = sgenn.generateHeader()

    val finalc = s"""
      |/** Generated code **/
      |${TPCHQueries.query1data}
      |${header}
      |var start = System.currentTimeMillis()
      |${ccode}
      |var end = System.currentTimeMillis() - start""".stripMargin

    var out = "src/test/scala/shredding/queries/tpch/Q1.Scala"
    val printer = new PrintWriter(new FileOutputStream(new File(out), false))
    printer.println(finalc)
    printer.close

    /**val anfed1 = new Finalizer(anfBase).finalize(exp)
    val anfExp1 = anfBase.anf(anfed1.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp1))
    val sgenn1 = ScalaNamedGenerator
    println(sgenn1.generate(anfExp1))
    println(sgenn1.generateHeader())*/
    
  }

  def main(args: Array[String]){
    runShred()
    runBase()
  }


}
