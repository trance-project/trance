package shredding.calc

import shredding.core._
import shredding.nrc2._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object App extends AlgTranslator with Algebra with Calc with CalcImplicits with NRC with NRCTranslator with NRCTransforms with NRCImplicits with Dictionary with Linearization with CalcTranslator with ShreddingTransform with Serializable{

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dtype = TupleType("dno" -> IntType, "dname" -> StringType)
    val etype = TupleType("dno" -> IntType, "ename" -> StringType)
    val e = VarDef("e", etype)
    val d = VarDef("d", dtype)
    val employees = Relation("Employees", 
                      List(Map("dno" -> 1, "ename" -> "one"),Map("dno" -> 2, "ename"-> "two"),
                            Map("dno" -> 3, "ename" -> "three"),Map("dno" -> 4, "ename" -> "four")), BagType(etype))
    val departments = Relation("Departments", 
                        List(Map("dno" -> 1, "dname" -> "five"),Map("dno" -> 2, "dname" -> "six"),
                              Map("dno" -> 3, "dname" -> "seven"),Map("dno" -> 4, "dname" -> "eight")), BagType(dtype))
    val sparke = SparkEvaluator(spark.sparkContext)

    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = Relation("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
      ), BagType(itemTp))

    println("--------------------- Query 1 ---------------------")
    val xdef = VarDef("x", itemTp)
    val q1 = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> Project(TupleVarRef(xdef), "b"))))
    println(q1.quote)
    println("")
    val q1shred = q1.shred
    println(q1.eval)
    println("")
    val q1lin = Linearize(q1shred)
    println("Linearized set: ")
    q1lin.foreach(e => println(e.quote))
    val cqs = q1lin.map(e => Translator.translate(e))
    println("")
    println("Comprehension calculus: ")
    cqs.foreach(e => println(calc.quote(e.asInstanceOf[calc.CompCalc])))
    val nqs = cqs.map(e => e match { case NamedCBag(n,b) => NamedTerm(n, Unnester.unnest(b.normalize)) })  
    nqs.foreach(e => {
      println("")
      println("Unnested to Algebra: ")
      println(calc.quote(e.asInstanceOf[calc.AlgOp]))
      println("")
      println("Evaluation: ")
      val nrdd2 = sparke.evaluate(e)
      nrdd2.take(100).foreach(println(_))
    })

    println("")
    println("--------------------- Query 2 ---------------------")
    val ydef = VarDef("y", itemTp)
    val yref = TupleVarRef(ydef)
    val q2 = ForeachUnion(xdef, relationR,
              IfThenElse(Cond(OpGt, Project(TupleVarRef(xdef), "a"), Const(35, IntType)),
              Singleton(Tuple(
                "grp" -> Project(TupleVarRef(xdef), "a"),
                "bag" -> ForeachUnion(ydef, relationR,
                  IfThenElse(
                    Cond(OpEq, Project(TupleVarRef(xdef), "a"), Project(TupleVarRef(ydef), "a")),
                    Singleton(Tuple("q" -> Project(TupleVarRef(ydef), "b")))
                  ))
              ))))
    println(q2.quote)
    println("")
    val q2shred = q2.shred
    //println(q2.eval)
    //println("")
    val q2lin = Linearize(q2shred)
    println("Linearized set: ")
    q2lin.foreach(e => println(e.quote))
    println("")
    println("Comprehension calculus: ")
    val cqs2 = q2lin.map(e => Translator.translate(e))
    cqs2.foreach(e => println(calc.quote(e.asInstanceOf[calc.CompCalc])))
    val nqs2 = cqs2.map(e => e match { case NamedCBag(n,b) => NamedTerm(n, Unnester.unnest(b.normalize)) })
    sparke.reset 
    nqs2.foreach(e => {
      println("")
      println("Unnested to Algebra: ")
      println(calc.quote(e.asInstanceOf[calc.AlgOp]))
      println("")
      val nrdd2 = sparke.evaluate(e)
      nrdd2.take(100).foreach(println(_))
    })

    println("")
    println("----------------------- Query 3 -------------------")
    val q3 = ForeachUnion(d, departments,
              Singleton(Tuple("D" -> Project(TupleVarRef(d), "dno"), "E" -> ForeachUnion(e, employees,
                IfThenElse(Cond(OpEq, Project(TupleVarRef(e), "dno"), Project(TupleVarRef(d), "dno")),
                  Singleton(TupleVarRef(e)))))))
    println(q3.quote)
    println("")
    val sq3 = q3.shred
    println(q3.eval)
    println("")
    val sq3lin = Linearize(sq3)
    println("Linearized set: ")
    sq3lin.foreach(e => println(e.quote))
    println("")
    println("Comprehension calculus: ")
    val cqs3 = sq3lin.map(e => Translator.translate(e))
    cqs3.foreach(e => println(calc.quote(e.asInstanceOf[calc.CompCalc])))
    val nqs3 = cqs3.map(e => e match { case NamedCBag(n,b) => NamedTerm(n, Unnester.unnest(b.normalize)) })
    sparke.reset
    nqs3.foreach(e => {
      println("")
      println("Unnested to Algebra: ")
      println(calc.quote(e.asInstanceOf[calc.AlgOp]))
      println("")
      val nrdd3 = sparke.evaluate(e)
      nrdd3.take(100).foreach(println(_))
    })

    println("----------------------- Query 4 -------------------")
    val q4 = ForeachUnion(d, departments,
              ForeachUnion(e, employees,
                Singleton(Tuple("D" -> Project(TupleVarRef(d), "dno"), "E" -> 
                  IfThenElse(Cond(OpEq, Project(TupleVarRef(e), "dno"), Project(TupleVarRef(d), "dno")),
                    Singleton(TupleVarRef(e)))))))
    println(q4.quote)
    println("")
    val sq4 = q4.shred
    println(q4.eval)
    println("")
    val sq4lin = Linearize(sq4)
    println("Linearized set: ")
    sq4lin.foreach(e => println(e.quote))
    println("")
    println("Comprehension calculus: ")
    val cqs4 = sq4lin.map(e => Translator.translate(e))
    cqs4.foreach(e => println(calc.quote(e.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    val nqs4 = cqs4.map(e => e match { case NamedCBag(n,b) => NamedTerm(n, Unnester.unnest(b.normalize)) })
    sparke.reset
    nqs4.foreach(e => {
      println("")
      println("Unnested to Algebra: ")
      println(calc.quote(e.asInstanceOf[calc.AlgOp]))
      println("")
      val nrdd4 = sparke.evaluate(e)
      nrdd4.take(100).foreach(println(_))
    })
    /**println("")
    println("Normalized: ")
    val ncqs4 = cqs4.map(e => e.asInstanceOf[CompCalc].normalize)
    ncqs4.foreach(e => println(calc.quote(e.asInstanceOf[calc.CompCalc])))
    val nnqs4 = ncqs4.map(e => e match { case NamedCBag(n,b) => NamedTerm(n, Unnester.unnest(b)) })
    sparke.reset
    nnqs4.foreach(e => {
      println("")
      println("Unnested to Algebra (w/ normalization): ")
      println(calc.quote(e.asInstanceOf[calc.AlgOp]))
      println("")
      val nrdd4n = sparke.evaluate(e)
      nrdd4n.take(100).foreach(println(_))
    })**/

  }

}
