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
    val employees = InputR("Employees", 
                      List(Map("dno" -> 1, "ename" -> "one"),Map("dno" -> 2, "ename"-> "two"),
                            Map("dno" -> 3, "ename" -> "three"),Map("dno" -> 4, "ename" -> "four")), BagType(etype))
    val departments = InputR("Departments", 
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

    val xdef = VarDef("x", itemTp)
    val q1 = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> Project(TupleVarRef(xdef), "b"))))
    val q1shred = q1.shred
    val q1lin = Linearize(q1shred)
    q1lin.foreach(e => println(e.quote))
    val cqs = q1lin.map(e => Translator.translate(e))
    cqs.foreach(e => println(calc.quote(e.asInstanceOf[calc.CompCalc])))
    val nqs = cqs.map(e => e match { case NamedCBag(n,b) => NamedTerm(n, Unnester.unnest(b.normalize)) })
    println(calc.quote(nqs.head.asInstanceOf[calc.AlgOp]))
    println(calc.quote(nqs.tail.head.asInstanceOf[calc.AlgOp]))
  
    nqs.foreach(e => {
      println("evaluation!!!!")
      println("")
      println(calc.quote(e.asInstanceOf[calc.AlgOp]))
      println("")
      val nrdd2 = sparke.evaluate(e)
      //println(nrdd2.toDebugString)
      nrdd2.take(100).foreach(println(_))
    })

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
    val q2shred = q2.shred
    val q2lin = Linearize(q2shred)
    q2lin.foreach(e => println(e.quote))
    val cqs2 = q2lin.map(e => Translator.translate(e))
    cqs2.foreach(e => println(calc.quote(e.asInstanceOf[calc.CompCalc])))
    val nqs2 = cqs2.map(e => e match { case NamedCBag(n,b) => NamedTerm(n, Unnester.unnest(b.normalize)) })
    sparke.reset 
    nqs2.foreach(e => {
      println("evaluation!!!!")
      println("")
      println(calc.quote(e.asInstanceOf[calc.AlgOp]))
      println("")
      val nrdd2 = sparke.evaluate(e)
      println(sparke.ctx)
      //println(nrdd2.toDebugString)
      println("")
      nrdd2.take(100).foreach(println(_))
    })


  }

}
