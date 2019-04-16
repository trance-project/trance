package shredding.calc

import shredding.Utils.Symbol
import shredding.core._
import shredding.nrc._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object App extends 
  AlgTranslator with Algebra 
  with Calc with CalcImplicits with NRCTranslator 
  with Shredding with ShreddedNRC with ShreddedPrinter with ShreddedEvaluator with Linearization
  with CalcTranslator with Serializable{
  
  def run1(){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)

    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
    ), BagType(itemTp))

    val xdef = VarDef(Symbol.fresh("x"), itemTp)
    println("--------------------- Query 1 ---------------------")
    val q1 = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> Project(TupleVarRef(xdef), "b"))))
    println(quote(q1))
    println("")
    val q1shred = shred(q1)
    println(eval(q1))
    println("")
    val q1lin = linearize(q1shred)
    println("Linearized set: ")
    quote(q1lin)
    val cqs = Translator.translate(q1lin)
    println("")
    println("Comprehension calculus: ")
    //cqs.foreach(e => println(calc.quote(e.asInstanceOf[calc.CompCalc])))
    cqs.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    })
    val ncqs = Unnester.unnest(cqs).asInstanceOf[PlanSet]
    sparke.execute(ncqs)
  }

  def runR1(){
    println("-------- Recursive Query: 1 Iteration ------------")
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)

    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
    ), BagType(itemTp))

    val xdef = VarDef(Symbol.fresh("x"), itemTp)
    val rq = ForeachUnion(xdef, relationR, Singleton(Tuple("w1" -> Singleton(TupleVarRef(xdef)))))
    println(quote(rq))
    println("")
    val rqshred = shred(rq)
    println(eval(rq))
    println("")
    val rqlin = linearize(rqshred)
    println("Linearized set: ")
    println(quote(rqlin))
    val crqs1 = Translator.translate(rqlin)
    println("")
    println("Comprehension calculus: ")
    crqs1.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    println("\nNormalized: ")
    val nrqs1 = Unnester.unnest(crqs1).asInstanceOf[PlanSet]
    sparke.execute(nrqs1)

    println("\nNot Normalized: ")
    Unnester.normalize = false
    val nnrqs1 = Unnester.unnest(crqs1).asInstanceOf[PlanSet]
    sparke.execute(nnrqs1)
  }
  
  def runR2(){
    println("-------- Recursive Query: Iteration 2 ------------")
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)

    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
    ), BagType(itemTp))

    val x0def = VarDef(Symbol.fresh("x"), itemTp)
    val x1def = VarDef(Symbol.fresh("x"), itemTp)
    val x1adef = VarDef(Symbol.fresh("x"), itemTp)
    val x11def = VarDef(Symbol.fresh("x"), TupleType("w4" -> StringType))
    val x2def = VarDef(Symbol.fresh("x"), TupleType("w1" -> BagType(TupleType("w2" -> IntType, "w3" -> BagType(itemTp)))))
    val x2adef = VarDef(Symbol.fresh("x"), TupleType("w0" -> StringType, "w1" -> 
                    BagType(TupleType("w2" -> IntType, "w3" -> BagType(TupleType("w4" -> StringType))))))//itemTp)))))
    val x3adef = VarDef(Symbol.fresh("x"), TupleType("w2" -> IntType, "w3" -> BagType(TupleType("w4" -> StringType))))
    val x3def = VarDef(Symbol.fresh("x"), TupleType("w2" -> IntType, "w3" -> BagType(itemTp)))
    val rq = ForeachUnion(x0def, relationR, 
                Singleton(Tuple("w1" -> ForeachUnion(x1def, relationR, 
                  Singleton(Tuple("w2" -> Project(TupleVarRef(x0def), "a"), "w3" -> Singleton(TupleVarRef(x1def))))))))

    val rqa = ForeachUnion(x0def, relationR, 
                Singleton(Tuple("w0" -> Project(TupleVarRef(x0def), "b"), "w1" -> ForeachUnion(x1def, relationR, 
                  Singleton(Tuple("w2" -> Project(TupleVarRef(x0def), "a"), "w3" -> ForeachUnion(x1adef, relationR, 
                    Singleton(Tuple("w4" -> Project(TupleVarRef(x1adef), "b"))))))))))

    // (3)
    val rq1 = ForeachUnion(x2adef, rqa, ForeachUnion(x3adef, Project(TupleVarRef(x2adef), "w1").asInstanceOf[BagExpr],
                /**Singleton(Tuple("w4" -> Project(TupleVarRef(x3adef), "w2"), "w5" ->**/ 
                  ForeachUnion(x11def, Project(TupleVarRef(x3adef), "w3").asInstanceOf[BagExpr], 
                    Singleton(Tuple("w6" -> Project(TupleVarRef(x11def), "w4"))))))//))    
    //(1)
    //val rq1 = ForeachUnion(x2def, rq, ForeachUnion(x3def, Project(TupleVarRef(x2def), "w1").asInstanceOf[BagExpr],
    //            Singleton(Tuple("w4" -> Project(TupleVarRef(x3def), "w2")))))
    
    //(2)
    //val rq1 = ForeachUnion(x2def, rq, Singleton(Tuple("w4" -> Project(TupleVarRef(x2def), "w1")))) 
    println(quote(rq1))
    println("")
    val rq1shred = shred(rq1)
    println(eval(rq1))
    println("")
    val rq1lin = linearize(rq1shred)
    println("Linearized set: ")
    println(quote(rq1lin))
    val crqs = Translator.translate(rq1lin)
    println("")
    println("Comprehension calculus: ")
    crqs.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))})
    crqs.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    println("\nNormalized: ")
    val nrqs = Unnester.unnest(crqs).asInstanceOf[PlanSet]
    nrqs.plans.foreach(e => calc.quote(e.asInstanceOf[calc.AlgOp]))
    //sparke.execute(nrqs)

    /**println("\nNot Normalized: ")
    Unnester.normalize = false
    val nnrqs = Unnester.unnest(crqs).asInstanceOf[PlanSet]
    sparke.execute(nnrqs)**/
  }

  def runR3(){
    println("-------- Recursive Query: Iteration 3 ------------")
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)

    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael")/**,
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")**/
    ), BagType(itemTp))
    val x0def = VarDef(Symbol.fresh("x"), itemTp)
    val x1def = VarDef(Symbol.fresh("x"), itemTp)
    val rq1 = ForeachUnion(x0def, relationR, 
                ForeachUnion(x1def, relationR, 
                  Singleton(Tuple("w1" -> Singleton(TupleVarRef(x0def)), "w2" -> Singleton(TupleVarRef(x1def))))))

    val x2def = VarDef(Symbol.fresh("x"), itemTp)
    val x3def = VarDef(Symbol.fresh("x"), TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp)))
    val x4def = VarDef(Symbol.fresh("x"), BagType(TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp))))
    val rq2 = Let(x4def, rq1, ForeachUnion(x3def, BagVarRef(x4def), ForeachUnion(x2def, relationR,
                Singleton(Tuple("w1" -> Singleton(TupleVarRef(x3def)), "w2" -> Singleton(TupleVarRef(x2def)))))))

    println(quote(rq2))
    println("")
    val rq2shred = shred(rq2)
    println(eval(rq2))
    println("")
    val rq2lin = linearize(rq2shred)
    println("Linearized set: ")
    println(quote(rq2lin))
    val crqs2 = Translator.translate(rq2lin)
    println("")
    println("Comprehension calculus: ")
    println("\nNormalized: ")
    crqs2.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    })
    val nrqs2 = Unnester.unnest(crqs2).asInstanceOf[PlanSet]
    sparke.execute(nrqs2)

    println("\nNot Normalized: ")
    crqs2.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    Unnester.normalize = false
    val nnrqs2 = Unnester.unnest(crqs2).asInstanceOf[PlanSet]
    nnrqs2.plans.foreach(e => println(calc.quote(e.asInstanceOf[calc.AlgOp])))
    /**sparke.execute(nnrqs2)**/

  }

  def runR4(){
    println("-------- Recursive Query: Iteration 3 ------------")
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)

    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael")/**,
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")**/
    ), BagType(itemTp))
    val x0def = VarDef(Symbol.fresh("x"), itemTp)
    val x1def = VarDef(Symbol.fresh("x"), itemTp)
    val rq1 = ForeachUnion(x0def, relationR, 
                ForeachUnion(x1def, relationR, 
                  Singleton(Tuple("w1" -> Singleton(TupleVarRef(x0def)), "w2" -> Singleton(TupleVarRef(x1def))))))

    val x2def = VarDef(Symbol.fresh("x"), itemTp)
    val itemTp2 = TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp))
    val x3def = VarDef(Symbol.fresh("x"), itemTp2)
    val rq2 = ForeachUnion(x3def, rq1, ForeachUnion(x2def, relationR,
                Singleton(Tuple("w1" -> Singleton(TupleVarRef(x3def)), "w2" -> Singleton(TupleVarRef(x2def))))))
    val x4def = VarDef(Symbol.fresh("x"), TupleType("w1" -> BagType(itemTp2), "w2" -> BagType(itemTp)))
    val rq3 = ForeachUnion(x4def, rq2, ForeachUnion(x2def, relationR, 
                Singleton(Tuple("w1" -> Singleton(TupleVarRef(x4def)), "w2" -> Singleton(TupleVarRef(x2def))))))
    println(quote(rq3))
    println("")
    val rq3shred = shred(rq3)
    println(eval(rq3))
    println("")
    val rq3lin = linearize(rq3shred)
    println("Linearized set: ")
    println(quote(rq3lin))
    val crqs3 = Translator.translate(rq3lin)
    println("")
    println("Comprehension calculus: ")
    println("\nNormalized: ")
    crqs3.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    })
    /**val nrqs3 = Unnester.unnest(crqs3).asInstanceOf[PlanSet]
    sparke.execute(nrqs3)**/

    /**println("\nNot Normalized: ")
    crqs3.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    Unnester.normalize = false
    val nnrqs3 = Unnester.unnest(crqs3).asInstanceOf[PlanSet]
    sparke.execute(nnrqs3)**/

  }

  def runR4a(){
    println("-------- Recursive Query: Iteration 3 ------------")
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)

    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael")/**,
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")**/
    ), BagType(itemTp))
    val x0def = VarDef(Symbol.fresh("x"), itemTp)
    val x1def = VarDef(Symbol.fresh("x"), itemTp)
    val rq1 = ForeachUnion(x0def, relationR, 
                ForeachUnion(x1def, relationR, 
                  Singleton(Tuple("w1" -> Singleton(TupleVarRef(x0def)), "w2" -> Singleton(TupleVarRef(x1def))))))

    val x2def = VarDef(Symbol.fresh("x"), itemTp)
    val x5def = VarDef(Symbol.fresh("x"), itemTp)
    val itemTp2 = TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp))
    val x3def = VarDef(Symbol.fresh("x"), itemTp2)
    val rq2 = ForeachUnion(x3def, rq1, ForeachUnion(x2def, relationR,
                                        ForeachUnion(x5def, relationR, 
                Singleton(Tuple("w1" -> Singleton(TupleVarRef(x3def)), "w2" -> 
                  Singleton(Tuple("w1" -> Singleton(TupleVarRef(x2def)), "w2" -> Singleton(TupleVarRef(x5def)))))))))
    val x4def = VarDef(Symbol.fresh("x"), 
                  TupleType("w1" -> BagType(itemTp2), "w2" -> BagType(TupleType("w1" -> BagType(itemTp), "w2" -> BagType(itemTp)))))
    val rq3 = ForeachUnion(x4def, rq2, ForeachUnion(x2def, relationR, 
                Singleton(Tuple("w1" -> Singleton(TupleVarRef(x4def)), "w2" -> Singleton(TupleVarRef(x2def))))))
    println(quote(rq3))
    println("")
    val rq3shred = shred(rq3)
    //println(eval(rq3))
    //println("")
    val rq3lin = linearize(rq3shred)
    println("Linearized set: ")
    println(quote(rq3lin))
    val crqs3 = Translator.translate(rq3lin)
    println("")
    println("Comprehension calculus: ")
    println("\nNormalized: ")
    crqs3.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    })
    val nrqs3 = Unnester.unnest(crqs3).asInstanceOf[PlanSet]
    nrqs3.plans.foreach(e => println(calc.quote(e.asInstanceOf[calc.AlgOp])))
    //sparke.execute(nrqs3)

    /**println("\nNot Normalized: ")
    crqs3.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    Unnester.normalize = false
    val nnrqs3 = Unnester.unnest(crqs3).asInstanceOf[PlanSet]
    sparke.execute(nnrqs3)**/

  }

  def runR4b(){
    println("-------- Recursive Query: Iteration 3 ------------")
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)

    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael")/**,
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")**/
    ), BagType(itemTp))
    val x0def = VarDef(Symbol.fresh("x"), itemTp)
    val x1def = VarDef(Symbol.fresh("x"), itemTp)
    val x4def = VarDef(Symbol.fresh("x"), itemTp)
    val rq2 = ForeachUnion(x0def, relationR,
                Singleton(Tuple("w1" -> Singleton(TupleVarRef(x0def)), "w2" -> ForeachUnion(x1def, relationR, 
                  Singleton(Tuple("w3" -> Singleton(TupleVarRef(x0def)), "w4" -> ForeachUnion(x4def, relationR, 
                    Singleton(TupleVarRef(x4def)))))))))

    val x2def = VarDef(Symbol.fresh("x"), 
                  TupleType("w1" -> BagType(itemTp), "w2" -> BagType(TupleType("w3" -> BagType(itemTp), "w4" -> BagType(itemTp)))))
    val x3def = VarDef(Symbol.fresh("x"), itemTp)
    val rq3 = ForeachUnion(x2def, rq2,
                //Singleton(//Tuple("w5" -> Singleton(TupleVarRef(x3def)), 
                Singleton(Tuple("w6" -> ForeachUnion(x3def, relationR, Singleton(TupleVarRef(x3def))))))
    println(quote(rq3))
    println("")
    val rq3shred = shred(rq3)
    println(rq3shred.quote)
    println(eval(rq3))
    println("")
    val rq3lin = linearize(rq3shred)
    println("Linearized set: ")
    println(quote(rq3lin))
    val crqs3 = Translator.translate(rq3lin)
    println("")
    println("Comprehension calculus: ")
    println("\nNormalized: ")
    crqs3.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    })
    val nrqs3 = Unnester.unnest(crqs3).asInstanceOf[PlanSet]
    nrqs3.plans.foreach(e => println(calc.quote(e.asInstanceOf[calc.AlgOp])))

    //sparke.execute(nrqs3)

    /**println("\nNot Normalized: ")
    crqs3.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    Unnester.normalize = false
    val nnrqs3 = Unnester.unnest(crqs3).asInstanceOf[PlanSet]
    sparke.execute(nnrqs3)**/

  }


  def run3(){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)
    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael"),
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")
    ), BagType(itemTp))
    val xdef = VarDef(Symbol.fresh("x"), itemTp)

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
    println(quote(q2))
    println("")
    val q2shred = shred(q2)
    val q2lin = linearize(q2shred)
    println("Linearized set: ")
    println(quote(q2lin))
    println("")
    println("Comprehension calculus: ")
    val cqs2 = Translator.translate(q2lin)
    cqs2.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    println("\nNormalized: ")
    val nrqs = Unnester.unnest(cqs2).asInstanceOf[PlanSet]
    sparke.execute(nrqs)
  }

  def run5(){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)

    val dtype = TupleType("dno" -> IntType, "dname" -> StringType)
    val etype = TupleType("dno" -> IntType, "ename" -> StringType)
    val e = VarDef(Symbol.fresh("e"), etype)
    val d = VarDef(Symbol.fresh("d"), dtype)
    val employees = InputBag("Employees", 
                      List(Map("dno" -> 1, "ename" -> "one"),Map("dno" -> 2, "ename"-> "two"),
                            Map("dno" -> 3, "ename" -> "three"),Map("dno" -> 4, "ename" -> "four")), BagType(etype))
    val departments = InputBag("Departments", 
                        List(Map("dno" -> 1, "dname" -> "five"),Map("dno" -> 2, "dname" -> "six"),
                              Map("dno" -> 3, "dname" -> "seven"),Map("dno" -> 4, "dname" -> "eight")), BagType(dtype))


    println("----------------------- Query 4 -------------------")
    val q4 = ForeachUnion(d, departments,
              ForeachUnion(e, employees,
                IfThenElse(Cond(OpEq, Project(TupleVarRef(d), "dno"), Project(TupleVarRef(e), "dno")),
                  Singleton(Tuple("D" -> Project(TupleVarRef(d), "dno"), "E" -> Singleton(TupleVarRef(e)))))))
    println(quote(q4))
    println("")
    val sq4 = shred(q4)
    println(eval(q4))
    println("")
    val sq4lin = linearize(sq4)
    println("Linearized set: ")
    println(quote(sq4lin))
    println("")
    println("Comprehension calculus: ")
    val cqs4 = Translator.translate(sq4lin)
    //cqs4.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    cqs4.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    })
    println("\nNormalized: ")
    val nrqs = Unnester.unnest(cqs4).asInstanceOf[PlanSet]
    sparke.execute(nrqs)  
  }  
  
  def run6(){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)
    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = InputBag("R", List(
        Map("a" -> 42, "b" -> "Milos"),
        Map("a" -> 69, "b" -> "Michael")/**,
        Map("a" -> 34, "b" -> "Jaclyn"),
        Map("a" -> 42, "b" -> "Thomas")**/
    ), BagType(itemTp))
    val xdef = VarDef(Symbol.fresh("x"), itemTp)
    val ydef = VarDef(Symbol.fresh("y"), itemTp)
    val q4 = ForeachUnion(xdef, relationR,
              Singleton(Tuple(
                "grp" -> Project(TupleVarRef(xdef), "a"),
                "bag" -> ForeachUnion(ydef, relationR,
                  IfThenElse(Cond(OpEq, Project(TupleVarRef(xdef), "a"), 
                    Project(TupleVarRef(ydef), "a")),
                      Singleton(Tuple("q" -> Project(TupleVarRef(ydef), "b")))
                    )))))

    println(quote(q4))
    println("")
    val sq4 = shred(q4)
    println(eval(q4))
    println("")
    val sq4lin = linearize(sq4)
    println("Linearized set: ")
    println(quote(sq4lin))
    println("")
    println("Comprehension calculus: ")
    val cqs4 = Translator.translate(sq4lin)
    //cqs4.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    cqs4.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    })
    println("\nNormalized: ")
    val nrqs = Unnester.unnest(cqs4).asInstanceOf[PlanSet]
    sparke.execute(nrqs)  

  }

  def run7(){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sparke = SparkEvaluator(spark.sparkContext)
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

      val q4 = ForeachUnion(xdef, relationR,
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


    println(quote(q4))
    println("")
    val sq4 = shred(q4)
    println(eval(q4))
    println("")
    val sq4lin = linearize(sq4)
    println("Linearized set: ")
    println(quote(sq4lin))
    println("")
    println("Comprehension calculus: ")
    val cqs4 = Translator.translate(sq4lin)
    //cqs4.asInstanceOf[calc.CSequence].exprs.foreach(e => println(calc.quote(e)))
    cqs4.asInstanceOf[calc.CSequence].exprs.foreach(e => e match {
      case calc.CNamed(n, b) => println(calc.quote(calc.CNamed(n, b.asInstanceOf[CompCalc].normalize.asInstanceOf[calc.CompCalc])))
    })
    println("\nNormalized: ")
    val nrqs = Unnester.unnest(cqs4).asInstanceOf[PlanSet]
    nrqs.plans.foreach(e => println(calc.quote(e.asInstanceOf[calc.AlgOp])))
    sparke.execute(nrqs)
  }
  
  def main(args: Array[String]){
    //run1()
    //run3()
    //run5()
    //run6()
    //run7()
     
    // recursive tests
    //runR1()
    runR2()
    //runR3()
    //runR4a()
    //runR4b()
   
  }

}
