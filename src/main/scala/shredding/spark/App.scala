package shredding.spark

/**
  * App to have runner for spark testing
  */

import shredding.Utils.Symbol
import shredding.core._
import shredding.calc.ShredPipelineRunner
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object App extends ShredPipelineRunner with SparkEvaluator with SparkRuntime with Serializable{
  
  def main(args: Array[String]){
    run1()
    //run2()
  }

  /**
    * Test 1: Run spark on flat input without shredding
    */
     
  def run1(){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
    val rValue = List((42, "Milos"), (69, "Michael"), (34, "Jaclyn"), (42, "Thomas"))
    val relationRValue = spark.sparkContext.parallelize(rValue)

    val ctx = new Context()
    ctx.add(relationR.varDef, relationRValue)
    val sparke = new Evaluator(ctx)
    
    val xdef = VarDef(Symbol.fresh("x"), itemTp)
    val q = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> TupleVarRef(xdef)("b"))))

    val ucq = Pipeline.run(q)
    val ucqs = ShredPipeline.run(q)
    sparke.execute(ucq)
    ctx.reset
    
    val flatR = spark.sparkContext.parallelize(List(SInLabel()))
    val ftp = LabelType(Map[String, Type]())
    val fdef = VarDef("R^F", ftp)
    val dictR = spark.sparkContext.parallelize(rValue)
    val dtp = BagDictType(BagType(TupleType(Map("a" -> IntType, "b" -> StringType))), 
                TupleDictType(Map("a" -> EmptyDictType, "b" -> EmptyDictType)))
    val ddef = VarDef("R^D", dtp)
    val initCtx = VarDef("initCtx", BagType(TupleType(Map("lbl" -> LabelType(Map("R^D" -> dtp, "R^F" -> ftp))))))
    val initCtxValue = spark.sparkContext.parallelize(List(SOutLabel("R^F" -> flatR, "R^D" -> dictR)))
    ctx.add(fdef, flatR)
    ctx.add(ddef, dictR)
    ctx.add(initCtx, initCtxValue)
    println(ctx.ctx)
    sparke.execute(ucqs)
  }

  def run2(){
      val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
      val spark = SparkSession.builder().config(conf).getOrCreate()
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
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      val relationRValue = spark.sparkContext.parallelize(
                            List((42, List(("Milos", 123, List(123,456,789,123)),
                                           ("Michael", 7, List(2,9,1)),
                                           ("Jaclyn", 12, List(14,12)))),
                                  (69, List(("Thomas", 987, List(987,654,987,654,987,987))))))
      val ctx = new Context()
      ctx.add(relationR.varDef, relationRValue)
      val sparke = new Evaluator(ctx)
      
      val xdef = VarDef("x", itemTp)
      val wdef = VarDef("w", nestedItemTp)
      val ndef = VarDef("y", TupleType("n" -> IntType))

      val q = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "o5" -> TupleVarRef(xdef)("h"),
          "o6" ->
            ForeachUnion(wdef, BagProject(TupleVarRef(xdef), "j"),
              Singleton(Tuple(
                "o7" -> TupleVarRef(wdef)("m"),
                "o8" -> Total(BagProject(TupleVarRef(wdef), "k"))
              ))
            )
        )))

      val ucq = Pipeline.run(q)
      sparke.execute(ucq)
  }

}
