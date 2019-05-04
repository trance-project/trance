package shredding.spark

/**
  * App to have runner for spark testing
  */

import shredding.Utils.Symbol
import shredding.core._
import shredding.calc.PipelineRunner
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object App extends PipelineRunner with SparkEvaluator with Serializable{
  
  def main(args: Array[String]){
    run1()
  }

  /**
    * Test 1: Run spark on flat input without shredding
    */
     
  def run1(){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
    val relationRValue = spark.sparkContext.parallelize(List(
      (42, "Milos"), (69, "Michael"), (34, "Jaclyn"), (42, "Thomas")))

    val ctx = new Context()
    ctx.add(relationR.varDef, relationRValue)
    val sparke = new Evaluator(ctx)
    
    val xdef = VarDef(Symbol.fresh("x"), itemTp)
    val q = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> TupleVarRef(xdef)("b"))))

    val ucq = Pipeline.run(q)
    sparke.execute(ucq)

  }

}
