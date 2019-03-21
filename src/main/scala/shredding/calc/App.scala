package shredding.calc

import shredding.core._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App extends AlgTranslator with Algebra with Calc{

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dtype = TupleType("dno" -> IntType)
    val etype = TupleType("dno" -> IntType)
    val e = VarDef("e", etype)
    val d = VarDef("d", dtype)
    val employees = InputR("Employees", 
                      List(("dno" -> 1),("dno" -> 2),("dno" -> 3),("dno" -> 4)), BagType(etype))
    val departments = InputR("Departments", 
                        List(("dno" -> 1),("dno" -> 2),("dno" -> 3),("dno" -> 4)), BagType(dtype))
    
    val v = VarDef("v", BagType(etype), VarCnt.currId+1)
    val nopreds = List[PrimitiveCalc]()
    val unq1 = Term(OuterJoin(List(e,d), nopreds), 
                 Term(Select(employees, e, nopreds),
                   Select(departments, d, nopreds)))
   
    val e1 = Select(employees, e, nopreds) 
    val sparke = SparkEvaluator(spark.sparkContext)
    val nrdd = sparke.evaluate(unq1)
    nrdd.take(10).foreach(println)
    println(nrdd)
    println(nrdd.count)

  }

}
