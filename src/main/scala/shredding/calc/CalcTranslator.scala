package shredding.calc

import shredding.core._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Translate Algebra term trees into Spark and execute
  */

trait AlgTranslator {
  this: Algebra with Calc =>
  
  class SparkEvaluator(sc: SparkContext){
    
    import collection.mutable.{HashMap => HMap}
    
    //val ctx: HMap[String, RDD[_]] = HMap[String, RDD[_]]()
     
    def evaluate[A,B](e: AlgOp): RDD[_] = e match {
      case Term(OuterJoin(v, p), Term(e1, e2)) =>
        evaluate(e1).asInstanceOf[RDD[(String,Any)]].fullOuterJoin(evaluate(e2).asInstanceOf[RDD[(String,Any)]])
      case Term(Join(v, p), Term(e1, e2)) => 
        evaluate(e1).asInstanceOf[RDD[(String,Any)]].join(evaluate(e2).asInstanceOf[RDD[(String,Any)]])
      case Select(x, v, p) => evaluateBag(x)
      case _ => sys.error("unsupported evaluation for "+e)
    }

    def evaluateBag(e: BagCalc): RDD[Any] = e match {
      // temporary solution to embedded maps
      case InputR(n, d, t) => sc.parallelize(d)
      case _ => sys.error("unsupported evaluation for "+e)
    }
      
  } 

  object SparkEvaluator{
    def apply(sc: SparkContext) = new SparkEvaluator(sc)
  }
}
