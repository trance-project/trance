package framework.library.intermediary

import framework.library.utilities.SparkUtil.getSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class WrapDataset[T](in: T) extends WrappedDataframe[T] {

  val spark: SparkSession = getSparkSession
  def evaluate[T](e: Rep[T], env: Map[Rep[_], Any]): T = e match {
    case FlatMap(e, Fun(in, out)) =>
      val c1 = evaluate(e, env)
      val d1 = c1.collect().flatMap(x => evaluate(out, env + (in -> x)).collect())
      val result: RDD[Row] = spark.sparkContext.parallelize(d1)
      val x = spark.createDataFrame(result, c1.schema)
      x.asInstanceOf[T]
    case Merge(e, Fun(in, out)) =>
      val c1 = evaluate(e, env)
      val d1 = c1.union(evaluate(out, env + (in -> c1)).asInstanceOf[DataFrame])
      d1.asInstanceOf[T]
    case Sng(x) =>
      val l = evaluate(x, env).asInstanceOf[GenericRowWithSchema]
      val r: Row = l
      val result: RDD[Row] = spark.sparkContext.parallelize(Seq(r))
      spark.createDataFrame(result, l.schema).asInstanceOf[T]
    case WrapDataset(in) =>
      in.asInstanceOf[T]
    case s@Sym(_) =>
      val e = env(s)
      e.asInstanceOf[T]
    case _ =>
      sys.error("Unsupported")
  }
}
