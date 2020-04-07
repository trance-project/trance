package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner

object TopRDD{
  
  implicit class TopFunctions[L: ClassTag](lrdd: RDD[L]) extends Serializable {

    def evaluate: Unit = 
      lrdd.sparkContext.runJob(lrdd, (iter: Iterator[_]) => {})

    def print: Unit = lrdd.collect.foreach(println(_))

    def empty: RDD[L] = lrdd.sparkContext.emptyRDD[L]

    // local distinct, vector distinct instead?
    def createDomain[K: ClassTag](f: L => K): RDD[K] = {
      lrdd.mapPartitions(it => it.foldLeft(Set.empty[K])(
        (acc, l) => acc + f(l)
      ).iterator)
    }

    def cartesianDomain(rrdd: RDD[K]): RDD[(K, L)] = {
      val domain = rrdd.distinct
      domain.cartesian(lrdd)
    }

}

