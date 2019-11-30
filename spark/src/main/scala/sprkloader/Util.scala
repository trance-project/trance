package sprkloader

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

object Util{ 

  val reducers = Config.minPartitions

  def countDistinct[K,V](i: Iterator[(K,V)]) = {
    i.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
      { acc(c._1) += 1; acc } )
  }

  def heavyKeys[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]): Set[K] = {
      lrdd.mapPartitions{ it =>
        countDistinct(it).filter(_._2 > 1000).iterator }
      .reduceByKey(_ + _)
      .filter(_._2 >= reducers)
      .keys.collect.toSet
   }
}
