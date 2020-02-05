package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import SkewPairRDD._
import UtilPairRDD._
import DomainRDD._

object SkewDictRDD {
  
  implicit class SkewDictFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,Iterable[V])]) extends Serializable {

  val reducers = Config.minPartitions
  val partitions = lrdd.getNumPartitions

  /**def heavyKeys(threshold: Int = 1000): Set[K] = {
    val hkeys = lrdd.mapPartitions( it =>
      Util.countValuesByKey(it).filter(_._2 > threshold).iterator,true)
    if (reducers > threshold) hkeys.filter(_._2 >= reducers).keys.collect.toSet
    else hkeys.keys.collect.toSet
  }**/
  
  def labelSizeByPartition(): RDD[((K, Int), Int)] = {
		lrdd.mapPartitionsWithIndex((index, it) => 
			it.map{ case (lbl, bag) => (lbl, index) -> bag.size }, true)	
	}
	
	def lookup[L: ClassTag](rrdd: RDD[L], domop: L => K): RDD[(K, Iterable[V])] = {
      val domPrep = rrdd.extractDistinct(domop) 
      val domain = lrdd.sparkContext.broadcast(domPrep)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => if (domain.value(key)) Iterator((key, value)) else Iterator()}, true)
  }

	def lookup[L: ClassTag,S](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(K, Iterable[S])] = {
      val domPrep = rrdd.extractDistinct(domop)
      val domain = lrdd.sparkContext.broadcast(domPrep)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => 
          if (domain.value(key)) Iterator((key, value.map(bagop))) else Iterator()}, true)
  }

  // label could maintain partition information to avoid this process
  // when the domain is too large to collect or broadcast
  def lookupSkewLeft[L: ClassTag,S](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(K, Iterable[S])] = {
    val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
      it.map{case (k,v) => (k, index) -> v.map(bagop)}, true)
    val tagDomain = rrdd.extractDistinctRekey(domop, partitions)
    tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).map{
      case ((key, index), (bag, _)) => key -> bag.flatten
    }
  }

  def lookupSkewLeftFlat[L: ClassTag,S](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(S, (K, V))] = {
    val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
      it.map{case (k,v) => (k, index) -> v}, true)
    val tagDomain = rrdd.extractDistinctRekey(domop, partitions)
    tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).flatMap{ pair =>
      for (b <- pair._2._1.flatten.iterator) yield (bagop(b), (pair._1._1, b))
    }
  }

  def lookupSkewLeft[L:ClassTag](rrdd: RDD[L], domop: L => K): RDD[(K, Iterable[V])] = {
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => (k, index) -> v}, true)
      val tagDomain = rrdd.extractDistinctRekey(domop, partitions)
      tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).map{
        case ((key, index), (bag, _)) => key -> bag.flatten
      }
    }

  }
}
