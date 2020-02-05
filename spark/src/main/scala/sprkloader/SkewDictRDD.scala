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

	def labelSizeByPartition(): RDD[((K, Int), Int)] = {
		lrdd.mapPartitionsWithIndex((index, it) => 
			it.map{ case (lbl, bag) => (lbl, index) -> bag.size }, true)	
	}
	
	def lookup[L: ClassTag](rrdd: RDD[L], domop: L => K): RDD[(K, Iterable[V])] = {
      val domain = rrdd.extractDistinct(domop) 
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => if (domain(key)) Iterator((key, value)) else Iterator()}, true)
  }

	def lookup[L: ClassTag,S](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(K, Iterable[S])] = {
      val domain = rrdd.extractDistinct(domop)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => 
          if (domain(key)) Iterator((key, value.map(bagop))) else Iterator()}, true)
  }


  def lookupSkewLeft[L: ClassTag,S](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(K, Iterable[S])] = {
      val hk = lrdd.heavyKeys()
      if (hk.nonEmpty) {
        val domain = rrdd.extractDistinctHeavy(domop, hk)
        val hkeys = lrdd.sparkContext.broadcast(domain)
        lrdd.mapPartitions(it => 
          it.flatMap{ case (key, value) => 
            if (hkeys.value(key)) Iterator((key, value.map(bagop))) else Iterator()}, true)
      } else lookup(rrdd, domop, bagop)      
    }

  def lookupSkewLeft[L:ClassTag](rrdd: RDD[L], domop: L => K): RDD[(K, Iterable[V])] = {
      val hk = lrdd.heavyKeys()
      if (hk.nonEmpty) {
        val domain = rrdd.extractDistinctHeavy(domop, hk)
        val hkeys = lrdd.sparkContext.broadcast(domain)
        lrdd.mapPartitions(it => 
          it.flatMap{ case (key, value) => 
            if (hkeys.value(key)) Iterator((key, value)) else Iterator()}, true)
      } else lookup(rrdd, domop)      
    }

  /**def lookupJoin[S](rrdd: RDD[(K, S)]): RDD[(S, Iterable[V])] = {
    lrdd.cogroup(rrdd).flatMap{ pair =>
      for (b <- pair._2._1.iterator; l <- pair._2._2.iterator) yield (l, b)
    }
  }

  def lookupJoin[S, T](rrdd: RDD[(K, S)], bagop: V => T): RDD[(S, Iterable[T])] = {
    lrdd.cogroup(rrdd).flatMap{ pair =>
      for (b <- pair._2._1.iterator.map(bagop); l <- pair._2._2.iterator) yield (l, b)
    }
  }**/

  }

}
