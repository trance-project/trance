package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import SkewPairRDD._
import UtilPairRDD._

object DomainRDD{

  implicit class DomainFunctions[L: ClassTag](domain: RDD[L]) extends Serializable {
    
    /**
      Gets a single domain partition, not using currently since local filtering 
      could result in data loss 
    **/
    def getLocalDomain(index: Int): Array[L] = {
      val part = domain.mapPartitionsWithIndex((i, it) => if (index == i) it else Iterator(), true)
      part.collect    
    }
  
    def extractDistinct[K: ClassTag](f: L => K): Set[K] = {
      domain.mapPartitions(it => it.foldLeft(Set.empty[K])(
        (acc, l) => acc + f(l)  
      ).iterator, true).collect.toSet
    }

    // local distinct 
    def createDomain[K: ClassTag](f: L => K): RDD[K] = {
      domain.mapPartitions(it => it.foldLeft(Set.empty[K])(
        (acc, l) => acc + f(l)  
      ).iterator, true)
    }

    def createDomainSet[K: ClassTag](f: L => K): Set[K] = {
      domain.mapPartitions(it => it.foldLeft(Set.empty[K])(
        (acc, l) => acc + f(l)  
      ).iterator, true).collect.toSet
    }

    def extractDistinctHeavy[K: ClassTag](f: L => K, hkeys: Set[K]): Set[K] = { 
      domain.mapPartitions(it => it.flatMap( lbl => {
        val key = f(lbl); if (hkeys(key)) Iterator(key) else Iterator()
      }), true).collect.toSet
    }

    // check performance with createDomainRekey
    def extractDistinctRekey[K: ClassTag](f: L => K, numPartitions: Int): RDD[((K, Int), Int)] = { 
      val partitions = Range(0, numPartitions)
      domain.flatMap(lbl => 
        partitions.foldLeft(Set.empty[((K,Int), Int)])((acc, i) => acc + { (f(lbl), i) -> 1 }))
    }

    // extract when the domain alreay consists of local sets
    def extractRekey[K: ClassTag](f: L => K, numPartitions: Int): RDD[((K, Int), Int)] = {
      val partitions = Range(0, numPartitions)
      domain.flatMap(lbl => partitions.map(i => (f(lbl), i) -> 1))
    }

  }

}
