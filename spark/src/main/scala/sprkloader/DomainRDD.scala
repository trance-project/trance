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

    // map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1) (distinct)
    def extractDistinctRekey[K: ClassTag](extract: L => K, partitioner: Partitioner, hkeys: Broadcast[Set[K]]): RDD[((K, Int), Int)] = { 
      val partitions = Range(0, partitioner.numPartitions)
      domain.flatMap(lbl => {
        val nlbl = extract(lbl) 
        if (hkeys.value(nlbl)) partitions.map(i => ((nlbl, i), null)) else List(((nlbl, -1),null))
      }).reduceByKey(partitioner, (x,y) => x).map(x => (x._1, 1))
    }

    // extract when the domain alreay consists of local sets
    def extractRekey[K: ClassTag](extract: L => K, numPartitions: Int, hkeys: Broadcast[Set[K]]): RDD[((K, Int), Int)] = {
      val partitions = Range(0, numPartitions)
      domain.flatMap(lbl => {
        val nlbl = extract(lbl) 
        if (hkeys.value(nlbl)) partitions.map(i => (nlbl, i) -> 1)
        else List(((nlbl, -1),1))
      })
    }

  }

}
