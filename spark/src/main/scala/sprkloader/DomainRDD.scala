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

    // top level 
    def splitDict(): (RDD[L], RDD[L])  = (domain, domain.sparkContext.emptyRDD[L])

    /**
      Gets a single domain partition, not using currently since local filtering 
      could result in data loss 
    **/
    def getLocalDomain(index: Int): Array[L] = {
      val part = domain.mapPartitionsWithIndex((i, it) => if (index == i) it else Iterator(), true)
      part.collect    
    }
  
    def extractLight[K:ClassTag](extract: L => K, hkeys: Set[K]): RDD[(K, L)] = 
      domain.flatMap( v => {
        val extracted = extract(v)
        if (!hkeys(extracted)) List((extracted, v)) else Nil})

    def extractDistinct[K: ClassTag](f: L => K): Set[K] = {
      domain.mapPartitions(it => it.foldLeft(Set.empty[K])(
        (acc, l) => acc + f(l)  
      ).iterator, true).collect.toSet
    }

    // local distinct 
    def createDomain[K: ClassTag](f: L => K): RDD[K] = {
      domain.mapPartitions(it => it.foldLeft(Set.empty[K])(
        (acc, l) => acc + f(l)  
      ).iterator)
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
    
    def extractDistinctBroadcastLight[K: ClassTag](f: L => K, hkeys: Set[K]): Map[K, Set[L]] = { 
      domain.mapPartitions(it => it.flatMap( lbl => {
        val key = f(lbl); if (!hkeys(key)) Iterator((key, lbl)) else Iterator()
      }), true).collect.foldLeft(HashMap.empty[K, Set[L]].withDefaultValue(Set.empty[L]))((acc, c) =>
        { acc(c._1) = acc(c._1) + c._2; acc }).toMap
    }

    def extractDistinctLight[K: ClassTag](f: L => K, hkeys: Set[K]): RDD[(K, Set[L])] = { 
      val accum1 = (acc: Set[L], l: L) => acc + l
      val accum2 = (acc1: Set[L], acc2: Set[L]) => acc1 ++ acc2
      domain.mapPartitions(it => it.flatMap( lbl => {
        val key = f(lbl); if (!hkeys(key)) Iterator((key, lbl)) else Iterator()
      }), true).aggregateByKey(Set.empty[L])(accum1, accum2)
    }

    def extractDistinctHeavyMap[K: ClassTag](f: L => K, hkeys: Set[K]): Map[K, Set[L]] = { 
      domain.mapPartitions(it => it.flatMap( lbl => {
        val key = f(lbl); if (hkeys(key)) Iterator((key, lbl)) else Iterator()
      }), true).collect.foldLeft(HashMap.empty[K, Set[L]].withDefaultValue(Set.empty[L]))((acc, c) =>
        { acc(c._1) = acc(c._1) + c._2; acc }).toMap
    }

    // map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1) (distinct)
    // this is an optimization for the case Label(InputLabel(...))
    // For label in Domain union 
    //  (label.lbl, For value in Lookup((label.lbl, dictionary)
    //    Sng(...)
    def extractDistinctLabelRekey[K: ClassTag](extract: L => K, partitioner: Partitioner, 
      hkeys: Broadcast[Set[K]]): RDD[((K, Int), Int)] = { 
        val partitions = Range(0, partitioner.numPartitions)
        domain.flatMap(lbl => {
          val nlbl = extract(lbl) 
          if (hkeys.value(nlbl)) partitions.map(i => ((nlbl, i), null)) else List(((nlbl, -1),null))
        }).reduceByKey(partitioner, (x,y) => x).map(x => (x._1, 1))
      }

    // This is an extract function that will support lookups on domains 
    // that contain labels with more than just an input label 
    // ie. Label(1, InputLabel()) 
    // For l in domain union 
    //  (l.lbl, For value in Lookup(extract_label(l.lbl), dictionary) union 
    //    For s in S union 
    //      If extract_a(l.lbl) == s.a 
    //      Then (value.b, s.c))
    def extractDistinctRekey[K: ClassTag](extract: L => K, partitioner: Partitioner, 
      hkeys: Broadcast[Set[K]]): RDD[((K, Int), Set[L])] = { 
        val partitions = Range(0, partitioner.numPartitions)
        domain.flatMap(lbl => {
          val nlbl = extract(lbl) 
          if (hkeys.value(nlbl)) partitions.map(i => ((nlbl, i), lbl)) else List(((nlbl, -1), lbl))
        }).aggregateByKey(Set.empty[L], partitioner)(
          (acc: Set[L], l: L) => acc + l, (acc1: Set[L], acc2: Set[L]) => acc1 ++ acc2)
    }

    // extract and do no distinct
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
