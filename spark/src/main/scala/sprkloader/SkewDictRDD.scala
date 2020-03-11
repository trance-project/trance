package sprkloader

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.HashMap
import DictRDDOperations._
import SkewTopRDD._
import Util._

object SkewDictRDD {

  implicit class SkewDictKeyFunctions[K: ClassTag, V: ClassTag](lrdd: (RDD[(K,Vector[V])], RDD[(K,Vector[V])], Broadcast[Set[K]])) extends Serializable {

    val light = lrdd._1
    val heavy = lrdd._2
    val heavyKeys = lrdd._3
    val partitions = light.getNumPartitions

    def print: Unit = (light, heavy).print
    def evaluate: Unit = (light, heavy).evaluate

    def createDomain[C: ClassTag](f: V => C): (RDD[C], RDD[C]) = {
      (light.createDomain(f), heavy.createDomain(f))
    }

    def flatMap[S:ClassTag](f: ((K, Vector[V])) => Vector[S]): (RDD[S], RDD[S]) = {
      (light.flatMap(f), heavy.flatMap(f))
    }

    // should only be used in unshredding right now...
    def rightCoGroupDropKey[S:ClassTag](rrdd:(RDD[(K,S)], RDD[(K,S)])): (RDD[(S, Vector[V])], RDD[(S, Vector[V])]) = {
      val runion = rrdd.union
      if (heavyKeys.value.nonEmpty){
        val rlight = runion.filter(i => !heavyKeys.value(i._1))
        val lresult = light.rightCoGroupDropKey(rlight)

        val rheavy = runion.filter(i => heavyKeys.value(i._1))
        val hresult = heavy.rightCoGroupDropKey(rheavy)
        (lresult, hresult)
      }else {
        val result = light.rightCoGroupDropKey(runion)
        (result, result.empty)
      }
    }

  }


  implicit class SkewDictFunctions[K: ClassTag, V: ClassTag](lrdd: (RDD[(K,Vector[V])], RDD[(K,Vector[V])])) extends Serializable {

    val light = lrdd._1
    val heavy = lrdd._2
    val threshold = Config.threshold
    val partitions = light.getNumPartitions

    def heavyKeys: (RDD[(K, Vector[V])], Set[K]) = {
      val lunion = lrdd.union
      val hkeys = lunion.mapPartitions( it =>
        it.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
          { acc(c._1) += c._2.size; acc } 
        ).filter(_._2 > threshold).iterator, true).keys.collect.toSet
      (lunion, hkeys)
    }

    def createDomain[C: ClassTag](f: V => C): (RDD[C], RDD[C]) = {
      (light.createDomain(f), heavy.createDomain(f))
    }

    def rightCoGroupDropKey[S:ClassTag](rrdd:(RDD[(K,S)], RDD[(K,S)])): (RDD[(S, Vector[V])], RDD[(S, Vector[V])]) = {
      val (lunion, hk) = heavyKeys
      val hkeys = lunion.sparkContext.broadcast(hk) 
      if (hkeys.value.nonEmpty){
        (lunion.filter(i => !hkeys.value(i._1)), lunion.filter(i => hkeys.value(i._1)), hkeys).rightCoGroupDropKey(rrdd)
      }else{
        val result = lunion.rightCoGroupDropKey(rrdd.union)
        (result, result.empty)
      }

    }

  }

}