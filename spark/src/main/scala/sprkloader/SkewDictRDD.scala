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
    def union: RDD[(K, Vector[V])] = (light, heavy).union
    def cache: Unit = (light, heavy).cache

    def createDomain[C: ClassTag](f: V => C): (RDD[C], RDD[C]) = {
      (light.createDomain(f), heavy.createDomain(f))
    }

    def flatMap[S:ClassTag](f: ((K, Vector[V])) => Vector[S]): (RDD[S], RDD[S]) = {
      (light.flatMap(f), heavy.flatMap(f))
    }

    def lookupIteratorDomain(rrdd: (RDD[K], RDD[K])): 
      (RDD[(K,V)], RDD[(K,V)], Broadcast[Set[K]]) = {
        val runion = rrdd.union
        if (heavyKeys.value.nonEmpty){
          val rlight = runion.filter(i => !heavyKeys.value(i))
          val lresult = light.lookupIteratorDomain(rlight)

          val rheavy = runion.filter(i => !heavyKeys.value(i)).collect.toSet
          val heavyRight = heavy.sparkContext.broadcast(rheavy)
          val hresult = heavy.mapPartitions(it =>
            it.flatMap{ case (k,v) => 
              if (heavyRight.value(k)) v.map( v1 => (k,v1))
              else Vector()
            }, true)
          (lresult, hresult, heavyKeys)
        }else{
          val result = lrdd.union.lookupIteratorDomain(runion)
          (result, result.sparkContext.emptyRDD[(K,V)], heavyKeys)
        }
      }

    def cogroupDomain(dom: (RDD[K], RDD[K])): 
      (RDD[(K, Vector[V])], RDD[(K, Vector[V])], Broadcast[Set[K]]) = {
        val domain = dom.union
        if (heavyKeys.value.nonEmpty){
          val ldomain = domain.filter(i => !heavyKeys.value(i))
          val lresult = light.cogroupDomain(ldomain)

          val hdomain = domain.filter(i => heavyKeys.value(i)).collect.toSet
          val heavyDomain = heavy.sparkContext.broadcast(hdomain)
          val hresult = heavy.filter{ case (k,v) => heavyDomain.value(k)}
          (lresult, hresult, heavyKeys)
        }else{
          val result = light.cogroupDomain(domain)
          (result, result.empty, heavyKeys)
        }
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

    def lookupIteratorDomain(rrdd: (RDD[K], RDD[K])): 
      (RDD[(K,V)], RDD[(K,V)], Broadcast[Set[K]]) = {
        val (lunion, hk) = lrdd.heavyKeys
        val hkeys = lunion.sparkContext.broadcast(hk)
        if (hkeys.value.nonEmpty){
          (lunion.filter(i => !hkeys.value(i._1)), 
            lunion.filter(i => hkeys.value(i._1)), hkeys).lookupIteratorDomain(rrdd)
        }else{
          val result = lunion.lookupIteratorDomain(rrdd.union)
          (result, result.sparkContext.emptyRDD[(K,V)], hkeys)
        }
      }

    def cogroupDomain(dom: (RDD[K], RDD[K])): 
      (RDD[(K, Vector[V])], RDD[(K, Vector[V])], Broadcast[Set[K]]) = {
        val (lunion, hk) = lrdd.heavyKeys
        val hkeys = lunion.sparkContext.broadcast(hk)
        if (hkeys.value.nonEmpty){
          (lunion.filter(i => !hkeys.value(i._1)), 
            lunion.filter(i => hkeys.value(i._1)), hkeys).cogroupDomain(dom)
        }else{
          val result = lunion.cogroupDomain(dom.union)
          (result, result.empty, hkeys)
        }
      }

    def rightCoGroupDropKey[S:ClassTag](rrdd:(RDD[(K,S)], RDD[(K,S)])): (RDD[(S, Vector[V])], RDD[(S, Vector[V])]) = {
      val (lunion, hk) = heavyKeys
      val hkeys = lunion.sparkContext.broadcast(hk) 
      if (hkeys.value.nonEmpty){
        (lunion.filter(i => !hkeys.value(i._1)), 
          lunion.filter(i => hkeys.value(i._1)), hkeys).rightCoGroupDropKey(rrdd)
      }else{
        val result = lunion.rightCoGroupDropKey(rrdd.union)
        (result, result.empty)
      }

    }

  }

}