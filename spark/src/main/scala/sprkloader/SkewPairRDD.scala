package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import UtilPairRDD._
import UtilSkewDistribution._

object SkewPairRDD {
 
  implicit class SkewPairRDDFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]) extends Serializable {

    val reducers = Config.minPartitions
	  val threshold = Config.threshold
    val partitions = lrdd.getNumPartitions
 	
	  def heavyKeys(threshold: Int = threshold): Set[K] = {
      val hkeys = lrdd.mapPartitions( it => 
        Util.countDistinct(it).filter(_._2 > threshold).iterator,true)
      //if (reducers > threshold) hkeys.filter(_._2 >= reducers).keys.collect.toSet
      //else hkeys.keys.collect.toSet
      hkeys.keys.collect.toSet
    }

    def rekeyBySet[S](rrdd: RDD[(K, S)], keyset: Broadcast[Set[K]]): (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val rekey = 
        lrdd.mapPartitions( it => {
          it.zipWithIndex.map{ case ((k,v), i) => 
            (k, { if (keyset.value(k)) i % reducers*2 else 0 }) -> v
          }}, true)

      val dupp = 
        rrdd.flatMap{ case (k,v) =>
          Range(0, {if (keyset.value(k)) reducers*2 else 1 }).map(id => (k, id) -> v) 
        }
      (rekey, dupp)
    }
    
    def rekeyByIndex[S: ClassTag](rrdd: RDD[(K, S)], keyset: Broadcast[Set[K]], distinctDomain: Boolean = false): (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      // tell each heavy partition to stay where it is
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)

      val partitionRange = Range(0, partitions)
      val dupp = if (distinctDomain) {
        val accum1 = (acc: Set[S], lbl: S) => acc + lbl
        val accum2 = (acc1: Set[S], acc2: Set[S]) => acc1 ++ acc2
        rrdd.aggregateByKey(Set.empty[S])(accum1, accum2).flatMap{
          case (key, cbuf) => cbuf.flatMap{ lbl =>
            if (keyset.value(key)) partitionRange.map(id => (key, id) -> lbl) else List((key, -1) -> lbl)
          }
        }
       } else rrdd.flatMap{ case (k,v) => 
          if (keyset.value(k)) partitionRange.map(id => (k, id) -> v)
          else List(((k, -1),v)) 
        }
      (rekey, dupp)
    }

	  def filterHeavy[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[(K, V)],RDD[(K, S)])  = {
      val lheavy = lrdd.filterPartitions((i: (K,V)) => hkeys.value(i._1))
      val rheavy = rrdd.filterPartitions((i: (K,S)) => hkeys.value(i._1))
      (lheavy, rheavy)
    }

	  def filterLight[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[(K, V)],RDD[(K, S)]) = {
      val llight = lrdd.filterPartitions((i: (K,V)) => !hkeys.value(i._1))
      val rlight = rrdd.filterPartitions((i: (K,S)) => !hkeys.value(i._1))
      (llight, rlight)
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[(K,S)], partitioner: Option[Partitioner] = None): RDD[(V,S)] = {
	    val cgrp = partitioner match {
		    case Some(p) => lrdd.cogroup(rrdd, p)
		    case _ => lrdd.cogroup(rrdd)
	    }
	    cgrp.flatMap{ pair =>
        for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
      }
    }

    def joinSaltLeft[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
      val hk = heavyKeys()
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyBySet(rrdd, hkeys)
        rekey.cogroup(dupp).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
        }
      } else joinDropKey(rrdd)
    }
    
    // removing distribution check for now
  	def joinSkewLeft[S:ClassTag](rrdd: RDD[(K, S)], distinctDomain: Boolean = true): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys, distinctDomain)
        rekey.joinDropKey(dupp, Some(new SkewPartitioner(partitions)))
  		}else 
        // need to look at effect here
        if (distinctDomain) joinDropKey(rrdd.distinct)
        else joinDropKey(rrdd)
    }

	  /**def joinSkewLeftPrintDist[S: ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
      val hkm = lrdd.heavyKeysByPartition[K]()
  	  val hk = hkm.getKeys()
      if (hk.nonEmpty) {
         val hkeys = lrdd.sparkContext.broadcast(hk)
         val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys)
         println("before join")
         hkm.foreach(println(_))
         val result = rekey.cogroup(dupp, new SkewPartitioner(lrdd.partitions.size)).flatMap{ pair =>
            for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
         }
         println("after join")
         result.heavyKeysByPartition[K]().foreach(println(_))
         result 
      }
      else joinDropKey(rrdd)
    }**/

    def groupByLabel(): RDD[(K, Iterable[V])] = {
      val groupBy = (i: Iterator[(K,V)]) => {
        val hm = HashMap[K, Vector[V]]()
        i.foreach{ v =>
          hm(v._1) = hm.getOrElse(v._1, Vector()) :+ v._2
        }
        hm.iterator
      }
      lrdd.mapPartitions(groupBy, true)
    }

    def groupByLabel[R: ClassTag,S: ClassTag](f: (K,V) => (R,S)): RDD[(R, Iterable[S])] = {
      val groupBy = (i: Iterator[(K,V)]) => {
        val hm = HashMap[R, Vector[S]]()
        i.foreach{ v0 =>
          val (k,v) = f(v0._1, v0._2) 
          hm(k) = hm.getOrElse(k, Vector()) :+ v
        }
        hm.iterator
      }
      lrdd.mapPartitions(groupBy, true)
    }
 
    def groupByLabelSet(): RDD[(K, Iterable[V])] = {
      val groupBy = (i: Iterator[(K,V)]) => {
        val hm = HashMap[K, Set[V]]()
        i.foreach{ v =>
          hm(v._1) = hm.getOrElse(v._1, Set()) + v._2
        }
        hm.iterator
      }
      lrdd.mapPartitions(groupBy, true)
    }
 
    // this needs more thought
	 def groupBySkew(): RDD[(K, Iterable[V])] = {
      val hk = heavyKeys()
      if (hk.nonEmpty){
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val rekey = lrdd.mapPartitionsWithIndex( (index, it) =>
          it.zipWithIndex.map{ case ((k,v), i) => 
            (k, { if (hkeys.value(k)) i % reducers else index }) -> v
          }, true)
        rekey.groupByKey(new SkewPartitioner(reducers)).map{ case ((k, _), v) => k -> v }
      }
      else groupByLabel() 
    }

  
  }

}
