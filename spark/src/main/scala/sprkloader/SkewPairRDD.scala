package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import org.apache.spark.rdd.CoGroupedRDD
import UtilPairRDD._
import UtilSkewDistribution._

object SkewPairRDD {
  
  /**
    Assume each dictionary element is tagged with partition information
    **/
  implicit class DictRDDFunctions[V: ClassTag](dict: RDD[(Int,V)]) extends Serializable {
    
    def indexKeyBy[K](f: V => K): RDD[((K, Int), V)] = {
      dict.mapPartitions(it =>
        it.map{ case (index, v) => ((f(v), index), v)}, true)
    }

    def duplicate[K](f: V => K, numPartitions: Int): RDD[((K, Int), V)] = {
      val range = Range(0, numPartitions)
      dict.mapPartitions(it =>
        it.flatMap{ case (index, v) => range.map(id => ((f(v), id), v))}, true)
    }

  }

  implicit class IndexJoinFunctions[K: ClassTag, V: ClassTag](dict: RDD[((K, Int), V)]) extends Serializable {
    
    val numPartitions = dict.getNumPartitions

   	def joinIndexed[S:ClassTag](rrdd: RDD[(Int, S)], f: S => K): RDD[(Int, (V, S))] = { 
      val dupp = rrdd.duplicate(f, numPartitions)
      val partitioner = new SkewPartitioner(numPartitions)
      dict.cogroup(dupp, partitioner).flatMapValues{ pair =>
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v,w)
      }.mapPartitions( it => it.map{ case ((k,id), (v,w)) => (id, (v,w)) }, true)
    }
  
  }
  
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
 
    def rekeyByIndex[S: ClassTag](rrdd: RDD[S], keyset: Broadcast[Set[K]], f: S => K): 
      (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)

      val partitionRange = Range(0, partitions)
      val dupp = rrdd.flatMap{ case v => 
          val k = f(v)
          if (keyset.value(k)) partitionRange.map(id => (k, id) -> v)
          else List(((k, -1),v)) 
        }
      (rekey, dupp)
    }
   
    def rekeyByIndex[S: ClassTag](rrdd: RDD[(K, S)], keyset: Broadcast[Set[K]]): 
      (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)

      val partitionRange = Range(0, partitions)
      val dupp = rrdd.flatMap{ case (k,v) => 
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

    def joinSaltLeft[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
      val hk = heavyKeys()
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyBySet(rrdd, hkeys)
        rekey.cogroup(dupp).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
        }
      } else 
        lrdd.cogroup(rrdd).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
        }
    }
 
   
  	def joinSkewLeft[S:ClassTag](rrdd: RDD[S], f: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys, f)
        rekey.cogroup(dupp, new SkewPartitioner(partitions)).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
        }
  		}else lrdd.cogroup(rrdd.map(v => f(v) -> v)).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
        }
    }

  	def joinSkewLeft[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys)
	      rekey.cogroup(dupp, new SkewPartitioner(partitions)).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
        }
  		}else 
 	      lrdd.cogroup(rrdd).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
        }
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
