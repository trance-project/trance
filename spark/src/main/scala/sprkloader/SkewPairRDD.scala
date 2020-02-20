package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import org.apache.spark.rdd.CoGroupedRDD
import UtilPairRDD._
import UtilSkewDistribution._
import DomainRDD._

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
 
    def rekeyByIndex[S: ClassTag](rrdd: RDD[S], keyset: Broadcast[Set[K]], f: S => K, partitioner: Partitioner): 
      (RDD[((K, Int), V)], RDD[((K, Int), Set[S])]) = {
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)
      val dupp = rrdd.duplicateDistinct(f, partitioner, keyset)
      (rekey, dupp)
    }

    def rekeyByIndex[S: ClassTag](rrdd: RDD[S], keyset: Broadcast[Set[K]], f: S => K): 
      (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)

      val partitionRange = Range(0, partitions)
      val dupp = rrdd.flatMap{ v => { 
          val k = f(v)
          if (keyset.value(k)) partitionRange.map(id => (k, id) -> v)
          else List(((k, -1),v)) 
        }}
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

	  def filterHeavy[S:ClassTag](rrdd: RDD[S], hkeys: Broadcast[Set[K]], extract: S => K): (RDD[(K, V)], Map[K, Set[S]])  = {
      val lheavy = lrdd.filterPartitions((i: (K,V)) => hkeys.value(i._1))
      val rheavy = rrdd.extractDistinctHeavyMap(extract, hkeys.value)
      (lheavy, rheavy)
    }

	  def filterLight[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[(K, V)],RDD[(K, S)]) = {
      val llight = lrdd.filterPartitions((i: (K,V)) => !hkeys.value(i._1))
      val rlight = rrdd.filterPartitions((i: (K,S)) => !hkeys.value(i._1))
      (llight, rlight)
    }

    def filterLight[S](rrdd: RDD[S], hkeys: Broadcast[Set[K]], extract: S => K): (RDD[(K, V)], RDD[(K, S)]) = {
      val llight = lrdd.filterPartitions((i: (K,V)) => !hkeys.value(i._1))
      val rlight = rrdd.mapPartitions(it =>
        it.flatMap{ v => {
          val nlbl = extract(v)
          if (!hkeys.value(nlbl)) List((nlbl, v)) else Nil 
         }}, true)
      (llight, rlight)
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[S], fkey: S => K): RDD[(V, S)] = {
      lrdd.cogroup(rrdd.map(v => (fkey(v),v))).flatMap{ pair =>
        for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)}
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = {
      lrdd.cogroup(rrdd).flatMap{ pair =>
        for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)}
    }

    def outerJoinDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, Option[S])] = {
      lrdd.cogroup(rrdd).flatMap{
        case (key, (vs, Seq())) => vs.iterator.map(v => (v, None))
        case (key, (vs, ss)) => 
          for (v <- vs.iterator; s <- ss.iterator) yield (v, Some(s))}
    }

    // left
    def outerJoinDropKey[S:ClassTag](rrdd: RDD[(K, S)], partitioner: Partitioner): RDD[(V, Option[S])] = {
      lrdd.cogroup(rrdd, partitioner).flatMap{
        case (key, (vs, Seq())) => vs.iterator.map(v => (v, None))
        case (key, (vs, ss)) => 
          for (v <- vs.iterator; s <- ss.iterator) yield (v, Some(s))}
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[S], fkey: S => K, partitioner: Partitioner): RDD[(V, S)] = {
      lrdd.cogroup(rrdd.map(v => (fkey(v),v)), partitioner).flatMap{ pair =>
        for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)}
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[(K, S)], partitioner: Partitioner): RDD[(V, S)] = {
      lrdd.cogroup(rrdd, partitioner).flatMap{ pair =>
        for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)}
    }

    def joinSalt[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
      val hk = heavyKeys()
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyBySet(rrdd, hkeys)
        rekey.joinDropKey(dupp)
      } else lrdd.joinDropKey(rrdd)
    }

    def outerJoinSalt[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, Option[S])] = { 
      val hk = heavyKeys()
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyBySet(rrdd, hkeys)
        rekey.outerJoinDropKey(dupp)
      } else lrdd.outerJoinDropKey(rrdd)
    }

    def joinDomain[S:ClassTag](rrdd: RDD[S], extract: S => K): RDD[(V, S)] = {
      val domain = rrdd.map(l => (extract(l), l))
      lrdd.cogroup(domain).flatMap{ pair =>
        for (v <- pair._2._1.iterator; l:S <- pair._2._2.toSet.iterator) yield (v, l) }
    }
 
    def joinDomainSkew[S:ClassTag](rrdd: RDD[S], extract: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (llrdd, ldomain) = lrdd.filterLight(rrdd, hkeys, extract)
        val (hlrdd, hdomain) = lrdd.filterHeavy(rrdd, hkeys, extract)
        val light = llrdd.cogroup(ldomain).flatMap{ pair =>
          for (v <- pair._2._1.iterator; l:S <- pair._2._2.toSet.iterator) yield (v, l)}
        val heavyDomain = hlrdd.sparkContext.broadcast(hdomain).value
        val heavy = hlrdd.mapPartitions(it =>
          it.flatMap{ case (k,v) => heavyDomain get k match {
            case Some(ls) => ls.map(l => (v, l))
            case None => Nil
          }}, true)
        light union heavy
  		}else lrdd.joinDomain(rrdd, extract)
    }
   
    def joinDomainSkewTag[S:ClassTag](rrdd: RDD[S], extract: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val partitioner = new SkewPartitioner(partitions)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys, extract, partitioner)
        rekey.cogroup(dupp, partitioner).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator.flatten) yield (v, s)
        }
  		}else lrdd.joinDomain(rrdd, extract)
    }

    def joinSkew[S:ClassTag](rrdd: RDD[S], fkey: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        // todo push this
        val rkeyed = rrdd.map(r => (fkey(r), r))
        val (llrdd, lrrdd) = lrdd.filterLight(rkeyed, hkeys)
        val (hlrdd, hrrdd) = lrdd.filterHeavy(rkeyed, hkeys)
        val light = llrdd.joinDropKey(lrrdd)
        val heavyRight = hlrdd.sparkContext.broadcast(hrrdd.collect.toMap).value
        val heavy = hlrdd.mapPartitions(it =>
          it.flatMap{ case (k,v) => heavyRight get k match {
            case Some(r) => List((v, r))
            case None => Nil
          }}, true)
        // maybe we don't want to union these yet?
  		  light union heavy
      }else lrdd.joinDropKey(rrdd, fkey)
    }

    def joinSkew[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (llrdd, lrrdd) = lrdd.filterLight(rrdd, hkeys)
        val (hlrdd, hrrdd) = lrdd.filterHeavy(rrdd, hkeys)
        val light = llrdd.joinDropKey(lrrdd)
        val heavyRight = hlrdd.sparkContext.broadcast(hrrdd.collect.toMap).value
        val heavy = hlrdd.mapPartitions(it =>
          it.flatMap{ case (k,v) => heavyRight get k match {
            case Some(r) => List((v, r))
            case None => Nil
          }}, true)
        // maybe we don't want to union these yet?
  		  light union heavy
      }else lrdd.joinDropKey(rrdd)
    }

    def joinSkewTag[S:ClassTag](rrdd: RDD[S], fkey: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val partitioner = new SkewPartitioner(partitions)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys, fkey)
        rekey.joinDropKey(dupp, partitioner)
  		}else lrdd.joinDropKey(rrdd, fkey)
    }

  	def joinSkewTag[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val partitioner = new SkewPartitioner(partitions)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys)
	      rekey.joinDropKey(dupp, partitioner)
  		}else lrdd.joinDropKey(rrdd) 
    }

  	def outerJoinSkew[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, Option[S])] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val partitioner = new SkewPartitioner(partitions)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys)
	      rekey.outerJoinDropKey(dupp, partitioner)
  		}else lrdd.outerJoinDropKey(rrdd) 
    }

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
 
	 def groupBySkew(): RDD[(K, Iterable[V])] = {
      val hk = heavyKeys()
      if (hk.nonEmpty){
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val rekey = lrdd.mapPartitionsWithIndex( (index, it) =>
          it.zipWithIndex.map{ case ((k,v), i) => 
            (k, { if (hkeys.value(k)) i % partitions else index }) -> v
          }, true)
        rekey.groupByKey(new SkewPartitioner(partitions)).map{ case ((k, _), v) => k -> v }
      }
      else groupByLabel() 
    }

  
  }

}
