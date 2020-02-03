package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import UtilPairRDD._

object SkewPairRDD {
  
  implicit class SkewDictFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,Iterable[V])]) extends Serializable {

    val reducers = Config.minPartitions 

    def heavyKeys(threshold: Int = 1000): Set[K] = {
      val hkeys = lrdd.mapPartitions( it => 
        it.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
          { acc(c._1) += c._2.size; acc } ) .filter(_._2 > threshold).iterator,
      true).reduceByKey(_ + _)
      
      if (reducers > threshold) hkeys.filter(_._2 >= reducers).keys.collect.toSet
      else hkeys.keys.collect.toSet
    }
 
 	def balanceLeft[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[((K, Int), Iterable[V])], RDD[((K, Int), S)]) = {
      val lrekey = lrdd.mapPartitionsWithIndex( (index, it) =>
        it.zipWithIndex.map{ case ((k,v), i) => 
          (k, { if (hkeys.value(k)) i % reducers else 0 }) -> v
        }, true)
      val rdupp = rrdd.flatMap{ case (k,v) =>
        Range(0, {if (hkeys.value(k)) reducers else 1 }).map(id => (k, id) -> v) 
      }
      (lrekey, rdupp)
    }

    def lookup[S](rrdd: RDD[(K,S)]): RDD[(S, Iterable[V])] = {
      lrdd.cogroup(rrdd).flatMap{ pair =>
        for (b <- pair._2._1.iterator; l <- pair._2._2.iterator) yield (l, b)
      }
    }

    def lookupSkewLeft[S](rrdd: RDD[(K, S)]): RDD[(S, Iterable[V])] = {
      val hk = heavyKeys()
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
	 	rekey.cogroup(dupp).flatMap{ pair =>
          for (b <- pair._2._1.iterator; l <- pair._2._2.iterator) yield (l, b)
        }
      } else lrdd.lookup(rrdd) 
    }

  }

  // ((key, partitionIndex), count)
  implicit class SkewDistributionFunctions[K: ClassTag](mrdd: RDD[((K, Int), Int)]) {
	def getKeys():Set[K] = mrdd.map{ case ((key, pid), cnt) => key }.collect.toSet
	def isBalanced(threshold: Double = 0.95): Boolean = {
		// this is wrong should be counts by key
		val cnts = mrdd.map{ case ((key, pid), cnt) => cnt }
		val min = cnts.min
		val max = cnts.max
		val skew = min/max.toDouble
		println(s"found this skew $skew $min $max")
		true
		//skew >= threshold
	}
  }

  implicit class SkewPairRDDFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]) extends Serializable {

    val reducers = Config.minPartitions
	val partitions = lrdd.getNumPartitions

    def heavyKeys(threshold: Int = 1000): Set[K] = {
      val hkeys = lrdd.mapPartitions( it => 
        Util.countDistinct(it).filter(_._2 > threshold).iterator,true)
      if (reducers > threshold) hkeys.filter(_._2 >= reducers).keys.collect.toSet
      else hkeys.keys.collect.toSet
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
	
    def rekeyByIndex[S](rrdd: RDD[(K, S)], keyset: Broadcast[Set[K]]): (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      // tell each heavy partition to stay where it is
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)

      val partitionRange = Range(0, partitions)
      val dupp = 
        rrdd.flatMap{ case (k,v) => 
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

    def joinDropKey[S](rrdd: RDD[(K,S)], partitioner: Option[Partitioner] = None): RDD[(V,S)] = {
	  val cgrp = partitioner match {
		case Some(p) => lrdd.cogroup(rrdd, p)
		case _ => lrdd.cogroup(rrdd)
	  }
	  cgrp.flatMap{ pair =>
        for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
      }
    }

    def joinSkewLeftOld[S](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
      val hk = heavyKeys()
      val hkm = lrdd.heavyKeysByPartition[K]()
      if (hk.nonEmpty) {
       val hkeys = lrdd.sparkContext.broadcast(hk)
       val (rekey, dupp) = lrdd.rekeyBySet(rrdd, hkeys)
       //println("before join")
       //hkm.foreach(println(_))
       val result = rekey.cogroup(dupp).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
       }
       //println("after join")
       //result.heavyKeysByPartition[K]().foreach(println(_))
       result 
      }
      else joinDropKey(rrdd)
    }

	// simple check on skew-distribution
	// currently assumes join on pk of right 
	def joinSkewLeft[S](rrdd: RDD[(K, S)], checkDistribution: Boolean = false): RDD[(V, S)] = { 
      val (skewedKeys, hk) = 
	  	if (checkDistribution) {
			val partCnts = lrdd.heavyKeysByPartition[K]()
			val hk = partCnts.getKeys()
			(partCnts.isBalanced() && hk.nonEmpty, hk)
		}else{
			val hk = heavyKeys()
			(hk.nonEmpty, hk)
		} 
	  	if (skewedKeys) {
       		val hkeys = lrdd.sparkContext.broadcast(hk)
       		val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys)
       		rekey.joinDropKey(dupp, Some(new SkewPartitioner(partitions)))
		}else joinDropKey(rrdd)
    }

	def joinSkewLeftPrintDist[S](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
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

class SkewPartitioner(override val numPartitions: Int) extends Partitioner {

  def defaultPartitionAssignment(key:Any): Int = {
    val hcmod = key.hashCode % numPartitions
    hcmod + (if (hcmod < 0) numPartitions else 0)
  }
  
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(Any, Int)]
    if (k._2 != -1) k._2
    else defaultPartitionAssignment(key)
  }

  override def equals(that: Any): Boolean = {
    that match {
      case sp:SkewPartitioner => sp.numPartitions == numPartitions
      case _ => false
    }
  }

}
