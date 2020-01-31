package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner

object SkewPairRDD {
  
  implicit class SkewPairDictFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,Iterable[V])]) extends Serializable {

    val reducers = Config.minPartitions 

    def heavyKeys(): Set[K] = {
      lrdd.mapPartitions( it => 
        it.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
          { acc(c._1) += c._2.size; acc } ) .filter(_._2 > 1000).iterator,
      true).reduceByKey(_ + _)/**.filter(_._2 >= reducers)**/.keys.collect.toSet
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
      val hk = heavyKeys
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
	 	    rekey.cogroup(dupp).flatMap{ pair =>
          for (b <- pair._2._1.iterator; l <- pair._2._2.iterator) yield (l, b)
        }
      } else lrdd.lookup(rrdd) 
    }

  }

  implicit class SkewPairRDDFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]) extends Serializable {

    val reducers = Config.minPartitions 

    def heavyKeys(): Set[K] = {
      lrdd.mapPartitions( it => 
        Util.countDistinct(it).filter(_._2 > 1000).iterator, true )
      .reduceByKey(_ + _)
      .filter(_._2 >= reducers)
      .keys.collect.toSet
    }
	
	def balanceLeft[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val lrekey = lrdd.mapPartitions( it =>
        it.zipWithIndex.map{ case ((k,v), i) => 
          (k, { if (hkeys.value(k)) i % reducers else 0 }) -> v
        }, true)
      val rdupp = rrdd.flatMap{ case (k,v) =>
        Range(0, {if (hkeys.value(k)) reducers else 1 }).map(id => (k, id) -> v) 
      }
      (lrekey, rdupp)
    }

    def cogroupSkewLeft[S](rrdd: RDD[(K, S)]): RDD[(Iterable[V], Iterable[S])] = { 
      val hk = heavyKeys
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
        rekey.cogroup(dupp).map{ case ((k, _), v) => v }
      }
      else lrdd.cogroup(rrdd).map{ case (k, v) => v } 
    }

    def joinSkewLeft[S](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
      val hk = heavyKeys
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
        rekey.cogroup(dupp).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
        }
      }
      else lrdd.cogroup(rrdd).flatMap{ pair =>
        for (v <- pair._2._1.iterator; s <- pair._2._2.iterator) yield (v, s)
      }
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
      val hk = heavyKeys 
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

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(Int, Int)]
    return k._2
  }

  override def equals(that: Any): Boolean = {
    that match {
      case sp:SkewPartitioner => sp.numPartitions == numPartitions
      case _ => false
    }
  }

}
