package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag

object SkewPairRDD {
  
  implicit class SkewPairRDDFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]) extends Serializable {

    val reducers = Config.minPartitions 

    def heavyKeys(): Set[K] = {
      lrdd.mapPartitions{ it => 
        Util.countDistinct(it).filter(_._2 > 1000).iterator }
      .reduceByKey(_ + _)
      .filter(_._2 >= reducers)
      .keys.collect.toSet
    }
	
	  def balanceLeft[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val lrekey = lrdd.mapPartitions{ it =>
        it.zipWithIndex.map{ case ((k,v), i) => 
          (k, { if (hkeys.value(k)) i % reducers else 0 }) -> v
        }
      }
      val rdupp = rrdd.flatMap{ case (k,v) =>
        Range(0, {if (hkeys.value(k)) reducers else 1 }).map(id => (k, id) -> v) 
      }
      (lrekey, rdupp)
    }

    def cogroupSkewLeft[S](rrdd: RDD[(K, S)]): RDD[(K, (Iterable[V], Iterable[S]))] = { 
      val hk = heavyKeys
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
        rekey.cogroup(dupp).map{ case ((k, _), v) => k -> v }
      }
      else lrdd.cogroup(rrdd) 
    }

    def joinSkewLeft[S](rrdd: RDD[(K, S)]): RDD[(K, (V, S))] = { 
      val hk = heavyKeys
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
        rekey.join(dupp).map{ case ((k, _), v) => k -> v }
      }
      else lrdd.join(rrdd) 
    }

    def outerLookup[S >: Null](rrdd: RDD[(K,S)]): RDD[(V,S)] = {
      lrdd.cogroup(rrdd).flatMap{ pair =>
        if (pair._2._2.isEmpty) {
          pair._2._1.iterator.map(k => (k, null))
        } else {
          for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k, w)
        }
      }
    }

    def lookup[S](rrdd: RDD[(K,S)]): RDD[(V,S)] = {
      lrdd.cogroup(rrdd).flatMap{ pair =>
        for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k, w)
      }
    }

    def lookupSkewLeft[S](rrdd: RDD[(K, S)]): RDD[(V, S)] = {
      val hk = heavyKeys
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
	 	rekey.cogroup(dupp).flatMap{ pair =>
          for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k, w)
        }
      } else lrdd.lookup(rrdd) 
    }

    def outerLookupSkewLeft[S >: Null](rrdd: RDD[(K, S)]): RDD[(V, S)] = {
      val hk = heavyKeys
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
        rekey.cogroup(dupp).flatMap{ pair =>
          if (pair._2._2.isEmpty) {
            pair._2._1.iterator.map{ case (k,_) => (k.asInstanceOf[V], null) }
          } else {
            for ((k, _) <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k.asInstanceOf[V], w)
          }
        }
      } else lrdd.outerLookup(rrdd) 
    }

    def groupByLabel(): RDD[(K, Iterable[V])] = {
      val groupBy = (i: Iterator[(K,V)]) => {
        val hm = HashMap[K, Vector[V]]()
        i.foreach{ v =>
          hm(v._1) = hm.getOrElse(v._1, Vector()) :+ v._2
        }
        hm.iterator
      }
      lrdd.mapPartitions(groupBy)
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
      lrdd.mapPartitions(groupBy)
    }
  
    def groupBySkew(): RDD[(K, Iterable[V])] = {
      val hk = heavyKeys 
      if (hk.nonEmpty){
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val rekey = lrdd.mapPartitions{ it =>
          it.zipWithIndex.map{ case ((k,v), i) => 
            (k, { if (hkeys.value(k)) i % reducers else 0 }) -> v
          }
        }
        rekey.groupByKey().map{ case ((k, _), v) => k -> v }
      }
      else groupByLabel() 
    }

  
  }

}
