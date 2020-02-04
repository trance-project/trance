package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import UtilPairRDD._

object SkewDictRDD {
  
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

}
