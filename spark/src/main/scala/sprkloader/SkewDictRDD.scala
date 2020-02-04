package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import SkewPairRDD._
import UtilPairRDD._

object SkewDictRDD {
  
  implicit class SkewDictFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,Iterable[V])]) extends Serializable {

    val reducers = Config.minPartitions

	def labelSizeByPartition(): RDD[((K, Int), Int)] = {
		lrdd.mapPartitionsWithIndex((index, it) => 
			it.map{ case (lbl, bag) => (lbl, index) -> bag.size }, true)	
	}
	
	def lookup[S](rrdd: RDD[(K,S)]): RDD[(S, Iterable[V])] = {
      lrdd.cogroup(rrdd).flatMap{ pair =>
        for (b <- pair._2._1.iterator; l <- pair._2._2.iterator) yield (l, b)
      }
    }

    /**
		Given a balanced dictionary, doing a lookup that maintains partitioning of 
		parent label (local filtering) will result in skewed partitions. 
	  **/
	def lookupSkewLeft[S](rrdd: RDD[(K, S)]): RDD[(S, Iterable[V])] = {
      val hk = lrdd.heavyKeys()
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.rekeyBySet(rrdd, hkeys)
    	rekey.cogroup(dupp).flatMap{ pair =>
          for (b <- pair._2._1.iterator; l <- pair._2._2.iterator) yield (l, b)
        }
      } else lrdd.lookup(rrdd) // maybe this could do local filtering
    }

  }

}
