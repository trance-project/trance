package sprkloader

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import SkewPairRDD._
import org.apache.spark.Partitioner

// rename
object UtilPairRDD {

  implicit class RDDUtils[W: ClassTag](lrdd: RDD[W]) extends Serializable {

    val threshold = Config.threshold

    def unionPartitions(rrdd: RDD[W], preserve: Boolean = true): RDD[W] =
      if (rrdd.getNumPartitions == 0) lrdd
      else lrdd.zipPartitions(rrdd, preserve)((l: Iterator[W], r: Iterator[W]) => l ++ r)
      //lrdd union rrdd

    def unionFilterPartitions(rrdd: RDD[W], cond: W => Boolean): RDD[W] = 
      if (rrdd.getNumPartitions == 0) lrdd.filterPartitions(cond)
      else lrdd.zipPartitions(rrdd, true)((l: Iterator[W], r: Iterator[W]) => 
          l.filter(cond) ++ r.filter(cond))
      //lrdd.filterPartitions(cond) union rrdd.filterPartitions(cond)

    def filterPartitions(cond: W => Boolean): RDD[W] = {
      lrdd.filter(cond)
      // .mapPartitions(it => 
      //   it.flatMap{ v => if (cond(v)) List(v) else Nil }, preserve)
    }

    def heavyKeysByPartition[R: ClassTag](threshold: Int = 1000): RDD[((R, Int), Int)] = {
      lrdd.mapPartitionsWithIndex( (index, it) =>
        Util.countDistinctByPartition(it.asInstanceOf[Iterator[(R,Any)]], 
			index).filter(_._2 > threshold).iterator,true)
    }

    def rekeyByPartition(): RDD[(Int, W)] = {
      lrdd.mapPartitionsWithIndex( (index, it) => 
        it.map(v => (index, v)), true)
    }
 
    def rekeyByIndex[K](f: W => K): RDD[((K, Int), W)] = {
      lrdd.mapPartitionsWithIndex( (index, it) =>
        it.map(v => ((f(v), index), v)), true)
    }
    
    // map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1) (distinct)
    def duplicateDistinct[K: ClassTag](fkey: W => K, partitioner: Partitioner, 
      hkeys: Broadcast[Set[K]]): RDD[((K, Int), Set[W])] = {
        val accum1 = (acc: Set[W], w: W) => acc + w
        val accum2 = (acc1: Set[W], acc2: Set[W]) => acc1 ++ acc2
        val partitions = Range(0, partitioner.numPartitions)
        lrdd.flatMap(v => {
          val k = fkey(v)
          if (hkeys.value(k)) partitions.map(i => ((k, i), v)) else List(((k, -1), v))
        }).aggregateByKey(Set.empty[W], partitioner)(accum1, accum2) 
    }

    def duplicate[K](f: W => K, numPartitions: Int, hkeys: Broadcast[Set[K]]): RDD[((K, Int), W)] = {
      val range = Range(0, numPartitions)
      lrdd.flatMap( v => { 
        val k = f(v)
        if (hkeys.value(k)) range.map(id => ((k, id),v))
        else List(((k, -1), v))
      })
    }
  }

}
