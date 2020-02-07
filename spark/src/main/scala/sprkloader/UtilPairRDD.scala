package sprkloader

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import SkewPairRDD._

object UtilPairRDD {

  implicit class RDDUtils[W: ClassTag](lrdd: RDD[W]) extends Serializable {

    val threshold = Config.threshold

    def filterPartitions(cond: W => Boolean): RDD[W] = {
      lrdd.mapPartitions(it => 
        it.flatMap{ v => if (cond(v)) List(v) else Nil }, true)
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
