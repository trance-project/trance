package sprkloader

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag

object UtilPairRDD {

  implicit class RDDUtils[W: ClassTag](lrdd: RDD[W]) extends Serializable {

    def filterPartitions(cond: W => Boolean): RDD[W] = {
      lrdd.mapPartitions(it => 
        it.flatMap{ v => if (cond(v)) List(v) else Nil }, true)
    }

    def heavyKeysByPartition[R](threshold: Int = 1000): Map[(R, Int), Int] = {
      val hkeys = lrdd.mapPartitionsWithIndex( (index, it) =>
        Util.countDistinctByPartition(it.asInstanceOf[Iterator[(R,Any)]], index).filter(_._2 > threshold).iterator,true)
      hkeys.collect.toMap
    }
  
  }

}
