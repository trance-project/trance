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

  }

}