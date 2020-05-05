package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import UtilPairRDD._

object UtilSkewDistribution {
  
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

}
