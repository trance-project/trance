package sparkutils.rdd

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import TopRDD._
import UtilPairRDD._

object SkewTopRDD{

  implicit class SkewTopFunctions[K:ClassTag](lrdd: (RDD[K], RDD[K])) extends Serializable {
    
    val light = lrdd._1
    val heavy = lrdd._2

    val partitions = light.getNumPartitions
    
    def union: RDD[K] = light.unionPartitions(heavy)

    // def heavyKeys: Set[K] = Set.empty[K]

    def zipWithIndex: (RDD[(K, Long)], RDD[(K, Long)]) = (light.zipWithIndex, heavy.zipWithIndex)

    def map[S:ClassTag](f: K => S): (RDD[S], RDD[S]) = 
      (light.map(k => f(k)), heavy.map(k => f(k)))

    def mapPartitions[S:ClassTag](f: Iterator[K] => Iterator[S], preserve: Boolean = false): (RDD[S], RDD[S]) = 
      (light.mapPartitions(f, preserve), //it => it.map(k => f(k)), preserve), 
       heavy.mapPartitions(f, preserve))//it => it.map(k => f(k)), preserve))

    def filter(p: K => Boolean): (RDD[K], RDD[K]) = 
      (light.filter(k => p(k)), heavy.filter(k => p(k)))

    def flatMap[S:ClassTag](f: K => Vector[S]): (RDD[S], RDD[S]) = {
      (light.flatMap(f), heavy.flatMap(f))
    }

    def createDomain[L: ClassTag](f: K => L): (RDD[L], RDD[L]) = 
      (light.createDomain(f), heavy.createDomain(f))

    def cache: Unit = {
      light.cache
      heavy.cache
    }

    def evaluate: Unit = {
      light.evaluate
      heavy.evaluate
    }

    def print: Unit = {
      println("light")
      light.print
      println("heavy")
      heavy.print
    }

  }

}