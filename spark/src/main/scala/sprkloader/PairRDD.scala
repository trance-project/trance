package sprkloader

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner

object PairRDDOperations {

  implicit class PairRDDOps[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]) extends Serializable {
    
    val partitions = lrdd.getNumPartitions

    def empty: RDD[(K,V)] = lrdd.sparkContext.emptyRDD[(K,V)]

    /** 
      Join two keyed RDDs dropping the key and therefore the 
      partitioner in the process
    **/
    def joinDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = {
      lrdd.cogroup(rrdd).flatMap{ 
        case (key, (vs, ss)) => 
          for (v <- vs.iterator; s <- ss.iterator) yield (v, s)}
    }
    
    /**
      leftOuterJoin + group by, drops the key and therefore the 
      partitioner in the process
    **/
    def cogroupDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, Vector[S])] = {
      lrdd.cogroup(rrdd).flatMap{
        case (_, (e1, e2)) => e1.map{ v => v -> e2.toVector }
      }
    }

    /**
      Join a top-level bag (dict) with a single element domain (ie. Label(childLabel))
      this does not drop the key, preserves/redefines partitioning, and handles domain
      deduplication
    **/
    def joinDomain(rrdd: RDD[K]): RDD[(K, V)] = {
      val domain = rrdd.map(l => l -> 1)
      lrdd.partitioner match {
          case Some(p) => 
            lrdd.cogroup(domain).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => vs.map(v => lbl -> v) }, true)
          case None =>
            lrdd.cogroup(domain, new HashPartitioner(partitions)).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => vs.map(v => lbl -> v) }, true)
        }
    }

    /**
      CoGroup a top-level bag (dict) with a single element domain (ie. Label(childLabel))
      This is joinDomain + group by (pushed group by before the join)
    **/
    def cogroupDomain(rrdd: RDD[K]): RDD[(K, Vector[V])] = {
      val domain = rrdd.map(l => l -> 1)
      lrdd.partitioner match {
          case Some(p) => 
            lrdd.cogroup(domain).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => 
                if (vs.nonEmpty) Vector((lbl -> vs.toVector)) else Nil}, true)
          case None =>
            lrdd.cogroup(domain, new HashPartitioner(partitions)).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => 
                if (vs.nonEmpty) Vector((lbl -> vs.toVector)) else Nil}, true)
        }
    }

  }

}