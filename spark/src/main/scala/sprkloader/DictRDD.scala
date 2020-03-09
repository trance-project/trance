package sprkloader

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner

object DictRDDOperations {

  implicit class SkewDictFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,Iterable[V])]) extends Serializable {

    val partitions = lrdd.getNumPartitions

    def createDomain[L: ClassTag](f: V => L): RDD[L] = {
      lrdd.flatMap{
        case (lbl, bag) => bag.foldLeft(Set.empty[L])(
          (acc, v) => acc + f(v))
      }
    }

    /**
      leftOuterJoin + group by, drops the key and therefore the 
      partitioner in the process; this is unshredding
    **/
    def cogroupDropKey[S:ClassTag](rrdd: RDD[(K, Iterable[S])]): RDD[(V, Iterable[S])] = {
      lrdd.cogroup(rrdd).flatMap{
        case (_, (e1, e2)) => e1.flatten.map{ v => v -> e2.flatten }
      }
    }

    /**
      rightOuterJoin + group by, drops the key; this is unshredding
    **/
    def rightCoGroupDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(S, Iterable[V])] = {
      lrdd.cogroup(rrdd).flatMap{
        case (_, (e1, e2)) => e2.map{ v => v -> e1.flatten }
      }
    }

    /** 
      Lookup from a single element domain on a dictionary (ie. Label(childLabel))
      This is join Domain + unnest (merge unnest with the join)
    **/
    def lookupIteratorDomain(rrdd: RDD[K]): RDD[(K, V)] = {
      val domain = rrdd.map(l => l -> 1)
      lrdd.partitioner match {
          case Some(p) => 
            lrdd.cogroup(domain).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => vs.flatten.map(v => lbl -> v) }, true)
          case None =>
            lrdd.cogroup(domain, new HashPartitioner(partitions)).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => vs.flatten.map(v => lbl -> v) }, true)
      }
    }  

    /**
      CoGroup a dictionary with a single element domain (ie. Label(childLabel))
      This is a standard lookup
    **/
    def cogroupDomain(rrdd: RDD[K]): RDD[(K, Iterable[V])] = {
      val domain = rrdd.map(l => l -> 1)
      lrdd.partitioner match {
          case Some(p) => 
            lrdd.cogroup(domain).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => {
                val fvs = vs.flatten
                if (fvs.nonEmpty) List((lbl -> fvs)) else Nil}}, true)
          case None =>
            lrdd.cogroup(domain, new HashPartitioner(partitions)).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => {
                val fvs = vs.flatten
                if (fvs.nonEmpty) List((lbl -> fvs)) else Nil}}, true)
        }
    }

  }
}