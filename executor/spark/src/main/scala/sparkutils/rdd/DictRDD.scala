package sparkutils.rdd

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner

object DictRDDOperations {

  implicit class DictRDDOps[K: ClassTag, V: ClassTag](lrdd: RDD[(K,Vector[V])]) extends Serializable {

    val partitions = lrdd.getNumPartitions

    def empty: RDD[(K,Vector[V])] = lrdd.sparkContext.emptyRDD[(K,Vector[V])]

    def createDomain[C: ClassTag](f: V => C): RDD[C] = {
       lrdd.flatMap{
         case (lbl, bag) => bag.foldLeft(Set.empty[C])(
          (acc, v) => acc + f(v)
         )
       }
    }

    /**
      leftOuterJoin + group by, drops the key and therefore the 
      partitioner in the process; this is unshredding
    **/
    def cogroupDropKey[S:ClassTag](rrdd: RDD[(K, Vector[S])]): RDD[(V, Vector[S])] = {
      lrdd.cogroup(rrdd).flatMap{
        case (_, (e1, e2)) => e1.toVector.flatten.map{ v => v -> e2.toVector.flatten }
      }
    }

    /**
      rightOuterJoin + group by, drops the key; this is unshredding
    **/
    def rightCoGroupDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(S, Vector[V])] = {
      lrdd.cogroup(rrdd).flatMap{
        case (_, (e1, e2)) => e2.toVector.map{ v => v -> e1.toVector.flatten }
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
              it.flatMap{ case (lbl, (vs, _)) => vs.toVector.flatten.map(v => lbl -> v) }, true)
          case None =>
            lrdd.cogroup(domain, new HashPartitioner(partitions)).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => vs.toVector.flatten.map(v => lbl -> v) }, true)
      }
    }  

    /**
      CoGroup a dictionary with a single element domain (ie. Label(childLabel))
      This is a standard lookup
    **/
    def cogroupDomain(rrdd: RDD[K]): RDD[(K, Vector[V])] = {
      val domain = rrdd.map(l => l -> 1)
      lrdd.partitioner match {
          case Some(p) => 
            lrdd.cogroup(domain).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => {
                val fvs = vs.toVector.flatten
                if (fvs.nonEmpty) Vector((lbl -> fvs)) else Nil}}, true)
          case None =>
            lrdd.cogroup(domain, new HashPartitioner(partitions)).mapPartitions(it =>
              it.flatMap{ case (lbl, (vs, _)) => {
                val fvs = vs.toVector.flatten
                if (fvs.nonEmpty) Vector((lbl -> fvs)) else Nil}}, true)
        }
    }

  }
}