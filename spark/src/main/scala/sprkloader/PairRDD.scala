package sprkloader

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast

object PairRDDOperations {

  implicit class PairRDDOps[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]) extends Serializable {
    
    val partitions = lrdd.getNumPartitions

    def empty: RDD[(K,V)] = lrdd.sparkContext.emptyRDD[(K,V)]

    // skew function
    def toHeavyMap(hkeys: Broadcast[Set[K]]): Map[K, Vector[V]] = {
      lrdd.flatMap{
        case (k,v) => if (hkeys.value(k)) Vector((k, Vector(v))) else Vector()
      }.reduceByKey(_++_).collect.toMap
    }

    def broadcastJoin[S: ClassTag](rrdd: Broadcast[Map[K, Vector[S]]]): RDD[(K, (V, S))] = {
      lrdd.mapPartitions(it => 
        it.flatMap{ case (k,v) => rrdd.value get k match {
          case Some(ls) => ls.map(s => (k, (v, s)))
          case None => Vector()
        }})
    }

    def broadcastOuterJoin[S: ClassTag](rrdd: Broadcast[Map[K, Vector[S]]]): RDD[(K, (V, Option[S]))] = {
      lrdd.mapPartitions(it => 
        it.flatMap{ case (k,v) => rrdd.value get k match {
          case Some(ls) => ls.map(s => (k, (v, Some(s))))
          case None => Vector((k, (v, None)))
        }})
    }

    def broadcastJoinDropKey[S: ClassTag](rrdd: Broadcast[Map[K, Vector[S]]]): RDD[(V, S)] = {
      lrdd.mapPartitions(it => 
        it.flatMap{ case (k,v) => rrdd.value get k match {
          case Some(ls) => ls.map(s => v -> s)
          case None => Vector()
        }})
    }

    def broadcastJoinDomain(rrdd: Broadcast[Set[K]]): RDD[(K, V)] = {
      lrdd.mapPartitions(it => it.filter{ case (k,v) => rrdd.value(k) }, true)
    }

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

    def rightCoGroupDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(S, Vector[V])] = {
      lrdd.cogroup(rrdd).flatMap{
        case (_, (e1, e2)) => e2.toVector.map{ v => v -> e1.toVector }
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

    def reduce(f: (V,V) => V): RDD[(K,V)] = lrdd.partitioner match {
      case Some(p) => lrdd.reduceByKey(f)
      case _ => lrdd.reduceByKey(new HashPartitioner(partitions), f)
    }

    // def nestGroup: RDD[(K,Vector[V])] = {
    //   val accum1 = (acc: Vector[V], v: V) => acc :+ v
    //   val accum2 = (acc1: Vector[V], acc2: Vector[V]) => acc1 ++ acc2
    //   lrdd.partitioner match {
    //     case Some(p) => lrdd.aggregateByKey(Vector.empty[V])(accum1, accum2)
    //     case _ => lrdd.aggregateByKey(Vector.empty[V], new HashPartitioner(partitions))(accum1, accum2)
    //   }
    // }
    def group(f: (V, V) => V): RDD[(K, V)] = reduce(f)
  }

}