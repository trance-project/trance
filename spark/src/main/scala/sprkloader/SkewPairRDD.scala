package sprkloader

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.HashMap
import PairRDDOperations._
import UtilPairRDD._

object SkewPairRDD {

  implicit class SkewPairRDDFunctions[K: ClassTag, V: ClassTag](lrdd: (RDD[(K,V)], RDD[(K, V)])) extends Serializable {
    
    val threshold = Config.threshold
    val partitions = lrdd._1.getNumPartitions

    def unioned(): RDD[(K,V)] = lrdd._1.unionPartitions(lrdd._2)

    def unionFilter(cond: ((K,V)) => Boolean): RDD[(K,V)] = lrdd._1.unionFilterPartitions(lrdd._2, cond)
  
    def heavyKeysStatic(threshold: Int = threshold): Set[K] = {
      val lunion = lrdd.unioned()
      val keys = lunion.mapPartitions( it => 
        Util.countDistinct(it).filter(_._2 > threshold).iterator,true).keys.collect.toSet
      keys
    }
    
    def heavyKeys(): (RDD[(K,V)], Set[K]) = {
      val lunion = lrdd.unioned()
      val samples = lunion.sample(false, .05)
      val thresh = (samples.countApprox(1).getFinalValue().low/partitions)*0.05
      if (thresh < 1) (unioned, Set.empty[K])
      else {
        (lunion, samples.mapPartitionsWithIndex((index, it) => {
          Util.countDistinct(it).filter(_._2 > thresh).iterator
        }).keys.collect.toSet)
      }
    }

    def cogroup[S:ClassTag](rrdd: (RDD[(K,S)], RDD[(K,S)])): 
      (RDD[(V, Iterable[S])], RDD[(V,Iterable[S])], Broadcast[Set[K]]) = {
      val (lunion, hk) = heavyKeys()
      val hkeys = lunion.sparkContext.broadcast(hk)
      if (hkeys.value.nonEmpty){
        val rlight = rrdd.unionFilter(i => !hkeys.value(i._1))
        val llight = lunion.filterPartitions(i => !hkeys.value(i._1))
        val light = llight.cogroup(rlight).flatMap{
          case (_, (left, right)) => left.map(l => (l, right))
        }

        val rheavy = rrdd.unionFilter(i => hkeys.value(i._1)).groupByKey().collect.toMap
        val lheavy = unioned.filterPartitions(i => hkeys.value(i._1))
        val heavyRights = lheavy.sparkContext.broadcast(rheavy)
        val heavy = lheavy.mapPartitions(it =>
          it.map{ case (k,v) => heavyRights.value get k match {
            case Some(ls) => (v, ls)
            case None => (v, Iterable())
          }})
        (light, heavy, hkeys)
      } else {
        val cg = lunion.cogroup(rrdd.unioned()).flatMap{
          case (_, (left, right)) => left.map(l => (l, right))
        }
        (cg, lunion.sparkContext.emptyRDD[(V,Iterable[S])], hkeys)
      }    
    }

    def cogroupDomain(dom: (RDD[K], RDD[K])): (RDD[(K, Iterable[V])], RDD[(K, Iterable[V])], Broadcast[Set[K]]) = {
      val (lunion, hk) = lrdd.heavyKeys()
      val hkeys = lunion.sparkContext.broadcast(hk)
      val domain = dom._1.unionPartitions(dom._2)
      if (hkeys.value.nonEmpty){
        val domainLight = domain.flatMap( l => if (!hkeys.value(l)) List((l -> 1)) else Nil )
        val light = lunion.filterPartitions((i: (K,V)) => !hkeys.value(i._1))

        val ldict = light.cogroup(domainLight, new HashPartitioner(partitions)).mapPartitions(it =>
          it.flatMap{ case (lbl, (vs, _)) => if (vs.nonEmpty) List((lbl -> vs)) else Nil}, true)

        val hdomain = domain.filter(l => hkeys.value(l)).collect.toSet
        val heavy = lunion.filterPartitions((i: (K,V)) => hkeys.value(i._1))
        val heavyDomain = heavy.sparkContext.broadcast(hdomain)
      
        // if there are a lot of heavy keys this method is not ideal
        val groupBy = (i: Iterator[(K,V)], hks: Set[K]) => {
          val hm = HashMap[K, Iterable[V]]()
            i.foreach{ v =>
              if (hks(v._1)) hm(v._1) = hm.getOrElse(v._1, Iterable()) ++ Iterable(v._2)
            }
            hm.iterator
          }
          val hdict = heavy.mapPartitions(it => groupBy(it, heavyDomain.value))
          (ldict, hdict, hkeys)
         }else
          (lunion.cogroupDomain(domain), lunion.sparkContext.emptyRDD[(K, Iterable[V])], hkeys)
    }



  }

}