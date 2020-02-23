package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import UtilPairRDD._
import UtilSkewDistribution._
import DomainRDD._

object SkewPairRDD {

  implicit class SkewPairRDDFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]) extends Serializable {

    val reducers = Config.minPartitions
	  val threshold = Config.threshold
    val partitions = lrdd.getNumPartitions
 	
	  def heavyKeys(threshold: Int = threshold): Set[K] = {
      val hkeys = lrdd.mapPartitions( it => 
        Util.countDistinct(it).filter(_._2 > threshold).iterator,true)
      //if (reducers > threshold) hkeys.filter(_._2 >= reducers).keys.collect.toSet
      //else hkeys.keys.collect.toSet
      hkeys.keys.collect.toSet
    }

    /** SPLIT OPS **/
    
    // without a known partitioner
    def joinSplit[S:ClassTag](rrdd: RDD[(K,S)]): (RDD[(V, S)], RDD[(V,S)], Set[K]) = {
      val hk = lrdd.heavyKeys()
      if (hk.nonEmpty){
        val hkeys = lrdd.sparkContext.broadcast(hk).value
        val rlight = rrdd.filterPartitions((i: (K, S)) => !hkeys(i._1))
        val llight = lrdd.filterPartitions((i: (K, V)) => !hkeys(i._1))
        val light = llight.joinDropKey(rlight, new HashPartitioner(partitions))

        val rheavy = rrdd.filterPartitions((i: (K, S)) => hkeys(i._1)).groupByKey().collect.toMap
        val heavyRights = lrdd.sparkContext.broadcast(rheavy).value
        val lheavy = lrdd.filterPartitions((i: (K, V)) => hkeys(i._1))
        val heavy = lheavy.mapPartitions(it =>
          it.flatMap{ case (k,v) => heavyRights get k match {
            case Some(ls) => ls.map(s => (v,s))
            case None => Nil
          }}, true)
        (light, heavy, hkeys)
      }else (lrdd.joinDropKey(rrdd, new HashPartitioner(partitions)), lrdd.sparkContext.emptyRDD[(V,S)], Set.empty[K])
    }

    def joinSplit[S:ClassTag](heavy: RDD[(K,V)], rrdd: RDD[(K,S)]): (RDD[(V, S)], RDD[(V,S)], Set[K]) = {
      val unioned = lrdd.unionPartitions(heavy)
      val hk = unioned.heavyKeys()
      if (hk.nonEmpty){
        val hkeys = unioned.sparkContext.broadcast(hk).value
        val rlight = rrdd.filterPartitions((i: (K, S)) => !hkeys(i._1))
        val llight = unioned.filterPartitions((i: (K, V)) => !hkeys(i._1))
        val light = llight.joinDropKey(rlight, new HashPartitioner(partitions))

        val rheavy = rrdd.filterPartitions((i: (K, S)) => hkeys(i._1)).groupByKey().collect.toMap
        val heavyRights = unioned.sparkContext.broadcast(rheavy).value
        val lheavy = unioned.filterPartitions((i: (K, V)) => hkeys(i._1))
        val heavy = lheavy.mapPartitions(it =>
          it.flatMap{ case (k,v) => heavyRights get k match {
            case Some(ls) => ls.map(s => (v,s))
            case None => Nil
          }}, true)
        (light, heavy, hkeys)
      }else (unioned.joinDropKey(rrdd, new HashPartitioner(partitions)), lrdd.sparkContext.emptyRDD[(V,S)], Set.empty[K])
    }

    def joinSplit[S:ClassTag](heavy: RDD[(K, V)], rlight: RDD[(K, S)], rheavy: RDD[(K, S)], hkeys: Set[K]):
      (RDD[(V, S)], RDD[(V, S)]) = {
      if (hkeys.nonEmpty){
        val rfilterLight = rlight.unionFilterPartitions(rheavy, (i: (K, S)) => !hkeys(i._1))
        val lightJoin = joinDropKey(rfilterLight)

        val rfilterHeavy = rlight.unionFilterPartitions(rheavy, (i: (K, S)) => 
          hkeys(i._1)).groupByKey().collect.toMap
        val bcHeavy = heavy.sparkContext.broadcast(rfilterHeavy).value
        val heavyJoin = heavy.mapPartitions(it =>
          it.flatMap{ case (k,v) => bcHeavy get k match {
            case Some(ls) => ls.map(s => (v, s))
            case None => Nil
          }}, true)
        (lightJoin, heavyJoin)
      }else {
        val rrdd = rlight.unionPartitions(rheavy)
		    (lrdd.joinDropKey(rrdd), lrdd.sparkContext.emptyRDD[(V,S)])
	    }
    }
    
  // for domains
  // can just return a dictionary as an optimization see lookup
  def joinSplit[L:ClassTag](domain: RDD[L], extract: L => K):
      (RDD[(V, L)], RDD[(V, L)], Set[K]) = {
    val hk = lrdd.heavyKeys()
    if (hk.nonEmpty){
      val hkeys = lrdd.sparkContext.broadcast(hk).value
      val domainLight = domain.extractLight(extract, hkeys)
      val light = lrdd.filterPartitions((i: (K,V)) => !hkeys(i._1))
      // fix this to not lose partitioning info
      val ldict = lrdd.cogroup(domainLight, new HashPartitioner(partitions)).mapPartitions(it =>
        it.flatMap{ case (lbl, (vs, ls)) =>
          for (v <- vs.iterator; l:L <- ls.toSet.iterator) yield (v, l) }, true)

      val hdomain = domain.extractDistinctHeavyMap(extract, hkeys)
      val heavy = lrdd.filterPartitions((i: (K,V)) => hkeys(i._1))
      val heavyDomain = heavy.sparkContext.broadcast(hdomain).value
      val hdict = heavy.mapPartitions(it =>
        it.flatMap{ case (k,v) => heavyDomain get k match {
          case Some(ls) => ls.map(l => (v, l))
          case None => Nil
         }}, true)
         (ldict, hdict, hkeys)
       }else
         (lrdd.joinDomain(domain, extract), lrdd.sparkContext.emptyRDD[(V, L)], Set.empty[K])
    }

    def reduceBySplit(heavy: RDD[(K, V)], f: (V, V) => V): (RDD[(K, V)], RDD[(K, V)]) =
      (lrdd.unionPartitions(heavy).reduceByKey(f), lrdd.sparkContext.emptyRDD[(K,V)])
 
    // the case where we do not have a known partitioner
    def groupBySplit(heavy: RDD[(K,V)]): (RDD[(K, Iterable[V])], RDD[(K, Iterable[V])], Set[K]) = {
      val unioned = lrdd.unionPartitions(heavy)
      val hkeys = unioned.heavyKeys()
      if (hkeys.nonEmpty){
        val heavyKeys = unioned.sparkContext.broadcast(hkeys).value
        val lset = unioned.filterPartitions((i: (K,V)) => !hkeys(i._1))
        val hset = unioned.filterPartitions((i: (K,V)) => hkeys(i._1))
        (lset.groupByKey(new HashPartitioner(partitions)), hset.groupByLabel(), hkeys) 
      }else (unioned.groupByKey(new HashPartitioner(partitions)), lrdd.sparkContext.emptyRDD[(K,Iterable[V])], Set.empty[K])
    }
 
    def groupBySplit(): (RDD[(K, Iterable[V])], RDD[(K, Iterable[V])], Set[K]) = {
      val hkeys = lrdd.heavyKeys()
      if (hkeys.nonEmpty){
        val heavyKeys = lrdd.sparkContext.broadcast(hkeys).value
        val lset = lrdd.filterPartitions((i: (K,V)) => !hkeys(i._1))
        val hset = lrdd.filterPartitions((i: (K,V)) => hkeys(i._1))
        (lset.groupByKey(new HashPartitioner(partitions)), hset.groupByLabel(), hkeys) 
      }else (lrdd.groupByKey(new HashPartitioner(partitions)), lrdd.sparkContext.emptyRDD[(K,Iterable[V])], Set.empty[K])
    }
   
    // known heavy keys split by light and heavy
    def groupBySplit(heavy: RDD[(K,V)], hkeys: Set[K]): (RDD[(K, Iterable[V])], RDD[(K, Iterable[V])]) = 
      lrdd.partitioner match {
	      case Some(p) => (lrdd.groupByKey(), heavy.groupByLabel())
	      case _ => (lrdd.groupByKey(new HashPartitioner(partitions)), heavy.groupByLabel()) 
      }

    def rekeyBySet[S](rrdd: RDD[(K, S)], keyset: Broadcast[Set[K]]): (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val rekey = 
        lrdd.mapPartitions( it => {
          it.zipWithIndex.map{ case ((k,v), i) => 
            (k, { if (keyset.value(k)) i % reducers*2 else 0 }) -> v
          }}, true)

      val dupp = 
        rrdd.flatMap{ case (k,v) =>
          Range(0, {if (keyset.value(k)) reducers*2 else 1 }).map(id => (k, id) -> v) 
        }
      (rekey, dupp)
    }
 
    def rekeyByIndex[S: ClassTag](rrdd: RDD[S], keyset: Broadcast[Set[K]], f: S => K, partitioner: Partitioner): 
      (RDD[((K, Int), V)], RDD[((K, Int), Set[S])]) = {
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)
      val dupp = rrdd.duplicateDistinct(f, partitioner, keyset)
      (rekey, dupp)
    }

    def rekeyByIndex[S: ClassTag](rrdd: RDD[S], keyset: Broadcast[Set[K]], f: S => K): 
      (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)

      val partitionRange = Range(0, partitions)
      val dupp = rrdd.flatMap{ v => { 
          val k = f(v)
          if (keyset.value(k)) partitionRange.map(id => (k, id) -> v)
          else List(((k, -1),v)) 
        }}
      (rekey, dupp)
    }
   
    def rekeyByIndex[S: ClassTag](rrdd: RDD[(K, S)], keyset: Broadcast[Set[K]]): 
      (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val rekey = 
        lrdd.mapPartitionsWithIndex( (index, it) => it.map{ case (k,v) => 
          ((k, { if (keyset.value(k)) index else -1 }), v) 
        }, true)

      val partitionRange = Range(0, partitions)
      val dupp = rrdd.flatMap{ case (k,v) => 
          if (keyset.value(k)) partitionRange.map(id => (k, id) -> v)
          else List(((k, -1),v)) 
        }
      (rekey, dupp)
    }

	  def split(): (RDD[(K, V)], RDD[(K, V)])  = {
      val hkeys = lrdd.heavyKeys()
      if (hkeys.nonEmpty){
        val heavyKeys = lrdd.sparkContext.broadcast(hkeys).value
        val light = lrdd.filterPartitions((i: (K,V)) => !heavyKeys(i._1))
        val heavy = lrdd.filterPartitions((i: (K,V)) => heavyKeys(i._1))
        (light, heavy)
      }else (lrdd, lrdd.sparkContext.emptyRDD[(K,V)])
    }

	  def filterHeavy[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[(K, V)],RDD[(K, S)])  = {
      val lheavy = lrdd.filterPartitions((i: (K,V)) => hkeys.value(i._1))
      val rheavy = rrdd.filterPartitions((i: (K,S)) => hkeys.value(i._1))
      (lheavy, rheavy)
    }

	  def filterHeavy[S:ClassTag](rrdd: RDD[S], hkeys: Broadcast[Set[K]], extract: S => K): (RDD[(K, V)], Map[K, Set[S]])  = {
      val lheavy = lrdd.filterPartitions((i: (K,V)) => hkeys.value(i._1))
      val rheavy = rrdd.extractDistinctHeavyMap(extract, hkeys.value)
      (lheavy, rheavy)
    }

	  def filterLight[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[(K, V)],RDD[(K, S)]) = {
      val llight = lrdd.filterPartitions((i: (K,V)) => !hkeys.value(i._1))
      val rlight = rrdd.filterPartitions((i: (K,S)) => !hkeys.value(i._1))
      (llight, rlight)
    }

    def filterLight[S](rrdd: RDD[S], hkeys: Broadcast[Set[K]], extract: S => K): (RDD[(K, V)], RDD[(K, S)]) = {
      val llight = lrdd.filterPartitions((i: (K,V)) => !hkeys.value(i._1))
      val rlight = rrdd.mapPartitions(it =>
        it.flatMap{ v => {
          val nlbl = extract(v)
          if (!hkeys.value(nlbl)) List((nlbl, v)) else Nil 
         }}, true)
      (llight, rlight)
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[S], fkey: S => K): RDD[(V, S)] = {
      lrdd.cogroup(rrdd.map(v => (fkey(v),v))).mapPartitions( it =>
	  	it.flatMap{ case (key, (vs, ss)) =>
          for (v <- vs.iterator; s <- ss.iterator) yield (v, s)}, true)
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = {
      lrdd.cogroup(rrdd).mapPartitions( it =>
	  	it.flatMap{ case (key, (vs, ss)) =>
          for (v <- vs.iterator; s <- ss.iterator) yield (v, s)}, true)
    }

    def outerJoinDropKey[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, Option[S])] = {
      lrdd.cogroup(rrdd).flatMap{
        case (key, (vs, Seq())) => vs.iterator.map(v => (v, None))
        case (key, (vs, ss)) => 
          for (v <- vs.iterator; s <- ss.iterator) yield (v, Some(s))}
    }

    // left
    def outerJoinDropKey[S:ClassTag](rrdd: RDD[(K, S)], partitioner: Partitioner): RDD[(V, Option[S])] = {
      lrdd.cogroup(rrdd, partitioner).flatMap{
        case (key, (vs, Seq())) => vs.iterator.map(v => (v, None))
        case (key, (vs, ss)) => 
          for (v <- vs.iterator; s <- ss.iterator) yield (v, Some(s))}
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[S], fkey: S => K, partitioner: Partitioner): RDD[(V, S)] = {
      lrdd.cogroup(rrdd.map(v => (fkey(v),v)), partitioner).mapPartitions( it =>
	          it.flatMap{ case (key, (vs, ss)) =>
			            for (v <- vs.iterator; s <- ss.iterator) yield (v, s)}, true)
    }

    def joinDropKey[S:ClassTag](rrdd: RDD[(K, S)], partitioner: Partitioner): RDD[(V, S)] = {
      lrdd.cogroup(rrdd, partitioner).mapPartitions( it =>
	          it.flatMap{ case (key, (vs, ss)) =>
			            for (v <- vs.iterator; s <- ss.iterator) yield (v, s)}, true)
    }

    def joinSalt[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
      val hk = heavyKeys()
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyBySet(rrdd, hkeys)
        rekey.joinDropKey(dupp)
      } else lrdd.joinDropKey(rrdd)
    }

    def outerJoinSalt[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, Option[S])] = { 
      val hk = heavyKeys()
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey, dupp) = lrdd.rekeyBySet(rrdd, hkeys)
        rekey.outerJoinDropKey(dupp)
      } else lrdd.outerJoinDropKey(rrdd)
    }

    def joinDomain[S:ClassTag](rrdd: RDD[S], extract: S => K): RDD[(V, S)] = {
      val domain = rrdd.map(l => (extract(l), l))
      lrdd.cogroup(domain, new HashPartitioner(partitions)).mapPartitions(it =>
        it.flatMap{ case (lbl, (vs, ss)) =>
          for (v <- vs.iterator; s:S <- ss.toSet.iterator) yield (v, s) }, true)
    }
 
    def joinDomainSkew[S:ClassTag](rrdd: RDD[S], extract: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (llrdd, ldomain) = lrdd.filterLight(rrdd, hkeys, extract)
        val (hlrdd, hdomain) = lrdd.filterHeavy(rrdd, hkeys, extract)
        val light = llrdd.cogroup(ldomain).flatMap{ pair =>
          for (v <- pair._2._1.iterator; l:S <- pair._2._2.toSet.iterator) yield (v, l)}
        val heavyDomain = hlrdd.sparkContext.broadcast(hdomain).value
        val heavy = hlrdd.mapPartitions(it =>
          it.flatMap{ case (k,v) => heavyDomain get k match {
            case Some(ls) => ls.map(l => (v, l))
            case None => Nil
          }}, true)
		    light.zipPartitions(heavy, true)((l: Iterator[(V,S)], r: Iterator[(V,S)]) => l ++ r)
	    }else lrdd.joinDomain(rrdd, extract)
    }
   
    def joinDomainSkewTag[S:ClassTag](rrdd: RDD[S], extract: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val partitioner = new SkewPartitioner(partitions)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys, extract, partitioner)
        rekey.cogroup(dupp, partitioner).flatMap{ pair =>
          for (v <- pair._2._1.iterator; s <- pair._2._2.iterator.flatten) yield (v, s)
        }
  		}else lrdd.joinDomain(rrdd, extract)
    }

    
    def joinSkew[S:ClassTag](rrdd: RDD[S], fkey: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        // todo push this
        val rkeyed = rrdd.map(r => (fkey(r), r))
        val (llrdd, lrrdd) = lrdd.filterLight(rkeyed, hkeys)
        val (hlrdd, hrrdd) = lrdd.filterHeavy(rkeyed, hkeys)
        val light = llrdd.joinDropKey(lrrdd)
        // can't just to map this?
        val heavyRight = hlrdd.sparkContext.broadcast(hrrdd.collect.toMap).value
        val heavy = hlrdd.mapPartitions(it =>
          it.flatMap{ case (k,v) => heavyRight get k match {
            case Some(r) => List((v, r))
            case None => Nil
          }}, true)
		    light.zipPartitions(heavy, true)((l: Iterator[(V,S)], r: Iterator[(V,S)]) => l ++ r)
      }else lrdd.joinDropKey(rrdd, fkey)
    }

    def joinSkew[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (llrdd, lrrdd) = lrdd.filterLight(rrdd, hkeys)
        val (hlrdd, hrrdd) = lrdd.filterHeavy(rrdd, hkeys)
        val light = llrdd.joinDropKey(lrrdd)
        val heavyRight = hlrdd.sparkContext.broadcast(hrrdd.collect.toMap).value
        val heavy = hlrdd.mapPartitions(it =>
          it.flatMap{ case (k,v) => heavyRight get k match {
            case Some(r) => List((v, r))
            case None => Nil
          }}, true)
		    light.zipPartitions(heavy, true)((l: Iterator[(V,S)], r: Iterator[(V,S)]) => l ++ r)
	    }else lrdd.joinDropKey(rrdd)
    }

    def joinSkewTag[S:ClassTag](rrdd: RDD[S], fkey: S => K): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val partitioner = new SkewPartitioner(partitions)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys, fkey)
        rekey.joinDropKey(dupp, partitioner)
  		}else lrdd.joinDropKey(rrdd, fkey)
    }

  	def joinSkewTag[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, S)] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val partitioner = new SkewPartitioner(partitions)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys)
	      rekey.joinDropKey(dupp, partitioner)
  		}else lrdd.joinDropKey(rrdd) 
    }

  	def outerJoinSkew[S:ClassTag](rrdd: RDD[(K, S)]): RDD[(V, Option[S])] = { 
  		val hk = heavyKeys()
  	  if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val partitioner = new SkewPartitioner(partitions)
        val (rekey, dupp) = lrdd.rekeyByIndex(rrdd, hkeys)
	      rekey.outerJoinDropKey(dupp, partitioner)
  		}else lrdd.outerJoinDropKey(rrdd) 
    }

    def groupByLabel(): RDD[(K, Iterable[V])] = {
      val groupBy = (i: Iterator[(K,V)]) => {
        val hm = HashMap[K, Vector[V]]()
        i.foreach{ v =>
          hm(v._1) = hm.getOrElse(v._1, Vector()) :+ v._2
        }
        hm.iterator
      }
      lrdd.mapPartitions(groupBy, true)
    }

    def groupByLabel[R: ClassTag,S: ClassTag](f: (K,V) => (R,S)): RDD[(R, Iterable[S])] = {
      val groupBy = (i: Iterator[(K,V)]) => {
        val hm = HashMap[R, Vector[S]]()
        i.foreach{ v0 =>
          val (k,v) = f(v0._1, v0._2) 
          hm(k) = hm.getOrElse(k, Vector()) :+ v
        }
        hm.iterator
      }
      lrdd.mapPartitions(groupBy, true)
    }
 
    def groupByLabelSet(): RDD[(K, Iterable[V])] = {
      val groupBy = (i: Iterator[(K,V)]) => {
        val hm = HashMap[K, Set[V]]()
        i.foreach{ v =>
          hm(v._1) = hm.getOrElse(v._1, Set()) + v._2
        }
        hm.iterator
      }
      lrdd.mapPartitions(groupBy, true)
   }

 
 	 def groupBySkew(): RDD[(K, Iterable[V])] = {
      val hk = heavyKeys()
      if (hk.nonEmpty){
        val hkeys = lrdd.sparkContext.broadcast(hk).value
        val llrdd = lrdd.filterPartitions((i: (K,V)) => !hkeys(i._1))
        val hlrdd = lrdd.filterPartitions((i: (K,V)) => hkeys(i._1))
        val light = llrdd.groupByKey()
        val heavy = hlrdd.groupByLabel()
		light.zipPartitions(heavy, true)(
			(l: Iterator[(K,Iterable[V])], r: Iterator[(K,Iterable[V])]) => l ++ r)
      }
      else lrdd.groupByKey() 
    }

	 def groupBySkewTag(): RDD[(K, Iterable[V])] = {
      val hk = heavyKeys()
      if (hk.nonEmpty){
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val rekey = lrdd.mapPartitionsWithIndex( (index, it) =>
          it.zipWithIndex.map{ case ((k,v), i) => 
            (k, { if (hkeys.value(k)) i % partitions else index }) -> v
          }, true)
        rekey.groupByKey(new SkewPartitioner(partitions)).map{ case ((k, _), v) => k -> v }
      }
      else groupByLabel() 
    }

  
  }

}
