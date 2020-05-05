package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import OldSkewPairRDD._
import UtilPairRDD._
import DomainRDD._

object OldSkewDictRDD {
  
  implicit class SkewDictFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,Iterable[V])]) extends Serializable {

  val reducers = Config.minPartitions
  val threshold = Config.threshold
  val partitions = lrdd.getNumPartitions

  // need to change this
  def heavyDictKeys(): Set[K] = {
    val hkeys = lrdd.mapPartitions( it =>
      it.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
        { acc(c._1) += c._2.size; acc } ).filter(_._2 > threshold).iterator, true)
    hkeys.keys.collect.toSet
  }
  
  def labelSizeByPartition(): RDD[((K, Int), Int)] = {
		lrdd.mapPartitionsWithIndex((index, it) => 
			it.map{ case (lbl, bag) => (lbl, index) -> bag.size }, true)	
	}

  def createDomain[C: ClassTag](f: V => C): RDD[C] = {
     lrdd.flatMap{
       case (lbl, bag) => bag.foldLeft(Set.empty[C])(
        (acc, v) => acc + f(v)
       )
     }
  }

  def splitDict(): (RDD[(K, Iterable[V])], RDD[(K, Iterable[V])], Set[K])  = {
      val hkeys = lrdd.heavyDictKeys()
      if (hkeys.nonEmpty){
        val heavyKeys = lrdd.sparkContext.broadcast(hkeys).value
        val light = lrdd.filterPartitions((i: (K,Iterable[V])) => !heavyKeys(i._1))
        val heavy = lrdd.filterPartitions((i: (K,Iterable[V])) => heavyKeys(i._1))
        (light, heavy, hkeys)
      }else (lrdd, lrdd.sparkContext.emptyRDD[(K,Iterable[V])], Set.empty[K])
    }

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

    // unshredding, uses known partitioning information for light keys 
  def rightLookup[S:ClassTag](heavy: RDD[(K,Iterable[V])], rrdd: RDD[(K,S)], hkeys: Set[K]): RDD[(S, Iterable[V])] = {
    if (hkeys.nonEmpty){
      val rlight = rrdd.filter(i => !hkeys(i._1))
      val light = rlight.cogroup(lrdd).flatMap{
        case (_, (parDict, chiDict)) => parDict.map(s => s -> chiDict.flatten)
      }

      val rheavy = rrdd.filter(i => hkeys(i._1))
      val lheavy = rheavy.cogroup(heavy).flatMap{
        case (_, (parDict, chiDict)) => parDict.map(s => s -> chiDict.flatten)
      }
      light.unionPartitions(lheavy, false)
    }else
      rrdd.cogroup(lrdd).flatMap{
        case (_, (parDict, chiDict)) => parDict.map(s => s -> chiDict.flatten)
      }
  }

  def lookupSplit[L:ClassTag](heavy: RDD[(K, Iterable[V])], domain: RDD[L], extract: L => K, hkeys: Set[K]):
      (RDD[(L, Iterable[V])], RDD[(L, Iterable[V])]) = {

       if (hkeys.nonEmpty){
         
         val domainLight = domain.extractLight(extract, hkeys)
         val ldict = lrdd.cogroup(domainLight).mapPartitions(it =>
            it.flatMap{ case (k, (bag, labels)) => 
              val fbag = bag.flatten
              if (fbag.nonEmpty) labels.toSet[L].map(l => l -> bag.flatten)
              else Nil}, true) 

          val hdomain = domain.extractDistinctHeavyMap(extract, hkeys)
          val heavyDomain = heavy.sparkContext.broadcast(hdomain).value 
          val hdict = heavy.mapPartitions(it =>
            it.flatMap{ case (k,v) => heavyDomain get k match {
              case Some(ls) => ls.map(l => (l, v))
              case None => Nil
            }}, true)
         (ldict, hdict)
       }else
         (lrdd.lookup(domain, extract), lrdd.sparkContext.emptyRDD[(L, Iterable[V])])
    }
  
  /**
    * 1) Build up a domain of labels from the parent dictionary 
    * creating a Set of labels per partition (local distinct)
    * 2) Extract from the domain before the lookup
    * 3) In the lookup (this function), collect the domain as a Set (global distinct)
    * 4) broadcast to each dictionary partition
    * 5) Filter each dictionary partition based on values in the domain (note that 
    * this could create skew in partition size.
    */
  def broadcastLookup(rrdd: RDD[K]): RDD[(K, Iterable[V])] = {
      val domPrep = rrdd.collect.toSet
      val domain = lrdd.sparkContext.broadcast(domPrep)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => 
          if (domain.value(key)) Iterator((key, value)) else Iterator()}, true)
  }
  
  /**
    * Same as above, but in step 5 map bagop over the values in the dictionary
    */	
  def broadcastLookup[S: ClassTag](rrdd: RDD[K], bagop: V => S): RDD[(K, Iterable[S])] = {
      val domPrep = rrdd.collect.toSet
      val domain = lrdd.sparkContext.broadcast(domPrep)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => 
          if (domain.value(key)) Iterator((key, value.map(bagop))) else Iterator()}, true)
  }

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

  def lookup[L:ClassTag](rrdd: RDD[(K, L)]): RDD[(L, Iterable[V])] = {
    lrdd.cogroup(rrdd).mapPartitions(it =>
      it.flatMap{ case (k, (bag, labels)) => 
	  	val fbag = bag.flatten
		if (fbag.nonEmpty) labels.toSet[L].map(l => l -> bag.flatten)
  		else Nil}, true)
  } 

  // LabelType: Label(...)
  // For label in domain
  //  (label.lbl, Lookup(extract_label(label.lbl), dictionary)
  // this will preserve Label -> {} for unshredding
  def lookup[L:ClassTag](rrdd: RDD[L], extract: L => K): RDD[(L, Iterable[V])] = {
    val domain = rrdd.mapPartitions(it => it.map(lbl => extract(lbl) -> lbl))
    lrdd.cogroup(domain).mapPartitions(it =>
      it.flatMap{ case (k, (bag, labels)) => 
	  	val fbag = bag.flatten
		if (fbag.nonEmpty) labels.toSet[L].map(l => l -> bag.flatten)
		else Nil}, true)
  } 

  // LabelType: InputLabel(...)
  // For label in domain
  //  (label.lbl, For value in Lookup(extract_label(label.lbl), dictionary) subexpr
  // This is an optimization for simple operations over dictionaries
  def lookup[L:ClassTag,S](rrdd: RDD[L], extract: L => K, domlabel: K => L, subexpr: V => S): RDD[(L, Iterable[S])] = {
    val domain = rrdd.mapPartitions(it => it.map(lbl => extract(lbl) -> 1))
    lrdd.cogroup(domain).mapPartitions(it => // bag is Iterable[Iterable[V]]
      it.flatMap{ case (k, (bag, _)) => bag.map(b => domlabel(k) -> b.map(subexpr))})
  } 



  def lookupSkew[L:ClassTag](rrdd: RDD[L], extract: L => K): RDD[(L, Iterable[V])] = {
    val hk = lrdd.heavyDictKeys()
    if (hk.nonEmpty){
      // if keys are associated to heavy bags then keep them where they are
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val (ldict, ldomain) = lrdd.filterLight(rrdd, hkeys, extract)
      val (hdict, hdomain) = lrdd.filterHeavy(rrdd, hkeys, extract)
      val light = ldict.cogroup(ldomain).mapPartitions(it =>
        it.flatMap{ case (k, (bag, labels)) => labels.toSet[L].map(l => l -> bag.flatten)}, true)
      val heavyDomain = hdict.sparkContext.broadcast(hdomain).value 
      val heavy = hdict.mapPartitions(it =>
        it.flatMap{ case (k,v) => heavyDomain get k match {
          case Some(ls) => ls.map(l => (l, v))
          case None => Nil
       }}, true)
      light.zipPartitions(heavy, true)((l: Iterator[(L,Iterable[V])], r: Iterator[(L,Iterable[V])]) => l ++ r)
    }else lrdd.lookup(rrdd, extract)
  }      

  // For label in domain 
  //  (label.lbl, Lookup(extract_label(label.lbl), dictionary)
  // check for skew
  def lookupSkewTag[L:ClassTag](rrdd: RDD[L], extract: L => K): RDD[(L, Iterable[V])] = {
    val hk = lrdd.heavyDictKeys()
    if (hk.nonEmpty){
      // if keys are associated to heavy bags then keep them where they are
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v}, true)
      val partitioner = new SkewPartitioner(partitions)
      val tagDomain = rrdd.extractDistinctRekey(extract, partitioner, hkeys)
      tagDict.cogroup(tagDomain, partitioner).mapPartitions(it => // Iterable[Iterable[L]]
        // remove labels associated with empty bags NOTE and in above as well
        it.flatMap{ case ((k, id), (bag, labels)) => {
          val bflat = bag.flatten 
          if (bflat.isEmpty) Nil
          else labels.flatMap(lbl => lbl.map(l => l -> bflat))}}, true)
    }else lrdd.lookup(rrdd, extract)
  }      
  
  def lookupSkewTag[L: ClassTag,S:ClassTag](rrdd: RDD[L], extract: L => K, domlabel: K => L, subexpr: V => S): RDD[(L, Iterable[S])] = {
    val hk = lrdd.heavyDictKeys() 
    if (hk.nonEmpty){
      // if keys are associated to heavy bags then keep them where they are
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v}, true)
      val partitioner = new SkewPartitioner(partitions)
      val tagDomain = rrdd.extractDistinctLabelRekey(extract, partitioner, hkeys)
      tagDict.cogroup(tagDomain, partitioner).mapPartitions(it =>
        it.flatMap{ case ((k, id), (bag, _)) => bag.map(b => domlabel(k) -> b.map(subexpr))}, true)
    }else lrdd.lookup(rrdd, extract, domlabel, subexpr)
  }

  def lookupIterator[L:ClassTag](rrdd: RDD[L], extract: L => K): RDD[(L, V)] = {
    val domain = rrdd.mapPartitions(it => it.map(lbl => extract(lbl) -> lbl))
    lrdd.cogroup(domain).flatMap{ pair => // check partitioning
      for (v <- pair._2._1.iterator.flatten; l:L <- pair._2._2.toSet.iterator) yield (l, v)
    }
  }

  def lookupIterator[L:ClassTag, S](rrdd: RDD[L], extract: L => K, fkey: V => S): RDD[(S, (L, V))] = {
    val domain = rrdd.mapPartitions(it => it.map(lbl => extract(lbl) -> lbl))
    lrdd.cogroup(domain).mapPartitions( it =>
      it.flatMap{ case (_, (bag, labels)) => bag.flatMap( b => 
        b.flatMap(v => labels.toSet[L].map(l => (fkey(v), (l, v)))))}, true)
  }

  def lookupSkewIteratorTag[L:ClassTag](rrdd: RDD[L], extract: L => K): RDD[(L, V)] = {
    val hk = lrdd.heavyDictKeys()
    if (hk.nonEmpty){
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{ case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v }, true)
      val partitioner = new SkewPartitioner(partitions)
      val tagDomain = rrdd.extractDistinctRekey(extract, partitioner, hkeys)
      tagDict.cogroup(tagDomain, partitioner).flatMap{ pair =>
        for (v <- pair._2._1.iterator.flatten; l <- pair._2._2.iterator.flatten) yield (l, v)
      }
    }else lrdd.lookupIterator(rrdd, extract)
  }      

  def lookupSkewIteratorTag[L:ClassTag,S](rrdd: RDD[L], extract: L => K, fkey: V => S): RDD[(S, (L, V))] = {
    val hk = lrdd.heavyDictKeys()
    if (hk.nonEmpty){
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{ case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v }, true)
      val partitioner = new SkewPartitioner(partitions)
      val tagDomain = rrdd.extractDistinctRekey(extract, partitioner, hkeys)
      tagDict.cogroup(tagDomain, partitioner).flatMap{ pair =>
        for (v <- pair._2._1.iterator.flatten; l <- pair._2._2.iterator.flatten) yield (fkey(v), (l, v))
      }
    }else lrdd.lookupIterator(rrdd, extract, fkey)
  }      

  /** need non-iterator version of this to use in unshredding **/
  def dictLookupIterator[L:ClassTag, S](rrdd: RDD[(K, (L, S))]): RDD[(L, (V, S))] = {
    lrdd.cogroup(rrdd).flatMap{ pair =>
      for (v <- pair._2._1.iterator.flatten; (l,s) <- pair._2._2.iterator) yield (l, (v, s))
    }
  }

  def dictLookupSkewIteratorTag[L:ClassTag, S](rrdd: RDD[(K, (L, S))]): RDD[(L, (V, S))] = {
    val hk = lrdd.heavyDictKeys()
    if (hk.nonEmpty){
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{ case (k, v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v}, true)
      val partitioner = new SkewPartitioner(partitions)
      val parts = Range(0, partitions)
      val tagRdict = rrdd.flatMap{ case (k,v) =>
        if (hkeys.value(k)) parts.map(i => ((k, i), v)) else List(((k, -1), v))
      }
      tagDict.cogroup(tagRdict).flatMap{ pair =>
        for (v <- pair._2._1.iterator.flatten; (l,s) <- pair._2._2.iterator) yield (l, (v, s))
      }
    }else dictLookupIterator(rrdd)
  }


  }
}
