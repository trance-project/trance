package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import SkewPairRDD._
import UtilPairRDD._
import DomainRDD._

object SkewDictRDD {
  
  implicit class SkewDictFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,Iterable[V])]) extends Serializable {

  val reducers = Config.minPartitions
  val threshold = Config.threshold
  val partitions = lrdd.getNumPartitions

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

  /**
    * When the domain is too large and/or there are heavy keys in the dictionary
    * cogroup the values with the partitioning strategy of the dictionary (that is 
    * assumed to be balanced). 
    * 1) tag each label in the dictionary with it's partition id - note this could 
    * be an attribute of the label to avoid having to key
    * 2) 
    */

  /**
    Possibly some extrat trait that would support the inverse K => L, 
    for now just explicitly state
    object extract {
      def apply(l: Record313) = l.lbl.c2__Fc_orders
      def unapply(k: Record168) = Record311(k)
    }
  **/

  // LabelType: Label(...)
  // For label in domain
  //  (label.lbl, Lookup(extract_label(label.lbl), dictionary)
  // this will preserve Label -> {} for unshredding
  def lookup[L:ClassTag](rrdd: RDD[L], extract: L => K): RDD[(L, Iterable[V])] = {
    val domain = rrdd.distinct.mapPartitions(it => it.map(lbl => extract(lbl) -> lbl))
    lrdd.cogroup(domain).mapPartitions(it =>
      it.flatMap{ case (k, (bag, labels)) => labels.map(l => l -> bag.flatten)}, true)
  } 

  // LabelType: InputLabel(...)
  // For label in domain
  //  (label.lbl, For value in Lookup(extract_label(label.lbl), dictionary) subexpr
  // This is an optimization for simple operations over dictionaries
  def lookup[L:ClassTag,S](rrdd: RDD[L], extract: L => K, domlabel: K => L, subexpr: V => S): RDD[(L, Iterable[S])] = {
    val domain = rrdd.distinct.mapPartitions(it => it.map(lbl => extract(lbl) -> 1))
    lrdd.cogroup(domain).mapPartitions(it => // bag is Iterable[Iterable[V]]
      it.flatMap{ case (k, (bag, _)) => bag.map(b => domlabel(k) -> b.map(subexpr))}, true)
  } 

  // For label in domain 
  //  (label.lbl, Lookup(extract_label(label.lbl), dictionary)
  // check for skew
  def lookupSkew[L:ClassTag](rrdd: RDD[L], extract: L => K): RDD[(L, Iterable[V])] = {
    val hk = lrdd.heavyDictKeys()
    if (hk.nonEmpty){
      // if keys are associated to heavy bags then keep them where they are
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v}, true)
      val partitioner = new SkewPartitioner(partitions)
      val tagDomain = rrdd.extractDistinctRekey(extract, partitioner, hkeys)
      tagDict.cogroup(tagDomain, partitioner).mapPartitions(it => // Iterable[Iterable[L]]
        it.flatMap{ case ((k, id), (bag, labels)) => labels.flatMap(lbl => lbl.map(l => l -> bag.flatten))}, true)
    }else lrdd.lookup(rrdd, extract)
  }      
  
  def lookupSkew[L: ClassTag,S:ClassTag](rrdd: RDD[L], extract: L => K, domlabel: K => L, subexpr: V => S): RDD[(L, Iterable[S])] = {
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
    val domain = rrdd.distinct.mapPartitions(it => it.map(lbl => extract(lbl) -> lbl))
    lrdd.cogroup(domain).flatMap{ pair => // check partitioning
      for (v <- pair._2._1.iterator.flatten; l <- pair._2._2.iterator) yield (l, v)
    }
  }

  def lookupIterator[L:ClassTag, S](rrdd: RDD[L], extract: L => K, fkey: V => S): RDD[(S, (L, V))] = {
    val domain = rrdd.distinct.mapPartitions(it => it.map(lbl => extract(lbl) -> lbl))
    lrdd.cogroup(domain).flatMap{ pair => // check partitioning
      for (v <- pair._2._1.iterator.flatten; l <- pair._2._2.iterator) yield (fkey(v), (l, v))
    }
  }

  def lookupSkewIterator[L:ClassTag](rrdd: RDD[L], extract: L => K): RDD[(L, V)] = {
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

  def lookupSkewIterator[L:ClassTag,S](rrdd: RDD[L], extract: L => K, fkey: V => S): RDD[(S, (L, V))] = {
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

  }
}
