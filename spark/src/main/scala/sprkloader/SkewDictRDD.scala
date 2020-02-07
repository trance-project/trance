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
  def lookupSkewLeft[L:ClassTag](rrdd: RDD[L], domop: L => K): RDD[(K, Iterable[V])] = {
    val hk = lrdd.heavyDictKeys()
    if (hk.nonEmpty){
      // if keys are associated to heavy bags then keep them where they are
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v}, true)
      val tagDomain = rrdd.extractDistinctRekey(domop, partitions, hkeys)
      tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).mapPartitions(it =>
        it.map{ case ((k, id), (bag, _)) => k -> bag.flatten}, true)
    }else{
      // check if small enough to broadcast
      // if not then just do a normal join
      val domain = rrdd.distinct.mapPartitions(it => it.map(lbl => domop(lbl) -> 1))
      lrdd.cogroup(domain).mapPartitions(it =>
        it.map{ case (k, (bag, _)) => k -> bag.flatten}, true)
    }
  }      
  
  def lookupSkewLeft[L: ClassTag,S:ClassTag](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(K, Iterable[S])] = {
    val hk = lrdd.heavyDictKeys() 
    if (hk.nonEmpty){
      // if keys are associated to heavy bags then keep them where they are
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v}, true)
      val tagDomain = rrdd.extractRekey(domop, partitions, hkeys)
      tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).mapPartitions(it =>
        it.map{ case ((k, id), (bag, _)) => k -> bag.flatMap(b => b.map(bagop))}, true)
    }else{
      // check if small enough to broadcast
      // if not then just do a normal join
      val domain = rrdd.distinct.mapPartitions(it => it.map(lbl => domop(lbl) -> 1))
      lrdd.cogroup(domain).mapPartitions(it =>
        it.map{ case (k, (bag, _)) => k -> bag.flatMap(b => b.map(bagop))}, true)
    }
  }

  // an option to push the flattening to the lookup process
  def lookupSkewLeftFlat[L: ClassTag,S:ClassTag](rrdd: RDD[L], domop: L => K): RDD[(K, V)] = {
    val hk = lrdd.heavyDictKeys() 
    if (hk.nonEmpty){
      // if keys are associated to heavy bags then keep them where they are
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v}, true)
      val tagDomain = rrdd.extractRekey(domop, partitions, hkeys)
      tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).flatMapValues{ pair => 
        pair._1.flatten }.mapPartitions(it => it.map{ case ((k,id), v) => k -> v }, true)
    }else{
      // check if small enough to broadcast
      // if not then just do a normal join
      val domain = rrdd.distinct.mapPartitions(it => it.map(lbl => domop(lbl) -> 1))
      lrdd.cogroup(domain).flatMapValues{ pair => pair._1.flatten }
    }
  }

  def lookupSkewLeftFlat[L: ClassTag,S:ClassTag](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(S, (K, V))] = {
    val hk = lrdd.heavyDictKeys() 
    if (hk.nonEmpty){
      // if keys are associated to heavy bags then keep them where they are
      val hkeys = lrdd.sparkContext.broadcast(hk)
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => if (hkeys.value(k)) (k, index) -> v else (k, -1) -> v}, true)
      val tagDomain = rrdd.extractRekey(domop, partitions, hkeys)
      tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).flatMapValues{ 
        pair => pair._1.flatMap(b => b.map(v => (bagop(v), v))) }.mapPartitions(it => 
          it.map{ case ((k,id), (k1, v)) => k1 -> (k, v) }, true)
    }else{
      // check if small enough to broadcast
      // if not then just do a normal join
      val domain = rrdd.distinct.mapPartitions(it => it.map(lbl => (domop(lbl),1)))
      lrdd.cogroup(domain).flatMapValues{ pair => 
        pair._1.flatten}.mapPartitions(it =>
          it.map{ case (k, v) => (bagop(v),(k, v))}, true)
    }
  }


  }
}
