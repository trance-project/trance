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
  //val threshold = Config.threshold
  val partitions = lrdd.getNumPartitions

  /**def heavyKeys(threshold: Int = 1000): Set[K] = {
    val hkeys = lrdd.mapPartitions( it =>
      Util.countValuesByKey(it).filter(_._2 > threshold).iterator,true)
    if (reducers > threshold) hkeys.filter(_._2 >= reducers).keys.collect.toSet
    else hkeys.keys.collect.toSet
  }**/
  
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
  def lookup(rrdd: RDD[K]): RDD[(K, Iterable[V])] = {
      val domPrep = rrdd.collect.toSet
      val domain = lrdd.sparkContext.broadcast(domPrep)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => 
          if (domain.value(key)) Iterator((key, value)) else Iterator()}, true)
  }
  
  /**
    * Same as above, but in step 5 map bagop over the values in the dictionary
    */	
  def lookup[S: ClassTag](rrdd: RDD[K], bagop: V => S): RDD[(K, Iterable[S])] = {
      val domPrep = rrdd.collect.toSet
      val domain = lrdd.sparkContext.broadcast(domPrep)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => 
          if (domain.value(key)) Iterator((key, value.map(bagop))) else Iterator()}, true)
  }

  /**def lookup[L: ClassTag](rrdd: RDD[L], domop: L => K): RDD[(K, Iterable[V])] = {
      val domPrep = rrdd.createDomainSet(domop) 
      val domain = lrdd.sparkContext.broadcast(domPrep)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => if (domain.value(key)) Iterator((key, value)) else Iterator()}, true)
  }

	def lookup[L: ClassTag,S](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(K, Iterable[S])] = {
      val domPrep = rrdd.createDomainSet(domop)
      val domain = lrdd.sparkContext.broadcast(domPrep)
      lrdd.mapPartitions(it => 
        it.flatMap{ case (key, value) => 
          if (domain.value(key)) Iterator((key, value.map(bagop))) else Iterator()}, true)
  }**/

  // label could maintain partition information to avoid this process
  // when the domain is too large to collect or broadcast

  /**
    * When the domain is too large and/or there are heavy keys in the dictionary
    * cogroup the values with the partitioning strategy of the dictionary (that is 
    * assumed to be balanced). 
    * 1) tag each label in the dictionary with it's partition id - note this could 
    * be an attribute of the label to avoid having to key
    * 2) 
    */
  def lookupSkewLeft[L:ClassTag](rrdd: RDD[L], domop: L => K): RDD[(K, Iterable[V])] = {
      val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
        it.map{case (k,v) => (k, index) -> v}, true)
      //val tagDomain = rrdd.extractDistinctRekey(domop, partitions)
      val tagDomain = rrdd.extractRekey(domop, partitions)
      tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).map{
        case ((key, index), (bag, _)) => key -> bag.flatten
      }
    }

  def lookupSkewLeft[L: ClassTag,S](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(K, Iterable[S])] = {
    val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
      it.map{case (k,v) => (k, index) -> v.map(bagop)}, true)
    //val tagDomain = rrdd.extractDistinctRekey(domop, partitions)
    val tagDomain = rrdd.extractRekey(domop, partitions)
    tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).map{
      case ((key, index), (bag, _)) => key -> bag.flatten
    }
  }

  // an option to push the flattening to the lookup process
  def lookupSkewLeftFlat[L: ClassTag,S](rrdd: RDD[L], domop: L => K, bagop: V => S): RDD[(S, (K, V))] = {
    val tagDict = lrdd.mapPartitionsWithIndex((index, it) =>
      it.map{case (k,v) => (k, index) -> v}, true)
    val tagDomain = rrdd.extractDistinctRekey(domop, partitions)
    tagDict.cogroup(tagDomain, new SkewPartitioner(partitions)).flatMap{ pair =>
      for (b <- pair._2._1.flatten.iterator) yield (bagop(b), (pair._1._1, b))
    }
  }


  }
}
