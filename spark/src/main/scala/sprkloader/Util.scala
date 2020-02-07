package sprkloader

import scala.reflect.ClassTag
import scala.collection.mutable.HashMap

object Util{ 

  def countDistinct[K,V](i: Iterator[(K,V)]) = 
    i.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
      { acc(c._1) += 1; acc } )

  def countDistinctWithId[K,V](i: Iterator[((K, Int),V)]) = 
    i.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
      { acc(c._1._1) += 1; acc } )
 
  def countValuesByKey[K,V](i: Iterator[(K, Iterator[V])]) = 
    i.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
      { acc(c._1) += c._2.size; acc } )

  def countDistinctByPartition[K,V](i: Iterator[(K,V)], index: Int) = {
    i.foldLeft(HashMap.empty[(K, Int), Int].withDefaultValue(0))((acc, c) =>
      { acc((c._1, index)) += 1; acc } )
  }

}
