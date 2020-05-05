package sparkutils.rdd

import scala.reflect.ClassTag
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast.Broadcast

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

  def cogroupBy[K,V,S](i: Iterator[(K,V)], hks: Broadcast[Map[K, Vector[S]]]): Iterator[(V, Vector[S])] = {
    val hm = HashMap[V, Vector[S]]()
      i.foreach{ case (k,v) =>
        hks.value get k match {
          case Some(ls) => hm(v) = hm.getOrElse(v, Vector()) ++ ls
          case None => hm(v) = hm.getOrElse(v, Vector())
        }
      }
    hm.iterator
  }

  def groupBy[K,V](i: Iterator[(K,V)], f: (V, V) => V): Iterator[(K, V)] = {
    val hm = HashMap[K, V]()
      i.foreach{ case (k,v) => hm get k match {
        case Some(v2) => hm(k) = f(v2, v)
        case None => hm(k) = v
      }
    } 
    hm.iterator
  }


  def groupBy[K,V](i: Iterator[(K,V)], hks: Broadcast[Set[K]]): Iterator[(K, Vector[V])] = {
    val hm = HashMap[K, Vector[V]]()
      i.foreach{ case (k,v) =>
        if (hks.value(k)) hm(k) = hm.getOrElse(k, Vector()) ++ Vector(v)
      }
    hm.iterator
  }

}