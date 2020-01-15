package sprkloader

import scala.collection.mutable.HashMap

object Util{ 

  def countDistinct[K,V](i: Iterator[(K,V)]) = {
    i.foldLeft(HashMap.empty[K, Int].withDefaultValue(0))((acc, c) =>
      { acc(c._1) += 1; acc } )
  }

}
