package sprkloader

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import SkewDictRDD._
import DomainRDD._

object SkewSplitOps{

  implicit class SkewSplitFunctions[K: ClassTag, V: ClassTag](light: RDD[(K,Iterable[V])]) extends Serializable {

    /**def lookupSplit[L:ClassTag](heavy: RDD[(K, Iterable[V])], domain: RDD[L], extract: L => K, hkeys: Set[K] = Set.empty[K]):  
      (RDD[(L, Iterable[V])], RDD[(L, Iterable[V])]) = {
       
       if (hkeys.nonEmpty){
         
         val domainLight = domain.extractLight(extract, hkeys)
         val ldict = light.lookup(domainLight)
         
         val heavyMap = domain.extractDistinctHeavyMap(extract, hkeys)
         val heavyDomain = heavy.sparkContext.broadcast(heavyMap).value
         val hdict = heavy.lookupHeavy(heavyDomain)
         
         (ldict, hdict)   

       }else 
         assert(heavy.isEmpty)
         (light.lookup(domain, extract), light.sparkContext.emptyRDD[(L, Iterable[V])])
      }**/

  }

}
