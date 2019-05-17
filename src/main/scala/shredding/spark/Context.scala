/**package shredding.spark

import shredding.core.VarDef
import org.apache.spark.rdd.RDD
import collection.mutable.{HashMap => HMap}

class Context() extends Serializable {
    
  val ctx: HMap[VarDef, RDD[_]] = HMap[VarDef, RDD[_]]()
    
  def apply(varDef: VarDef): RDD[_] = ctx(varDef)

  def add(varDef: VarDef, r: RDD[_]) = ctx(varDef) = r
    
  def remove(varDef: VarDef) = ctx.remove(varDef)

  def reset = ctx.clear
}**/
