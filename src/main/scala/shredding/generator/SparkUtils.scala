package shredding.generator

import shredding.core._
import shredding.wmcc._
import shredding.utils.Utils.ind

trait SparkUtils {

  def validLabel(e: Type): Boolean = e match {
    case EmptyCType => true
    case LabelType(_) => true
    case _ => false
  }
  
  /**
    * Check for nulls only if there could have been 
    * a previous OUTER operator
    */
  def checkNull(e: List[Variable]) = e.size match {
    case 1 => ""
    case _ => s" case (a, null) => (null, (a, null));" 
  }

  def castNull(e: CExpr): String = e.tp match {
    case IntType => "-1"
    case DoubleType => "-1.0"
    case _ => "null"
  }

  def zero(e: CExpr): String = e.tp match {
    case IntType => "0"
    case DoubleType => "0.0"
    case _ => "null"
  } 

  def empty(e: CExpr): String = e.tp match {
    case IntType => "0"
    case DoubleType => "0.0"
    case _ => "Vector()"
  }

  def agg(e: CExpr): String = e.tp match {
    // case IntType => "agg(_+_)"
    // case DoubleType => "agg(_+_)"
    case _:NumericType => "agg(_+_)"
    case _ => "group(_++_)"
  } 

  def isDomain(e: CExpr): Boolean = e.tp match {
    case BagCType(RecordCType(ms)) => ms get "_LABEL" match {
      case Some(tp:LabelType) => true
      case _ => false
    }
    case _ => false
  }

  def hasLabel(e: Type): Boolean = e match {
    case TTupleType(ls) => ls.filter(t => hasLabel(t)).nonEmpty
    case RecordCType(ms) => ms.keySet == Set("_LABEL")
    case LabelType(_) => true
    case _ => false
  }

  def isDictRecord(e: CExpr): Boolean = e match {
    case Record(fs) => (fs.keySet == Set("_1","_2") || fs.keySet == Set("key", "value"))
    case _ => sys.error("unsupported type")
  }

  def comment(n: String): String = s"//$n"

  def runJob(n: String, cache:Boolean, evaluate:Boolean): String = {
      s"""|${if (cache) n else comment(n)}.cache
          |//$n.print
          |${if (evaluate) n else comment(n)}.evaluate"""
  }

}