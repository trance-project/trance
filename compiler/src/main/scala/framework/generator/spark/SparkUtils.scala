package framework.generator.spark

import framework.common._
import framework.plans._
import framework.utils.Utils.ind

case class COption(e: CExpr) extends CExpr {
  def tp: OptionType = OptionType(e.tp)
}

/** 
  * Utility functions requried for generating Spark/Scala applications
  */
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
    case DoubleType => "0.0"
    case _ => "null"
  }

  def castNull(e: Type): String = e match {
    case IntType => "-1"
    case DoubleType => "0.0"
    case _ => "null"
  }

  def zero(e: CExpr): String = e.tp match {
    case OptionType(tp) => zero(tp)
    case IntType => "0"
    case DoubleType => "0.0"
    case _ => "null"
  } 

  def zero(e: Type, df: Boolean = false): String = e match {
    case OptionType(tp) => zero(tp)
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
    case BagCType(LabelType(_)) => true
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
    case Record(fs) => fs.contains("_1")
    case _ => sys.error("unsupported type")
  }

  def comment(n: String): String = s"//$n"

  def runJob(n: String, cache:Boolean, evaluate:Boolean): String = {
    val dictCache = cache && (n.contains("MDict") || n.contains("MBag")) 
    if (!n.contains("UDict")){
      s"""|${if (cache || dictCache) n else comment(n)}.cache
          |//$n.print
          |${if (evaluate) n else comment(n)}.evaluate"""
    }else ""

  }

  /** Record writing utils **/

  def getRecord(name: String, v: String, ms: Map[String, Type]): String =
    ms.map(t => s"$v.${t._1}").mkString(s"$name(", ", ", ")")

  def getRecord(name: String, v: String, ms: Map[String, CExpr], col: String = ""): String = 
    ms.map{
      case (attr, expr) if attr == col => col
      case (attr, Project(_, f)) => s"$v.$f"
    }.mkString(s"$name(", ", ", ")")



}
