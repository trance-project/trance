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
    case DoubleType => "0.0"
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

  /** dataframe specifc utils **/

  def flattenLabel(e: CExpr): CExpr = e match {
    case Record(ms) => Record(ms.map(m => m._1 -> flattenLabel(m._2)))
    case Label(ls) if ls.size == 1 => ls.head._2
    // case LabelType(ls) if ls.size == 1 => 
    case _ => e
  }

  def flattenLabelType(ms: Map[String, Type], wrap: String = ""): Map[String, Type] = {
    ms.map{
      case(attr, tp) => tp match {
        case LabelType(ls) if ls.size == 1 => (attr -> ls.head._2)
        case y if attr == wrap => (attr -> BagCType(tp))
        case _ => (attr -> tp)
      }
    }
  }

  def project(e: CExpr): Map[String, String] = e match {
    case Record(ms) => ms.map{ case (attr, value) => value match {
      case Project(v, f) => attr -> f
      case v:Variable => attr -> attr
      case _ => ???
    }}
    case _ => Map() //sys.error(s"not supported $e")
  }

  def renameColumns(cols: Map[String, String], keepCols: Set[String] = Set()): String = {
    cols.flatMap{
      case (ncol, ocol) => if (ncol == ocol) Nil
        else if (keepCols(ocol)) List(s"""| .withColumn("$ncol", $$"$ocol")""")
        else List(s"""| .withColumnRenamed("$ocol", "$ncol")""")
    }.mkString("\n")
  }

  def getTypeMap(tp: Type): Map[String, Type] = tp match {
    case BagCType(ttp) => getTypeMap(ttp)
    case RecordCType(ms) => ms
    case LabelType(ls) if ls.size == 1 => Map("_1" -> ls.head._2)
    case _ => ???
  }

  def flatRecord(e1: CExpr, e2: CExpr): Type = {
    RecordCType(getTypeMap(e1.tp) ++ getTypeMap(e2.tp))
  }

  def flatType(es: List[Variable]): RecordCType = {
    RecordCType(es.flatMap{ e => e.tp match {
      case RecordCType(ms) => ms.toList
      case _ => ???
    }}.toMap)
  }

  def flatDictType(e: Type): Type = e match {
    case BagCType(tup) => tup
    case MatDictCType(lbl, bag) => RecordCType(getTypeMap(lbl) ++ getTypeMap(bag))
    case _ => ???
  }

}