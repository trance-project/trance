package shredding.generator

import shredding.core._
import shredding.plans._
import shredding.utils.Utils.ind

case class COption(e: CExpr) extends CExpr {
  def tp: OptionType = OptionType(e.tp)
}

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

  def getTypeMap(tp: Type, outer: Boolean = false): Map[String, Type] = tp match {
    case BagCType(ttp) => getTypeMap(ttp, outer)
    case RecordCType(ms) => if (outer) ms.map{ case (attr, ttp) => (attr, OptionType(ttp)) } else ms
    case LabelType(ls) if ls.size == 1 => Map("_1" -> ls.head._2)
    case TTupleType(ts) => ts.map(t => getTypeMap(t, outer)).flatten.toMap
    case _ => Map()//sys.error(s"not supported $tp")
  }

  def flatRecord(e1: Type, e2: Type, outer: Boolean = false): RecordCType = {
    RecordCType(getTypeMap(e1) ++ getTypeMap(e2, outer))
  }

  def flatType(es: List[Variable], index: Boolean = false, wrapOption: String = "", wrapNested: Boolean = false): RecordCType = {
    val rmap = es.zipWithIndex.flatMap{ case (e, id) => e.tp match {
      case RecordCType(ms) => if (wrapOption != "") {
        ms.map{
          case ("index", tp) => ("index", tp)
          case (attr, btp:BagCType) if (attr == wrapOption) => (attr, OptionType(btp))
          case (attr, tp) if (id > 0 && wrapOption == "null") => (attr, OptionType(tp))
          case (attr, tp) if (id > 0 && wrapNested) => (attr, OptionType(tp))
          case (attr, tp) => (attr, tp)
        }.toList
      }else ms.toList
      case _ => Nil //sys.error(s"not supported ${e.tp}")
    }}.toMap
    if (index) RecordCType(rmap ++ Map("index" -> LongType)) else RecordCType(rmap)
  }

  def unnestDataframe(v1: Variable, v2: Variable, field: String, nulls: Boolean = false): Record = {
    Record(v1.tp match {
      case RecordCType(ms) => ms.flatMap{
        case (attr, BagCType(_)) if (attr == field) => v2.tp.asInstanceOf[RecordCType].attrTps.map{
          case (nattr, ntp) => if (nulls) (nattr,COption(Null)) else (nattr, COption(Project(v2, nattr)))
        }
        case (attr, OptionType(BagCType(_))) if (attr == field) => v2.tp.asInstanceOf[RecordCType].attrTps.map{
          case (nattr, ntp) => if (nulls) (nattr,COption(Null)) else (nattr, COption(Project(v2, nattr)))
        }
        case (attr, tp) => List((attr,Project(v1, attr)))
      }
      case _ => ???
    })
  }

  def flatDictType(e: Type): Type = e match {
    case BagCType(tup) => tup
    case MatDictCType(lbl, bag) => RecordCType(getTypeMap(bag) ++ getTypeMap(lbl))
    case _ => ???
  }

  def addIndex(e: Type): RecordCType = e match {
    case BagCType(RecordCType(ms)) => RecordCType(ms + ("index" -> LongType))
    case RecordCType(ms) => RecordCType(ms + ("index" -> LongType))
    case _ => ???
  }

  def wrapOption(e: Type, f: String = ""): Type = e match {
    case RecordCType(ms1) => RecordCType(ms1.map{
      case (attr, RecordCType(ms2)) => 
        (attr, RecordCType(ms2.map(m => m._1 -> OptionType(m._2))))
      case (attr, rtp) => (attr, rtp)
    })
    case _ => e
  }

  def getIndex(vs: List[Variable], f: String): String = {
    val len = vs.size - 1
    vs.zipWithIndex.foreach{
      case (variable, index) => 
        if (getTypeMap(variable.tp).keySet(f)) 
          if (index < len) return "._1"
          else return "._2"
    }
    return "";
  }

}
