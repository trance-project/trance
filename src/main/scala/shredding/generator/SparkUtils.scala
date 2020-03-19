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
    case IntType => "agg(_+_)"
    case DoubleType => "agg(_+_)"
    case _ => "group(_++_)"
  } 

  def drop(tp: Type, v: Variable, field: String): CExpr = tp match {
    case TTupleType(fs) => 
      Tuple(fs.drop((field.replace("_", "").toInt-1)).zipWithIndex.map{ case (t, i) 
        => Project(v, "_"+i) })
    case RecordCType(fs) => 
      Record((fs - field).map{ case (attr, atp) => attr -> Project(v, attr)})
    case _ => sys.error(s"unsupported type $tp")
  }

  def projectBag(e: CExpr, vs: List[Variable]): (String, String, List[CExpr], List[CExpr]) = e match {
    case Bind(v, Project(v2 @ Variable(n,tp), field), e2) => 
      val nvs1 = vs.map( v3 => if (v3 == v2) drop(tp, v2, field) else v3)
      val nvs2 = vs.map( v3 => if (v3 == v2) v2.nullValue else v3)
      (n, field, nvs1, nvs2)
    case _ => sys.error(s"unsupported bag projection $e")
  }

  def isDomain(e: CExpr): Boolean = e.tp match {
    case BagCType(RecordCType(ms)) => ms get "lbl" match {
      case Some(tp:LabelType) => true
      case _ => false
    }
    case _ => false
  }

  def hasLabel(e: Type): Boolean = e match {
    case TTupleType(ls) => ls.filter(t => hasLabel(t)).nonEmpty
    case RecordCType(ms) => ms.keySet == Set("lbl")
    case LabelType(_) => true
    case _ => false
  }

  def isDictRecord(e: CExpr): Boolean = e match {
    case Record(fs) => (fs.keySet == Set("_1","_2") || fs.keySet == Set("key", "value"))
    case _ => sys.error("unsupported type")
  }

  def runJob(n: String, last:Boolean = false): String = {
    if (!n.contains("ctx")){
      val fcomment = if (last) "" else "//"
      s"""|//$n.cache
          |//$n.print
          |${fcomment}$n.evaluate"""
    }else ""
  }

}