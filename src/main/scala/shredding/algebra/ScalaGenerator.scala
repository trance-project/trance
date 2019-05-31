package shredding.algebra

import shredding.core._
import shredding.Utils.ind

trait ScalaGenerator extends BaseStringify {

  var ctx = scala.collection.mutable.Map[Rep, Type]()

  def quotes(e: Rep): Rep = "\""+e+"\""
  def input(x: Any): Rep = x match {
    case l @ (head :: tail) => s"List(${l.map(input(_)).mkString(",")})"
    case m:Map[_,_] => "Map("+m.toList.map(v => quotes(v._1.asInstanceOf[Rep]) +" -> "+input(v._2)).mkString(",")+")"
    case _ => x+""
  }

  def cast(v: Rep): Rep = ctx(v) match {
    case IntType => s"${v}.asInstanceOf[Int]"
    case BagCType(_) => s"${v}.asInstanceOf[List[_]]"
    case _ => v
  } 
 
  override def emptysng: Rep = "Nil"
  override def unit: Rep = "Unit" //??
  override def sng(x: Rep): Rep = s"List(${x})"
  override def record(fs: Map[String, Rep]): Rep = s"Map(${fs.map(f => quotes(f._1) + " -> " + f._2).mkString(",")})"
  override def project(e1: Rep, field: String): Rep = e1 match {
    case m if e1.startsWith("Map") => s"${e1}.getOrElse(${quotes(field)}, None)"
    case m if e1.startsWith("(") => s"${e1}.asInstanceOf[Product].productElement(${field})"
    case m =>  
      val v = e1.split("\\.").filter(!_.contains("asInstanceOf"))
      val v0 = v.mkString(".")
      val v1 = if (v.size > 1) { v.dropRight(1).mkString(".") } else { v0 }
      val tp = ctx(v1)
      tp match {
        case t:TTupleType => 
          val s = s"${e1}._${field.toInt+1})"
          ctx(s) = t(field.toInt)
          cast(s)
        case t:KVTupleCType => // deprecated type
          val s = s"${e1}._${field.toInt+1}"
          ctx(s) = t(field)
          cast(s)
        case t:RecordCType => 
          val s = s"${e1}.getOrElse(${quotes(field)}, None)"
          ctx(s) = t(field)
          cast(s)
        case t => sys.error("projecting on invalid type "+t+" at "+e1+" with "+field)
      }
  }
  override def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = e2 match {
    case Some(a) => s"""
      | if (${cond}) { 
      | ${ind(e1)})
      | } else { 
      | ${ind(a)} 
      | }""".stripMargin
    case None => s"""
      | if (${cond})
      | ${ind(e1)}
      | else  Nil """.stripMargin
  }
  override def merge(e1: Rep, e2: Rep): Rep = s"${e1} ++ ${e1}"
  override def bind(e1: Rep, e: Rep => Rep): Rep = {
    val v = Variable.fresh(StringType).quote
    s"val ${v} = ${e(v)}\n ${e1}\n" // adjust type here
  }
  override def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    val v0 = e1.split("\\.").filter(!_.contains("asInstanceOf")).mkString(".")
    val v = freshVar(v0)
    val agg = e(v) match { case "1" => ".sum"; case _ => "" }
    val filt = p(v) match { case "true" => ""; case _ => s".withFilter(${v} => ${p(v)})"}
    s"""${e1}${filt}.map(${v} => 
        | ${ind(e(v))})${agg}""".stripMargin  
  }
  override def dedup(e1: Rep): Rep = s"${e1}.distinct"
  override def named(n: String, e: Rep): Rep = s"val ${n} = ${e}\n"
  override def linset(e: List[Rep]): Rep = s"${e.mkString("\n")}"

  def varType(tp: Type): Variable = tp match {
    case BagCType(tt) => Variable.fresh(tt)
    case t => Variable.fresh(t) 
  }

  def freshVar(str: String): String = {
    val v = varType(ctx(str))
    ctx(v.quote) = v.tp
    v.quote
  }
 
}
