package shredding.algebra

import shredding.core._
import shredding.Utils.ind

trait ScalaGenerator extends BaseStringify {

  var ctx = scala.collection.mutable.Map[Rep, Type]()

  def quotes(e: Rep): Rep = "\""+e+"\""

  def cast(v: Rep): Rep = ctx(v) match {
    case BagCType(_) => s"${v}.asInstanceOf[List[_]]"
    case _ => v
  }
   
  override def inputref(x: String, tp: Type): Rep = {
    ctx(x) = tp 
    x
  }

  override def lt(e1: Rep, e2: Rep): Rep = s"${e1}.asInstanceOf[Int] < ${e2}.asInstanceOf[Int]"
  override def gt(e1: Rep, e2: Rep): Rep = s"${e1}.asInstanceOf[Int] > ${e2}.asInstanceOf[Int]"
  override def lte(e1: Rep, e2: Rep): Rep = s"${e1}.asInstanceOf[Int] <= ${e2}.asInstanceOf[Int]"
  override def gte(e1: Rep, e2: Rep): Rep = s"${e1}.asInstanceOf[Int] >= ${e2}.asInstanceOf[Int]"
  override def and(e1: Rep, e2: Rep): Rep = s"${e1}.asInstanceOf[Boolean] && ${e2}.asInstanceOf[Boolean]"
  override def not(e1: Rep): Rep = s"!${e1}.asInstanceOf[Boolean]"
  override def or(e1: Rep, e2: Rep): Rep = s"${e1}.asInstanceOf[Boolean] || ${e2}.asInstanceOf[Boolean]"
  override def emptysng: Rep = "Nil"
  override def unit: Rep = "Unit" 
  override def sng(x: Rep): Rep = s"List(${x})"
  override def record(fs: Map[String, Rep]): Rep = s"""Map(${fs.map(f => quotes(f._1) + " -> " + f._2).mkString(",")})"""
  override def project(e1: Rep, field: String): Rep = e1 match {
    case m if e1.startsWith("Map") => s"${e1}.getOrElse(${quotes(field)}, None)"
    case m if e1.startsWith("(") => s"${e1}.asInstanceOf[Product].productElement(${field})"
    case m =>  
      val v = e1.split("\\.").filter(!_.contains("asInstanceOf[List[_]]"))
      val v0 = v.mkString(".")
      val v1 = if (v.size > 1 && !v.last.contains("productElement") && !v.last.contains("getOrElse")) { 
                  v.dropRight(1).mkString(".") } else { v0 }
      val tp = ctx(v1)
      tp match {
        case t:TTupleType => 
          val s = s"${e1}.asInstanceOf[Product].productElement(${field.toInt})"
          ctx(s) = t(field.toInt)
          cast(s)
        case t:RecordCType => 
          val s = s"${e1}.asInstanceOf[Map[String,_]].getOrElse(${quotes(field)}, None)"
          ctx(s) = t(field)
          cast(s)
        case t:BagDictCType =>
          val s = s"${e1}.asInstanceOf[Product].productElement(${field.toInt})"
          ctx(s) = t(field)
          cast(s)
        case t:TupleDictCType =>
          val s = s"${e1}.asInstanceOf[Map[String, _]].getOrElse(${quotes(field)}, None)"
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
    // s"val ${e(v)} = ${e1}\n" // adjust type here
    s"val $v = $e1\n${e(v)}"
  }
  override def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    val v0 = e1.split("\\.").filter(!_.contains("asInstanceOf[List[_]]")).mkString(".")
    val v = freshVar(v0)
    val filt = p(v) match { case "true" => ""; case _ => s".withFilter(${v} => ${p(v)})"}
    e(v) match {
      case "1" => s"${e1}${filt}.map(${v} => 1).sum"
      case t => s"""${e1}${filt}.flatMap(${v} => 
        | ${ind(t)})""".stripMargin  
    }
  }
  override def dedup(e1: Rep): Rep = s"${e1}.distinct"
  override def named(n: String, e: Rep): Rep = {
    ctx(n) = StringType
    s"val ${n} = ${e}.asInstanceOf[List[_]]\n"
  }
  override def linset(e: List[Rep]): Rep = s"${e.map("| "+_).mkString("\n")}"

  override def label(id: Int, fs: Map[String, Rep]): Rep = {
    s"""(${id}, ${fs.map(f => "ctx.getOrElseUpdate("+quotes(f._1)+","+f._2+")")})"""
  }
  override def extract(lbl: Rep, exp: Rep): Rep = exp
  //  fs.map(f => s"val ${f._1} = ctx.getOrElseUpdate(${f._2})").mkString("\n")
  override def emptydict: Rep = s"()"
  override def bagdict(lbl: Rep, flat: Rep, dict: Rep): Rep = {
    s"(${flat}.asInstanceOf[List[_]].map(v => (${lbl}, v)), ${dict})"
  }
  override def tupledict(fs: Map[String, Rep]): Rep = s"(${fs.map(f => quotes(f._1) -> f._2).mkString(",")})"
  

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

object ScalaNamedGenerator {
  var types = Map[Type, String]()

  implicit def expToString(e: CExpr): String = generate(e)

  def generateTypeDef(tp: Type): String = tp match {
    case RecordCType(fs) =>
     val name = types(tp)
      s"case class $name(${fs.map(x => s"${x._1}: ${generateType(x._2)}").mkString(", ")})" 
    case _ => sys.error("unsupported type "+tp)
  }

  def generateType(tp: Type): String = tp match {
    case RecordCType(_) if types.contains(tp) => types(tp)
    case IntType => "Int"
    case StringType => "String"
    case BoolType => "Boolean"
    case BagCType(tp) => s"List[${generateType(tp)}]"
    case BagDictCType(flat, dict) => s"(${generateType(flat)}, ${generateType(dict)})"
    case TTupleType(fs) => s"(${fs.map(f => generateType(f))})"
    case TupleDictCType(fs) if !fs.isEmpty => generateType(RecordCType(fs))
    case LabelType(fs) if fs.filter(_._1 != "RF").isEmpty => "Int" 
    case LabelType(fs) => generateType(RecordCType())
    case _ => sys.error("not supported type " + tp)
  }

  def generateHeader(): String = {
    types.map(x => generateTypeDef(x._1)).mkString("\n")
  }

  def handleType(tp: Type, givenName: Option[String] = None): Unit = {
    if(!types.contains(tp)) {
      tp match {
        case RecordCType(fs) =>
          fs.foreach(f => handleType(f._2))
          val name = givenName.getOrElse("Record" + Variable.newId)
          types = types + (tp -> name)
        case BagCType(tp) =>
          handleType(tp, givenName)
        case LabelType(fs) if !fs.filter(_._1 != "RF").isEmpty => handleType(RecordCType(fs), Some("Label"+Variable.newId))
        case BagDictCType(BagCType(TTupleType(ls)), dict) => handleType(ls.last, givenName)
        case TupleDictCType(fs) =>
          val ffs = fs.filter(_._2 != EmptyDictCType)
          if (!ffs.isEmpty) { handleType(RecordCType(ffs)) } else { () }
        case _ => ()
      }
      
    }
  }

  def generate(e: CExpr): String = e match {
    case Variable(name, _) => name
    case InputRef(name, tp) => 
      handleType(tp, Some(name))
      name
    case Comprehension(e1, v, p, e) =>
      val filt = p match { case Constant(true) => ""; case _ => s".withFilter({${generate(v)} => ${generate(p)}})"}
      e match {
        case Constant(1) => s"${generate(e1)}${filt}.map({${generate(v)} => 1}).sum"
        case t => s"""${generate(e1)}${filt}.flatMap({${generate(v)} => 
          | ${ind(generate(t))}})""".stripMargin  
      }
    case Bind(v, Record(fs), e2) => {
      handleType(v.tp)
      s"val ${generate(v)} = ${generateType(v.tp)}(${fs.map(f => generate(f._2)).mkString(", ")})\n${generate(e2)}"
    }
    case Bind(v, e1, e2) =>
      s"val ${generate(v)} = ${generate(e1)}\n${generate(e2)}"
    case Project(e, field) => s"${generate(e)}.$field"
    case Equals(e1, e2) => s"${generate(e1)} == ${generate(e2)}"
    case Lt(e1, e2) => s"${generate(e1)} < ${generate(e2)}"
    case Gt(e1, e2) => s"${generate(e1)} > ${generate(e2)}"
    case Lte(e1, e2) => s"${generate(e1)} <= ${generate(e2)}"
    case Gte(e1, e2) => s"${generate(e1)} >= ${generate(e2)}"
    case And(e1, e2) => s"${generate(e1)} && ${generate(e2)}"
    case Or(e1, e2) => s"${generate(e1)} || ${generate(e2)}"
    case Not(e1) => s"!(${generate(e1)})"
    case Constant(x) => x match {
      case s:String => s""""$s""""
      case _ => x.toString
    }
    case Sng(e) => s"List(${generate(e)})"
    case CUnit => "()"
    case EmptySng => "Nil"
    case If(cond, e1, e2) => e2 match {
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
    case Merge(e1, e2) => s"${generate(e1) ++ generate(e2)}"
    case CDeDup(e1) => s"${generate(e1)}.distinct"
    case CNamed(n, e) => s"/* bnamed */${generate(e)}/* enamed */"
    case LinearCSet(exprs) => s"""${exprs.map(generate(_)).mkString("\n")}"""
    case Label(id, fs) if !fs.filter(_._1 != "RF").isEmpty => 
      handleType(e.tp)
      s"${generateType(e.tp)}(${fs.map(f => generate(f._2)).mkString(", ")})"
    case Label(id, fs)  => id.toString
    case Extract(lbl @ Label(id, fs), exp) => fs.map(f => s"val ${f._1} = ${generate(f._2)}").mkString("\n")+generate(exp)
    case Extract(lbl, exp) => generate(exp)
    case EmptyCDict => "()"
    case BagCDict(lbl, flat, dict) => 
      s"(${generate(flat)}.map(v => (${generate(lbl)}, v)), ${generate(dict)})"
    case TupleCDict(fs) => generate(Record(fs))
    case _ => sys.error("not supported "+e)
  }

}
