package shredding.algebra

import shredding.core._
import shredding.Utils.ind

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
    case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) => dict match {
        case TupleDictCType(ds) if !ds.filter(_._2 != EmptyDictCType).isEmpty => 
          s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], ${generateType(dict)})"
        case _ => s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], Unit)"
      }
    case TupleDictCType(fs) if !fs.filter(_._2 != EmptyDictCType).isEmpty => 
      generateType(RecordCType(fs.filter(_._2 != EmptyDictCType)))
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
        case LabelType(fs) if !fs.filter(_._1 != "RF").isEmpty => 
          val name = givenName.getOrElse("Label" + Variable.newId)
          handleType(RecordCType(fs), Some(name))
        case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) =>
          val nid = Variable.newId 
          handleType(fs.head, Some(givenName.getOrElse("")+s"Label$nid"))
          handleType(fs.last, Some(givenName.getOrElse("")+s"Flat$nid"))
          handleType(dict, Some(givenName.getOrElse("")+s"Dict$nid"))
        case TupleDictCType(fs) =>
          val ffs = fs.filter(_._2 != EmptyDictCType)
          if (!ffs.isEmpty) { handleType(RecordCType(ffs), givenName) } else { () }
        case _ => ()
      }
      
    }
  }

  def generate(e: CExpr): String = e match {
    case Variable(name, _) => name
    case InputRef(name, tp) => 
      handleType(tp, Some("Input_"+name))
      name
    case Comprehension(e1, v, p, e) =>
      val filt = p match { case Constant(true) => ""; case _ => s".withFilter({${generate(v)} => ${generate(p)}})"}
      e match {
        case Constant(1) => s"${generate(e1)}${filt}.map({${generate(v)} => 1}).sum"
        // nested sum, // .. val x0 = x.map({ y => 1 }); x0
        case t @ Bind(_, _, Bind(_, Comprehension(_,_,_,Constant(1)), _)) =>
          s"""${generate(e1)}${filt}.flatMap({${generate(v)} =>              
            | ${ind(generate(t).replace(".sum", ""))}}).sum""".stripMargin  
        case t => 
          s"""${generate(e1)}${filt}.flatMap({${generate(v)} => 
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
    case CNamed(n, e) => generate(e)
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
