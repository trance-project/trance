package shredding.generator

import shredding.core._
import shredding.wmcc._
import shredding.utils.Utils.ind

/**
  * Generates Scala code, assumes nodes are compiled from ANF
  */

class ScalaNamedGenerator(inputs: Map[Type, String] = Map()) {
  var types:Map[Type,String] = inputs
  var typelst:Seq[Type] = Seq()//inputs.map(_._1).toSeq

  implicit def expToString(e: CExpr): String = generate(e)
  
  def kvName(x: String): String = x match {
    case "k" => "_1"
    case "v" => "_2" 
    case _ => x
  } 

  def generateTypeDef(tp: Type): String = tp match {
    case RecordCType(fs) =>
     val name = types(tp)
      s"case class $name(${fs.map(x => s"${kvName(x._1)}: ${generateType(x._2)}").mkString(", ")})" 
    case _ => sys.error("unsupported type "+tp)
  }

  def generateType(tp: Type): String = tp match {
    case RecordCType(_) if types.contains(tp) => types(tp)
    case IntType => "Int"
    case StringType => "String"
    case BoolType => "Boolean"
    case DoubleType => "Double"
    case BagCType(tp) => s"List[${generateType(tp)}]" 
    case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) => 
      dict match {
        case TupleDictCType(ds) if !ds.filter(_._2 != EmptyDictCType).isEmpty => 
          //s"(List[${generateType(RecordCType("_1" -> fs.head, "_2" -> fs.last))}], ${generateType(dict)})"
          s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], ${generateType(dict)})"
        case _ => //s"(List[${generateType(RecordCType("_1" -> fs.head, "_2" -> fs.last))}], Unit)"
          s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], Unit)"
      }
    case TupleDictCType(fs) if !fs.filter(_._2 != EmptyDictCType).isEmpty => 
      generateType(RecordCType(fs.filter(_._2 != EmptyDictCType)))
    case LabelType(fs) if fs.isEmpty => "Int" 
    case LabelType(fs) => generateType(RecordCType(fs))
    case _ => sys.error("not supported type " + tp)
  }

  def generateHeader(): String = {
    typelst.map(x => generateTypeDef(x)).mkString("\n")
  }

  def handleType(tp: Type, givenName: Option[String] = None): Unit = {
    if(!types.contains(tp)) {
      tp match {
        case RecordCType(fs) =>
          fs.foreach(f => handleType(f._2))
          val name = givenName.getOrElse("Record" + Variable.newId)
          types = types + (tp -> name)
          typelst = typelst :+ tp 
        case BagCType(tp) =>
          handleType(tp, givenName)
        case LabelType(fs) if !fs.isEmpty => 
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
      val acc = "acc" + Variable.newId()
      val cur = generate(v)
      def conditional(thenp: String, elsep: String): String = 
        p match {
          case Constant(true) => s"{${ind(thenp)}}"
          case _ => s"if({${generate(p)}}) {${ind(thenp)}} else {${ind(elsep)}}"
        }
      e.tp match {
        case IntType =>
          s"${generate(e1)}.foldLeft(0)(($acc, $cur) => \n${ind(conditional(s"$acc + ${generate(e)}", s"$acc"))})"
        case DoubleType =>
          s"${generate(e1)}.foldLeft(0.0)(($acc, $cur) => \n${ind(conditional(s"$acc + ${generate(e)}", s"$acc"))})"
        case _ =>
          s"${generate(e1)}.flatMap($acc =>  \n${ind(conditional(generate(e), "Nil"))})"
      }
      
      // val filt = p match { case Constant(true) => ""; case _ => s".withFilter({${generate(v)} => ${generate(p)}})"}
      // e match {
      //   case Constant(1) => s"${generate(e1)}${filt}.count"
      //   // nested sum, // .. val x0 = x.map({ y => 1 }); x0
      //   case t => 
      //     val gt = generate(t)
      //     if (gt.contains(".sum")){
      //       s"""${generate(e1)}${filt}.flatMap({${generate(v)} =>
      //           | ${ind(gt.replace(".sum", ""))}}).sum""".stripMargin
      //     }else{
      //       s"""${generate(e1)}${filt}.flatMap({${generate(v)} => 
      //       | ${ind(gt)}})""".stripMargin  
      //     }
      //   }
    case Record(fs) => {
      val tp = e.tp
      handleType(tp)
      s"${generateType(tp)}(${fs.map(f => generate(f._2)).mkString(", ")})"
    }
    case Bind(v, e1, e2) =>
      s"val ${generate(v)} = ${generate(e1)}\n${generate(e2)}"
    case Project(e, field) => s"${generate(e)}.${kvName(field)}"
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
    case WeightedSng(e, q) => s"(1 to ${generate(q)}.asInstanceOf[Int]).map(v => ${generate(e)})"
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
    case LinearCSet(exprs) => 
      //val names = exprs.map(cn => cn match { case CNamed(n, e1) => n }).mkString(",")
      s"""(${exprs.map(generate(_)).mkString(",")})"""
    case EmptyCDict => "()"
    case BagCDict(lbl, flat, dict) => 
      s"(${generate(flat)}.map(v => (${generate(lbl)}, v)), ${generate(dict)})"
    case TupleCDict(fs) => generate(Record(fs))
    case _ => sys.error("not supported "+e)
  }

}
