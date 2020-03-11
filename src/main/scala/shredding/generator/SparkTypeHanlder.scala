package shredding.generator

import shredding.core._
import shredding.wmcc._
import shredding.utils.Utils.ind

trait SparkTypeHandler {

  var types: Map[Type, String]
  var typelst: Seq[Type] = Seq()

  def kvName(x: String)(implicit s: Int = -1): String = x match {
    case "k" if s == 2 => "_1"
    case "v" if s == 2 => "_2"
    case "key" if s == 2 => "_1"
    case "value" if s == 2 => "_2"
    case _ => x
  }

  def generateTypeDef(tp: Type): String = tp match {
    case LabelType(fs) =>
     val name = types(tp)
     val fsize = fs.size
      s"case class $name(${fs.map(x => s"${kvName(x._1)(fsize)}: ${generateType(x._2)}").mkString(", ")})"
    case RecordCType(fs) =>
     val name = types(tp)
     val fsize = fs.size
      s"case class $name(${fs.map(x => s"${kvName(x._1)(fsize)}: ${generateType(x._2)}").mkString(", ")})"
    case _ => sys.error("unsupported type "+tp)
  }

  def generateType(tp: Type): String = tp match {
    case RecordCType(fs) if fs.isEmpty => "Unit"
    case LabelType(fs) if fs.isEmpty => "Unit"
    //case RecordCType(fs) if fs.keySet == Set("_1", "_2") => fs.generateType(fs._2).mkString("(",",",")")
    // case RecordCType(ms) if ms.values.toList.contains(EmptyCType) => ""
    case RecordCType(_) if types.contains(tp) => types(tp)
    case LabelType(fs) => generateType(RecordCType(fs))
    case IntType => "Int"
    case StringType => "String"
    case BoolType => "Boolean"
    case DoubleType => "Double"
    case LongType => "Long"
    case TTupleType(fs) => s"(${fs.map(generateType(_)).mkString(",")})"
    case BagCType(tp) => s"Vector[${generateType(tp)}]" //combineByKey, etc. may need this to be iterable
    case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) =>
      dict match {
        case TupleDictCType(ds) if !ds.filter(_._2 != EmptyDictCType).isEmpty =>
          s"(Vector[(${generateType(fs.head)}, ${generateType(fs.last)})], ${generateType(dict)})"
        case _ => s"(Vector[(${generateType(fs.head)}, ${generateType(fs.last)})], Unit)"
      }
    case TupleDictCType(fs) if !fs.filter(_._2 != EmptyDictCType).isEmpty =>
      generateType(RecordCType(fs.filter(_._2 != EmptyDictCType)))
    case EmptyCType => "Unit"
    case _ => sys.error("not supported type " + tp)
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
          handleType(RecordCType(fs))
        case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) =>
          val nid = Variable.newId
          handleType(fs.last, Some(givenName.getOrElse("")))
        case TupleDictCType(fs) =>
          val ffs = fs.filter(_._2 != EmptyDictCType)
          if (!ffs.isEmpty) { handleType(RecordCType(ffs), givenName) } else { () }
        case _ => ()
      }
    }
  }
  
}