package shredding.core

import shredding.Utils.ind

trait BaseDict

  trait Dict extends BaseDict{ self =>
    def union(that: Dict): Dict
    def toString: String
  }

  trait AttributeDict extends Dict {
    def union(that: Dict): AttributeDict
    def toString: String
  }

  case object EmptyDict extends AttributeDict {
    def union(that: Dict): AttributeDict = that match {
      case EmptyDict => EmptyDict
      case _ => sys.error("Illegal dictionary union")
    }
    override def toString: String = "Nil"
  }

  trait BagDict extends AttributeDict {
    def flatBagTp: BagType

    def tupleDict: TupleDict
    def toString: String 
  }

  case class InputBagDict(f: Map[Label, List[Any]], flatBagTp: BagType, tupleDict: TupleDict) extends BagDict {
    def union(that: Dict): InputBagDict = that match {
      case InputBagDict(f2, flatTp2, dict2) if flatBagTp == flatTp2 =>
        val keys = f.keySet ++ f2.keySet
        val merge = keys.map(k => (k, f.getOrElse(k, Nil) ++ f2.getOrElse(k, Nil))).toMap
        InputBagDict(merge, flatBagTp, tupleDict.union(dict2))
      case _ => sys.error("Illegal dictionary union")
    }
    override def toString: String = 
      s"""|( $f,
          |${ind(tupleDict.toString)}
          |)""".stripMargin

  }

  case class TupleDict(fields: Map[String, AttributeDict]) extends Dict {
    def union(that: Dict): TupleDict = that match {
      case TupleDict(fields2) if fields.keySet == fields2.keySet =>
        TupleDict(fields.map { case (k, v) => k -> v.union(fields2(k)) })
      case _ => sys.error("Illegal dictionary union")
    }
    override def toString: String = 
      s"(${fields.map(f => f._1 + " := " + f._2.toString).mkString(", ")})"
  }

