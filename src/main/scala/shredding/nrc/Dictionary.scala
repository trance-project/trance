package shredding.nrc

import shredding.core._

trait Dictionary {
  this: ShreddedNRC =>

  sealed trait Dict {
    def union(that: Dict): Dict

    def isPrimitiveDict: Boolean
  }

  sealed trait AttributeDict extends Dict {
    def union(that: Dict): AttributeDict
  }

  case object EmptyDict extends AttributeDict {
    def union(that: Dict): AttributeDict = that match {
      case EmptyDict => EmptyDict
      case _ => sys.error("Illegal dictionary union")
    }

    def isPrimitiveDict: Boolean = true
  }

  sealed trait BagDict extends AttributeDict {
    def isPrimitiveDict: Boolean = false

    def flatBagTp: BagType

    def tupleDict: TupleDict
  }

  case class InputBagDict(f: Map[Label, List[Any]], flatBagTp: BagType, tupleDict: TupleDict) extends BagDict {
    def union(that: Dict): InputBagDict = that match {
      case InputBagDict(f2, flatBagTp2, tupleDict2) if flatBagTp == flatBagTp2 =>
        val keys = f.keySet ++ f2.keySet
        val merge = keys.map(k => (k, f.getOrElse(k, Nil) ++ f2.getOrElse(k, Nil))).toMap
        InputBagDict(merge, flatBagTp, tupleDict.union(tupleDict2))
      case _ => sys.error("Illegal dictionary union")
    }
  }

  case class OutputBagDict(lbl: Label, flatBag: BagExpr, tupleDict: TupleDict) extends BagDict {
    def union(that: Dict): OutputBagDict = that match {
      case OutputBagDict(lbl2, flatBag2, tupleDict2) if flatBag.tp == flatBag2.tp =>
        val l = Label(lbl.vars ++ lbl2.vars)
        OutputBagDict(l, Union(flatBag, flatBag2), tupleDict.union(tupleDict2))
      case _ => sys.error("Illegal dictionary union")
    }

    def flatBagTp: BagType = flatBag.tp
  }

  case class TupleDict(fields: Map[String, AttributeDict]) extends Dict {
    def union(that: Dict): TupleDict = that match {
      case TupleDict(fields2) if fields.keySet == fields2.keySet =>
        TupleDict(fields.map { case (k, v) => k -> v.union(fields2(k)) })
      case _ => sys.error("Illegal dictionary union")
    }

    def isPrimitiveDict: Boolean = fields.forall(_._2.isPrimitiveDict)
  }

}
