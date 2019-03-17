package shredding.nrc2

trait Dictionary {
  this: NRC =>

  sealed trait Dict {
    def union(that: Dict): Dict
  }

  trait AttributeDict extends Dict {
    def union(that: Dict): AttributeDict
  }

  case object EmptyDict extends AttributeDict {
    def union(that: Dict): AttributeDict = that match {
      case EmptyDict => EmptyDict
      case _ => sys.error("Illegal dictionary union")
    }
  }

  trait BagDict extends AttributeDict {
    def flatBagTp: BagType

    def tupleDict: TupleDict
  }

  case class OutputBagDict(lbl: LabelExpr, flat: BagExpr, tupleDict: TupleDict) extends BagDict {
    def union(that: Dict): OutputBagDict = that match {
      case OutputBagDict(lbl2, flat2, tupleDict2) if flat.tp == flat2.tp =>
        (lbl, lbl2) match {
          //          case (NewLabel(free1), NewLabel(free2)) =>
          //            val free = free1 ++ free2
          //            OutputBagDict(NewLabel(free), Union(flat, flat2), tupleDict.union(dict2))

          case (LabelId(), LabelId()) =>
            val lbl = LabelId()
            OutputBagDict(lbl, Union(flat, flat2), tupleDict.union(tupleDict2))

          case _ => sys.error("Illegal dictionary union - unknown label type")
        }
      case _ => sys.error("Illegal dictionary union")
    }

    def flatBagTp: BagType = flat.tp
  }

  case class InputBagDict(f: Map[LabelId, List[Any]], flatBagTp: BagType, tupleDict: TupleDict) extends BagDict {
    def union(that: Dict): InputBagDict = that match {
      case InputBagDict(f2, flatTp2, dict2) if flatBagTp == flatTp2 =>
        val keys = f.keySet ++ f2.keySet
        val merge = keys.map(k => (k, f.getOrElse(k, Nil) ++ f2.getOrElse(k, Nil))).toMap
        InputBagDict(merge, flatBagTp, tupleDict.union(dict2))
      case _ => sys.error("Illegal dictionary union")
    }
  }

  case class TupleDict(fields: Map[String, AttributeDict]) extends Dict {
    def union(that: Dict): TupleDict = that match {
      case TupleDict(fields2) if fields.keySet == fields2.keySet =>
        TupleDict(fields.map { case (k, v) => k -> v.union(fields2(k)) })
      case _ => sys.error("Illegal dictionary union")
    }
  }

}