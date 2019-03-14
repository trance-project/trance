package shredding.nrc2

trait Dictionary {
  this: NRC =>

  sealed trait Dict {
    def union(that: Dict): Dict
  }

  trait AttributeDict extends Dict {
    def union(that: Dict): AttributeDict
  }

//  trait BagDict extends AttributeDict {
//    def union(that: Dict): BagDict
//
//    def lookup(lbl: NewLabel): BagExpr
//  }

  case object EmptyDict extends AttributeDict {
    def union(that: Dict): AttributeDict = that match {
      case EmptyDict => EmptyDict
      case _ => sys.error("Illegal dictionary union")
    }
  }

  case class BagDict(lbl: LabelExpr, flat: BagExpr, dict: TupleDict) extends AttributeDict {
    def union(that: Dict): BagDict = that match {

      case BagDict(lbl2, flat2, dict2) if flat.tp == flat2.tp =>
        (lbl, lbl2) match {
          case (NewLabel(free1), NewLabel(free2)) =>
            val free = free1 ++ free2
            BagDict(NewLabel(free), Union(flat, flat2), dict.union(dict2))
//          case (RLabelId(tp1), RLabelId(tp2)) if tp1 == tp2 =>
//            BagDict(RLabelId(tp1), )

          case _ => sys.error("Illegal dictionary union - unknown label type")
        }
      case _ => sys.error("Illegal dictionary union")
    }

    def lookup(l: NewLabel): BagExpr = { assert(lbl == l); flat }

    def tp: BagType = flat.tp
  }

  case class MaterializedDict(f: Map[RLabelId, List[Any]], dict: TupleDict, tp: BagType) extends AttributeDict {
    def union(that: Dict): MaterializedDict = that match {
      case MaterializedDict(f2, dict2, tp2) if tp == tp2 =>
        val keys = f.keySet ++ f2.keySet
        val merge = keys.map(k => (k, f.getOrElse(k, Nil) ++ f2.getOrElse(k, Nil))).toMap
        MaterializedDict(merge, dict.union(dict2), tp)
      case _ => sys.error("Illegal dictionary union")
    }

    // TODO: fix this
    def lookup(l: LabelExpr): BagExpr = Lookup(l, null)
  }

  case class TupleDict(fields: Map[String, AttributeDict]) extends Dict {
    def union(that: Dict): TupleDict = that match {
      case TupleDict(fields2) if fields.keySet == fields2.keySet =>
        TupleDict(fields.map { case (k, v) => k -> v.union(fields2(k)) })
      case _ => sys.error("Illegal dictionary union")
    }
  }

}