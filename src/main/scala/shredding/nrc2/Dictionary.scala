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
    def flatBag(lbl: LabelExpr): BagExpr

    def flatBagTp: BagType

    def tupleDict: TupleDict
  }

  case class OutputBagDict(lbl: LabelExpr, flat: BagExpr, tupleDict: TupleDict) extends BagDict {

    def flatBag(l: LabelExpr): BagExpr = { assert(l == lbl); flat }

    def flatBagTp: BagType = flat.tp

    def union(that: Dict): OutputBagDict = that match {
      case OutputBagDict(lbl2, flat2, dict2) if flat.tp == flat2.tp =>
        (lbl, lbl2) match {
          case (NewLabel(free1), NewLabel(free2)) =>
            val free = free1 ++ free2
            OutputBagDict(NewLabel(free), Union(flat, flat2), tupleDict.union(dict2))
//          case (RLabelId(tp1), RLabelId(tp2)) if tp1 == tp2 =>
//            BagDict(RLabelId(tp1), )

          case _ => sys.error("Illegal dictionary union - unknown label type")
        }
      case _ => sys.error("Illegal dictionary union")
    }

//    def lookup(l: NewLabel): BagExpr = { assert(lbl == l); flat }

//    def tp: BagType = flat.tp
  }

  case class InputBagDict(f: Map[LabelId, List[Any]], flatBagTp: BagType, tupleDict: TupleDict) extends BagDict {

    def flatBag(l: LabelExpr): BagExpr = Lookup(l, this)

//    def flatBag(l: LabelExpr): BagExpr = l match {
//      case l2: LabelId =>
////        Lookup(l2, )
////        Lookup("dummy", f(l2), flatBagTp)
//      case _ => sys.error("Illegal dictionary lookup in input bag dictionary")
//    }

    def union(that: Dict): InputBagDict = that match {
      case InputBagDict(f2, flatTp2, dict2) if flatBagTp == flatTp2 =>
        val keys = f.keySet ++ f2.keySet
        val merge = keys.map(k => (k, f.getOrElse(k, Nil) ++ f2.getOrElse(k, Nil))).toMap
        InputBagDict(merge, flatBagTp, tupleDict.union(dict2))
      case _ => sys.error("Illegal dictionary union")
    }

//    // TODO: fix this
//    def lookup(l: LabelExpr): BagExpr = Lookup(l, null)
  }

  case class TupleDict(fields: Map[String, AttributeDict]) extends Dict {
    def union(that: Dict): TupleDict = that match {
      case TupleDict(fields2) if fields.keySet == fields2.keySet =>
        TupleDict(fields.map { case (k, v) => k -> v.union(fields2(k)) })
      case _ => sys.error("Illegal dictionary union")
    }
  }

}