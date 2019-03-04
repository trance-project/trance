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
      case _ => throw new IllegalArgumentException("Illegal dictionary union")
    }
  }

  case class BagDict(lbl: NewLabel, flat: BagExpr, dict: TupleDict) extends AttributeDict {
    def union(that: Dict): BagDict = that match {
      case BagDict(lbl2, flat2, dict2) if flat.tp == flat2.tp =>
        val free = (lbl.free ++ lbl2.free).distinct
        BagDict(NewLabel(free), Union(flat, flat2), dict.union(dict2))
      case _ => throw new IllegalArgumentException("Illegal dictionary union")
    }

    def lookup(l: NewLabel): BagExpr = { assert(lbl == l); flat }
  }

//  case class EagerBagDict(f: Map[NewLabel, BagExpr], dict: TupleDict) extends BagDict {
//    def union(that: Dict): BagDict = that match {
//      case EagerBagDict(f2, dict2) => EagerBagDict(f ++ f2, dict.union(dict2))
//      case _ => throw new IllegalArgumentException("Illegal dictionary union")
//    }
//
//    def lookup(l: NewLabel): BagExpr = Lookup(l, this)
//  }

  case class TupleDict(fields: Map[String, AttributeDict]) extends Dict {
    def union(that: Dict): TupleDict = that match {
      case TupleDict(fields2) if fields.keySet == fields2.keySet =>
        TupleDict(fields.map { case (k, v) => k -> v.union(fields(k)) })
      case _ => throw new IllegalArgumentException("Illegal dictionary union")
    }
  }

}