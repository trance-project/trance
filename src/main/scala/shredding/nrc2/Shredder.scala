package shredding.nrc2

object Shredder {

//  trait Dict {
//    def isEmpty: Boolean
//    def flat: BagExpr
////    def dict: Dict
//  }
//
//  case object EmptyDict extends Dict {
//    def isEmpty = true
//    def flat: Nothing = throw new NoSuchElementException("flat of empty dictionary")
////    def dict: Nothing = throw new NoSuchElementException("dict of empty dictionary")
//  }
//
//  case class BagDict(flat: BagExpr) extends Dict {
//    def isEmpty = false
//  }
//  case class Dict(flat: BagExpr)


  def shred(e: Expr): Expr = e match {

    case Const(_, _) => e

    case _: VarRef => e

    case ForeachUnion(x, e1, e2) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      val l1 = shred(e1).asInstanceOf[Label]
      val l2 = shred(e2).asInstanceOf[Label]
      val sx = TupleVarDef(x.n, l1.flat.tp.tp)
      Label(ivars, ForeachUnion(sx, l1.flat, l2.flat))

    case Union(e1, e2) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      val l1 = shred(e1).asInstanceOf[Label]
      val l2 = shred(e2).asInstanceOf[Label]
      Label(ivars, Union(l1.flat, l2.flat))

    case Singleton(e1) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      Label(ivars, Singleton(shred(e1).asInstanceOf[TupleExpr]))

    case Tuple(fs) =>
      Tuple(fs.map(f => f._1 -> shred(f._2).asInstanceOf[AttributeExpr]))

    case Let(x, e1, e2) =>
      sys.error("TODO")

    case Mult(e1, e2) =>
      Mult(shred(e1).asInstanceOf[TupleExpr], shred(e2).asInstanceOf[Label])

    case IfThenElse(c, e1, None) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      Label(ivars, IfThenElse(c, shred(e1).asInstanceOf[BagExpr], None))

    case IfThenElse(c, e1, Some(e2)) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      Label(ivars, IfThenElse(c, shred(e1).asInstanceOf[BagExpr], Some(shred(e2).asInstanceOf[BagExpr])))

    case PhysicalBag(_, _) => Label(Nil, e.asInstanceOf[PhysicalBag])

    case Relation(_, _) => Label(Nil, e.asInstanceOf[Relation])

    case _ => sys.error("not implemented")

  }
}
