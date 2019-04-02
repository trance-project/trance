package shredding.nrc2

/**
  * Extension methods for NRC expressions
  */
trait NRCImplicits {
  this: NRC =>

  implicit class TraversalOps(e: Expr) {

    def collect[A](f: PartialFunction[Expr, List[A]]): List[A] =
      f.applyOrElse(e, (ex: Expr) => ex match {
        case p: Project => p.tuple.collect(f)
        case ForeachUnion(_, e1, e2) => e1.collect(f) ++ e2.collect(f)
        case Union(e1, e2) => e1.collect(f) ++ e2.collect(f)
        case Singleton(e1) => e1.collect(f)
        case Tuple(fs) => fs.flatMap(_._2.collect(f)).toList
        case Let(_, e1, e2) => e1.collect(f) ++ e2.collect(f)
        case Total(e1) => e1.collect(f)
        case IfThenElse(Cond(_, e1, e2), e3, None) =>
          e1.collect(f) ++ e2.collect(f) ++ e3.collect(f)
        case IfThenElse(Cond(_, e1, e2), e3, Some(e4)) =>
          e1.collect(f) ++ e2.collect(f) ++ e3.collect(f) ++ e4.collect(f)
        case Named(_, e1) => e1.collect(f)
        case Sequence(ee) => ee.flatMap(_.collect(f))
        case _ => List()
      })

    def replace(f: PartialFunction[Expr, Expr]): Expr =
      f.applyOrElse(e, (ex: Expr) => ex match {
        case p: Project =>
          val rt = p.tuple.replace(f).asInstanceOf[TupleExpr]
          Project(rt, p.field)
        case ForeachUnion(x, e1, e2) =>
          val r1 = e1.replace(f).asInstanceOf[BagExpr]
          val xd = VarDef(x.name, r1.tp.tp)
          val r2 = e2.replace(f).asInstanceOf[BagExpr]
          ForeachUnion(xd, r1, r2)
        case Union(e1, e2) =>
          val r1 = e1.replace(f).asInstanceOf[BagExpr]
          val r2 = e2.replace(f).asInstanceOf[BagExpr]
          Union(r1, r2)
        case Singleton(e1) =>
          Singleton(e1.replace(f).asInstanceOf[TupleExpr])
        case Tuple(fs) =>
          val rfs = fs.map(x => x._1 -> x._2.replace(f).asInstanceOf[TupleAttributeExpr])
          Tuple(rfs)
        case Let(x, e1, e2) =>
          val r1 = e1.replace(f).asInstanceOf[BagExpr]
          val xd = VarDef(x.name, r1.tp)
          val r2 = e2.replace(f)
          Let(xd, r1, r2)
        case Total(e1) =>
          Total(e1.replace(f).asInstanceOf[BagExpr])
        case IfThenElse(Cond(c, e1, e2), e3, None) =>
          val r1 = e1.replace(f).asInstanceOf[TupleAttributeExpr]
          val r2 = e2.replace(f).asInstanceOf[TupleAttributeExpr]
          val r3 = e3.replace(f).asInstanceOf[BagExpr]
          IfThenElse(Cond(c, r1, r2), r3, None)
        case IfThenElse(Cond(c, e1, e2), e3, Some(e4)) =>
          val r1 = e1.replace(f).asInstanceOf[TupleAttributeExpr]
          val r2 = e2.replace(f).asInstanceOf[TupleAttributeExpr]
          val r3 = e3.replace(f).asInstanceOf[BagExpr]
          val r4 = e4.replace(f).asInstanceOf[BagExpr]
          IfThenElse(Cond(c, r1, r2), r3, Some(r4))
        case Named(n, e1) =>
          Named(n, e1.replace(f))
        case Sequence(ee) =>
          Sequence(ee.map(_.replace(f)))
        case _ => ex
      })


    def inputVars: Set[VarRef] = inputVars(Map[String, VarDef]()).toSet

    def inputVars(scope: Map[String, VarDef]): List[VarRef] = collect {
      case v: VarRef =>
        if (!scope.contains(v.name)) List(v)
        else {
          assert(v.tp == scope(v.name).tp); Nil
        }
      case ForeachUnion(x, e1, e2) =>
        e1.inputVars(scope) ++ e2.inputVars(scope + (x.name -> x))
      case Let(x, e1, e2) =>
        e1.inputVars(scope) ++ e2.inputVars(scope + (x.name -> x))
    }
  }

}