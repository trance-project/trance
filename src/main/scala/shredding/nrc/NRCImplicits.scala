package shredding.nrc

import shredding.core.VarDef

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
        case l: Let => l.e1.collect(f) ++ l.e2.collect(f)
        case Total(e1) => e1.collect(f)
        case i: IfThenElse =>
          i.cond.e1.collect(f) ++ i.cond.e2.collect(f) ++
            i.e1.collect(f) ++ i.e2.map(_.collect(f)).getOrElse(Nil)
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
        case l: Let =>
          val r1 = l.e1.replace(f)
          val xd = VarDef(l.x.name, r1.tp)
          val r2 = l.e2.replace(f)
          Let(xd, r1, r2)
        case Total(e1) =>
          Total(e1.replace(f).asInstanceOf[BagExpr])
        case i: IfThenElse =>
          val c1 = i.cond.e1.replace(f).asInstanceOf[TupleAttributeExpr]
          val c2 = i.cond.e2.replace(f).asInstanceOf[TupleAttributeExpr]
          val r1 = i.e1.replace(f)
          if (i.e2.isDefined)
            IfThenElse(Cond(i.cond.op, c1, c2), r1, i.e2.get.replace(f))
          else
            IfThenElse(Cond(i.cond.op, c1, c2), r1)
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
      case l: Let =>
        l.e1.inputVars(scope) ++ l.e2.inputVars(scope + (l.x.name -> l.x))
    }
  }

}

trait ShreddedNRCImplicits extends NRCImplicits {
  this: ShreddedNRC with Dictionary =>

  def inputVars(e: Expr): Set[VarRef] =
    inputVars(e, Map[String, VarDef]()).toSet

  def inputVars(e: Expr, scope: Map[String, VarDef]): List[VarRef] = e.collect {
    case v: VarRef =>
      if (!scope.contains(v.name)) List(v)
      else {
        assert(v.tp == scope(v.name).tp); Nil
      }
    case ForeachUnion(x, e1, e2) =>
      inputVars(e1, scope) ++ inputVars(e2, scope + (x.name -> x))
    case l: Let =>
      inputVars(l.e1, scope) ++ inputVars(l.e2, scope + (l.x.name -> l.x))
    case Lookup(l1, _) => inputVars(l1, scope)
    case Label(vs) => vs.flatMap(inputVars(_, scope)).toList
  }

  implicit class DictionaryOps(d: Dict) {

    def inputVars(scope: Map[String, VarDef]): List[VarRef] = d match {
      case EmptyDict => Nil
      case _: InputBagDict => Nil
      case o: OutputBagDict => o.flatBag.inputVars(scope)
      case TupleDict(fs) => fs.values.flatMap(_.inputVars(scope)).toList
    }
  }

}

