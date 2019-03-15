package shredding.nrc2

import shredding.core._

object NRCImplicits extends NRC with NRCTransforms with ShreddingTransform {

  /**
    * Extension methods for NRC expressions
    */
  implicit class TraversalOps(e: Expr) {

    def quote: String = Printer.quote(e)

    def eval: Any = new Evaluator().eval(e)

    def shred: ShredExpr = Shredder(e)

    //    def collect[A](f: PartialFunction[Expr, List[A]]): List[A] =
    //      f.applyOrElse(e, (ex: Expr) => ex match {
    //        case ForeachUnion(_, e1, e2) => e1.collect(f) ++ e2.collect(f)
    //        case Union(e1, e2) => e1.collect(f) ++ e2.collect(f)
    //        case Singleton(e1) => e1.collect(f)
    //        case Tuple(fs) => fs.flatMap(_._2.collect(f)).toList
    //        case Let(_, e1, e2) => e1.collect(f) ++ e2.collect(f)
    //        case Mult(e1, e2) => e1.collect(f) ++ e2.collect(f)
    //        case IfThenElse(_, e1, None) => e1.collect(f)
    //        case IfThenElse(_, e1, Some(e2)) => e1.collect(f) ++ e2.collect(f)
    //        case _ => List()
    //      })
    //
    //      def inputVars: List[BaseVarRef] = inputVars(Map.empty)
    //
    //      private[TraversalOps] def inputVars(scope: Map[String, VarDef]): List[BaseVarRef] = collect {
    //        case v: BaseVarRef =>
    //          if (!scope.contains(v.varDef.name)) List(v)
    //          else { assert(v.tp == scope(v.varDef.name).tp); Nil }
    //        case ForeachUnion(x, e1, e2) =>
    //          e1.inputVars(scope) ++ e2.inputVars(scope + (x.name -> x))
    //        case Let(x, e1, e2) =>
    //          e1.inputVars(scope) ++ e2.inputVars(scope + (x.name -> x))
    //        case Label(vs) =>
    //          vs.filterNot(v => scope.contains(v.varDef.name))
    //        case _ => sys.error("Unhandled case in inputVars: " + e)
    //      }
  }

//  implicit class TypeOps(tp: Type) {
//
//    def flatTp: Type = tp match {
//      case t: PrimitiveType => t
//      case _: BagType => LabelType
//      case TupleType(as) =>
//        TupleType(as.map(a => a._1 -> a._2.flatTp.asInstanceOf[TupleAttributeType]))
//      case _ => sys.error("unknown flat type for " + tp)
//    }
//  }
}
