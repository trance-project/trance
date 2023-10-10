package org.trance.nrclibrary

import framework.common._
import framework.plans.NRCTranslator
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, types}
import org.trance.nrclibrary.utilities.DropContext

object NRCConverter extends NRCTranslator {
  def toNRC[T, S](rep: Rep[S], env: Map[Rep[_], Any]): Expr = rep match {
    //TODO - Map
    //case Map() =>
    //
    //ForEachUnion(tvr, expr.asBag, expr2.asTuple)
    //TODO - Filter
    //Case Filter()
    case FlatMap(e, Fun(in, out)) =>
      val expr1 = toNRC(e, env)
      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr1.asInstanceOf[BagExpr].tp.tp)
      val expr2 = toNRC(out, env + (in -> tvr))
      ForeachUnion(tvr, expr1.asInstanceOf[BagExpr], expr2.asInstanceOf[BagExpr])
    case Join(e1, e2, joinCond, joinType) =>
      val c1 = toNRC(e1, env).asInstanceOf[BagVarRef]
      val c2 = toNRC(e2, env).asInstanceOf[BagVarRef]
      val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)
      val tvr2 = TupleVarRef(utilities.Symbol.fresh(), c2.tp.tp)
      // If there are duplicate columns add suffix
      val map: Map[String, TupleVarRef] = Map(c1.name -> tvr) ++ Map(c2.name -> tvr2)

      val combinedColumnMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> PrimitiveProject(tvr, f)).toMap ++
        tvr2.tp.attrTps.keys.toSeq.map { f =>
          val newKey = if (tvr.tp.attrTps.contains(f)) f + "_2" else f
          newKey -> PrimitiveProject(tvr2, f)
        }.toMap
      val col = translateColumn(joinCond.asInstanceOf[Rep[T]], map).asInstanceOf[CondExpr]
      ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, IfThenElse(col, Singleton(Tuple(combinedColumnMap)))))
    case Merge(e1, e2) =>
      val c1 = toNRC(e1, env)
      val c2 = toNRC(e2, env)
      Union(c1.asInstanceOf[BagExpr], c2.asInstanceOf[BagExpr])
    case DropDuplicates(e1) =>
      val c1 = toNRC(e1, env).asInstanceOf[BagExpr]
      DeDup(c1)
    case Select(e, cols) =>
      val expr = toNRC(e, env).asInstanceOf[BagVarRef]
      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr.tp.tp)
      val map: Map[String, TupleVarRef] = Map(expr.name -> tvr)
      val pq = cols.asInstanceOf[Seq[Col[T]]]
      val k = pq.map {
        case compCol: CompCol[T] if compCol.lhs.isInstanceOf[BaseCol[T]] =>
          val baseCol = compCol.lhs.asInstanceOf[BaseCol[T]]
          baseCol.n -> translateColumn(compCol, map)
        case baseCol: BaseCol[T] => baseCol.n -> translateColumn(baseCol, map)
      }
      ForeachUnion(tvr, expr, Singleton(Tuple(k.toMap)))
      //TODO - Reimplementation needed
//    case Drop(e, cols) =>
//      DropContext.addField(cols: _*)
//      val expr = toNRC(e, env).asInstanceOf[BagExpr]
//      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr.tp.tp)
//      ForeachUnion(tvr, expr, Singleton(Tuple(unnestBagExpr(expr).diff(DropContext.getDropFields).map(f => f -> PrimitiveProject(tvr, f)).toMap)))
    case Reduce(e, cols, fields) =>
      val expr = toNRC(e, env).asInstanceOf[BagExpr]
      ReduceByKey(expr, cols, fields)
    case Sng(x) =>
      val e = env(x).asInstanceOf[TupleExpr]
      Singleton(e)
    case Wrapper(in, s) =>
      val ds: DataFrame = in.asInstanceOf[DataFrame]
      val nrcTypeMap: Map[String, TupleAttributeType] = ds.schema.fields.map {
        case StructField(name, ArrayType(dataType, _), _, _) => "array" -> BagType(TupleType(name -> typeToNRCType(dataType)))
        case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
      }.toMap
      BagVarRef(s, BagType(TupleType(nrcTypeMap)))
    case s@Sym(_) =>
      val e = env(s)
      e.asInstanceOf[Expr]
    case s@_ =>
      sys.error("Unsupported: " + s)
  }

  // TODO - Remove from WrappedDataframe


//  @tailrec
//  private def unnestBagExpr[T](b: BagExpr): Array[String] = b match {
//    case bd: DeDup => unnestBagExpr(bd.e)
//    case u: Union => unnestBagExpr(u.e1)
//    case f: ForeachUnion => unnestBagExpr(f.e1)
//    case bvr: BagVarRef => sys.error("Drop error")
//      getCtx()(bvr.name).dtypes.map { case (str, _) => str }
//    case s@_ => sys.error("Unnesting invalid: " + s)
//  }

  private def typeToNRCType(s: DataType): TupleAttributeType = s match {
    case structType: StructType => BagType(TupleType(structType.fields.map(f => f.name -> typeToNRCType(f.dataType)).toMap))
    case types.StringType => StringType
    case types.IntegerType => IntType
    case types.LongType => LongType
    case types.DoubleType => DoubleType
    case types.BooleanType => BoolType
    case _ => null
  }

  private def translateColumn[T](c: Rep[T], ctx: Map[String, TupleVarRef]): PrimitiveExpr = c match {
    case BaseCol(df, n) => Project(ctx(df), n).asPrimitive
    case Equality(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpEq, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case Inequality(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpNe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case GreaterThan(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpGt, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case GreaterThanOrEqual(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpGe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case LessThan(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpGt, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case LessThanOrEqual(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpGe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case Mult(e1: BaseCol[_], e2: BaseCol[_]) => ArithmeticExpr(OpMultiply, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Add(e1: BaseCol[_], e2: BaseCol[_]) => ArithmeticExpr(OpPlus, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Sub(e1: BaseCol[_], e2: BaseCol[_]) => ArithmeticExpr(OpMinus, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Divide(e1: BaseCol[_], e2: BaseCol[_]) => ArithmeticExpr(OpDivide, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Mod(e1: BaseCol[_], e2: BaseCol[_]) => ArithmeticExpr(OpMod, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case OrRep(e1: CompCol[_], e2: CompCol[_]) => Or(translateColumn(e1, ctx).asCond, translateColumn(e2, ctx).asCond)
    case AndRep(e1: CompCol[_], e2: CompCol[_]) => And(translateColumn(e1, ctx).asCond, translateColumn(e2, ctx).asCond)
    case compCol: CompCol[T] =>
      val left = compCol.lhs.asInstanceOf[CompCol[T]]
      val right = compCol.rhs
      val unnestedLeftCol1 = translateColumn(left.lhs, ctx)
      val unnestedLeftCol2 = translateColumn(left.rhs, ctx)

      val unnestedRightCol = translateColumn(right, ctx).asInstanceOf[PrimitiveExpr]

      //      And(PrimitiveCmp(getOp(left), translateColumn(left.lhs, ctx), translateColumn(left.rhs, ctx)), PrimitiveCmp(getOp(compCol), translateColumn(left.rhs, ctx), translateColumn(right, ctx)))

      val firstCmp = PrimitiveCmp(OpEq, unnestedLeftCol1, unnestedLeftCol2)
      And(firstCmp, PrimitiveCmp(OpEq, firstCmp, unnestedRightCol))
  }


}
