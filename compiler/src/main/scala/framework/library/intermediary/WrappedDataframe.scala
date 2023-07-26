package framework.library.intermediary

import framework.common.{BagCType, BagType, BoolType, DoubleType, IntType, LongType, StringType, TupleAttributeType, TupleType, VarDef}
import framework.library.WrappedDataset
import framework.library.utilities.SparkUtil.getSparkSession
import framework.plans.{AddIndex, BaseNormalizer, CExpr, Constant, Finalizer, InputRef, Join, NRCTranslator, Printer, Projection, Record, Select, Unnester, Variable, Merge => PlanMerge}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}
import org.apache.spark.sql.functions._



trait WrappedDataframe[T] extends Rep[DataFrame] with NRCTranslator {
  def flatMap[S](f: Rep[T] => WrappedDataframe[S]): WrappedDataframe[S] = {
    val sym = Sym[T](Symbol.fresh())
    val out = f(sym)
    val fun = Fun(sym, out)
    FlatMap(this, fun)
  }

  def union[S](df: WrappedDataframe[S]): WrappedDataframe[S] = {
    val sym = Sym[T](Symbol.fresh())
    val fun = Fun(sym, df)
    Merge(this, fun)
  }

  def leaveNRC(): DataFrame = {
    val nrcExpr = toNRC(this, Map())
    println("nrcExpression: " + nrcExpr)
    val cExpr = translate(nrcExpr)
    println("initial cExpr: " + cExpr)
    println("Initial cExpr Quote: " + Printer.quote(cExpr))
    val normalizer = new Finalizer(new BaseNormalizer())
    val normalized = normalizer.finalize(cExpr).asInstanceOf[CExpr]
    println("nested and normalized cExpr: " + normalized)
    println("corresponding quote: " + Printer.quote(normalized))

    val unnested = Unnester.unnest(normalized)(Map(), Map(), None, "_2")
    println("unnested and normalized cExpr: " + unnested)
    println("corresponding quote: " + Printer.quote(unnested))

    val stringified = Printer.quote(unnested)
    println("Printer quote: " + stringified)
    Context.ctx.foreach(x => println("Context name in context:" + x._1))
    println("Datasets in Context: " + Context.ctx.foreach(x => x._2.show()))


    planToDF(unnested)
  }


  def toNRC[T](rep: Rep[T], env: Map[Rep[_], Any]): Expr = rep match {
    case FlatMap(e, Fun(in, out)) =>
      val c1 = toNRC(e, env)
      val tvr = TupleVarRef(Symbol.fresh(), c1.asInstanceOf[BagExpr].tp.tp)
      val d1 = toNRC(out, env + (in -> tvr))
      val d2 = ForeachUnion(tvr, c1.asBag, d1.asInstanceOf[BagExpr])
      d2
    case Merge(e, Fun(in, out)) =>
      val c1 = toNRC(e, env)
      val tvr = TupleVarRef(Symbol.fresh(), c1.asInstanceOf[BagExpr].tp.tp)
      val d = toNRC(out, env + (in -> tvr))
      val d1 = Union(c1.asBag, d.asInstanceOf[BagExpr])
      d1
    case Sng(x) =>
      val e = env(x).asInstanceOf[TupleExpr]
     Singleton(e)
    case WrapDataset(in) =>
      val datasetIdentifier = Symbol.fresh()
      val ds: DataFrame = in.asInstanceOf[DataFrame]
      Context.addMapping(datasetIdentifier, ds)
      val nrcTypeMap: Map[String, TupleAttributeType] = ds.schema.fields.map {
        case StructField(name, ArrayType(dataType, _), _, _) => "array" -> BagType(TupleType(name -> typeToNRCType(dataType)))
        case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
      }.toMap

      BagVarRef(datasetIdentifier, BagType(TupleType(nrcTypeMap)))
    case s@Sym(_) =>
      val e = env(s)
      e.asInstanceOf[Expr]
    case _ =>
      sys.error("Unsupported")
  }

  def planToDF(cExpr: CExpr): DataFrame = cExpr match {
    case Projection(e1, v, p, fields) =>
      val i1 = planToDF(e1)
      val i2 = planToDF(p)
      val s = fields.toSeq
      //      i1.select(fields.map(col): _*)
      i1
    case Join(left, v, right, v2, cond, fields) =>
      val i1 = planToDF(left)
      val i2 = planToDF(right)
      i1.union(i2)
    case PlanMerge(e1, e2) =>
      planToDF(e1).union(planToDF(e2))
    case Select(x, v, p) =>
      if(p == Constant(true)) {
        return planToDF(x)
      }
      getSparkSession.emptyDataFrame
    case InputRef(data, tp) =>
      val df = Context.getMapping(data)
      df
    case AddIndex(e, name) =>
      val df = planToDF(e)
      df
    case Variable(name, tp) =>
      val df = Context.getMapping(name)
      df
    case Record(fs) =>
      getSparkSession.emptyDataFrame
    case _ =>
      sys.error("Unsupported")
  }


  def typeToNRCType(s: DataType): TupleAttributeType = {
    println("s :" + s)
    s match {
      case structType: StructType => BagType(TupleType(structType.fields.map(f => f.name -> typeToNRCType(f.dataType)).toMap))
      case types.StringType => StringType
      case types.IntegerType => IntType
      case types.LongType => LongType
      case types.DoubleType => DoubleType
      case types.BooleanType => BoolType
      case _ => null
    }
  }
}

