package framework.library

import framework.common._
import framework.library.utilities.Context
import framework.library.utilities.SparkUtil.getSparkSession
import framework.plans.{AddIndex, BaseNormalizer, CDeDup, CExpr, Constant, Finalizer, InputRef, Join, NRCTranslator, Printer, Projection, Unnester, Variable, Merge => CMerge, Project => CProject, Select => CSelect, Sng => CSng}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, types}


trait WrappedDataframe[T] extends Rep[DataFrame] with NRCTranslator {

  def flatMap[S](f: Rep[T] => WrappedDataframe[S]): WrappedDataframe[S] = {
    val sym = Sym[T](utilities.Symbol.fresh())
    val out = f(sym)
    val fun = Fun(sym, out)
    FlatMap(this, fun)
  }

  def union[S](df: WrappedDataframe[S]): WrappedDataframe[S] = {
    val sym = Sym[T](utilities.Symbol.fresh())
    val fun = Fun(sym, df)
    Merge(this, fun)
  }

  def dropDuplicates[S]: WrappedDataframe[T] = {
    DropDuplicates(this)
  }

  def select(col: String): WrappedDataframe[T] = {
    Select(this, col)
  }
  def select(col: Column): WrappedDataframe[T] = {
    val p = col.toString()
    Select(this, p)
  }

  def drop(col: String): WrappedDataframe[T] = {
    Drop(this, col)
  }

  def leaveNRC(): DataFrame = {

    println("This: " + this)
    val nrcExpr = toNRC(this, Map())
    println("nrcExpression: " + nrcExpr)
    println("NRC Expr: " + quote(nrcExpr))
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
      val tvr = TupleVarRef(c1.asInstanceOf[BagVarRef].name, c1.asInstanceOf[BagExpr].tp.tp)
      val c2 = toNRC(out, env + (in -> tvr))
      val d = ForeachUnion(tvr, c1.asInstanceOf[BagExpr], c2.asInstanceOf[BagExpr])
      d
    case Merge(e1, Fun(in, out)) =>
      val c1 = toNRC(e1, env)
      val c2 = toNRC(out, env + (in -> c1))
      val d = Union(c1.asInstanceOf[BagExpr], c2.asInstanceOf[BagExpr])


      d
    case DropDuplicates(e1) =>
      val c1 = toNRC(e1, env).asInstanceOf[BagExpr]
      DeDup(c1)
    case Select(e, cols) =>
      val c1 = toNRC(e, env).asInstanceOf[BagVarRef]
      val tvr = TupleVarRef(c1.name, c1.tp.tp)
      Project(tvr, cols)
//    TODO - multiple column projection?
//    case Drop(e, col) =>
//      val c1 = toNRC(e, env).asInstanceOf[BagVarRef]
//      val tvr = TupleVarRef(c1.name, c1.tp.tp)
//      val x = e.asInstanceOf[WrapDataset[T]]
//      val y = x.in.asInstanceOf[DataFrame]
//      val types = y.dtypes.map{ case (str, _) => str }
//      val updatedTypes = types.filter(_ != col)
//      Project(tvr, updatedTypes)
    case Sng(x) =>
      val e = env(x).asInstanceOf[TupleExpr]
     Singleton(e)
    case Dataset(in) =>
      val datasetIdentifier = utilities.Symbol.fresh()
      val ds: DataFrame = in.asInstanceOf[DataFrame]
      Context.addMapping(datasetIdentifier, ds)
      val nrcTypeMap: Map[String, TupleAttributeType] = ds.schema.fields.map {
        case StructField(name, ArrayType(dataType, _), _, _) => "array" -> BagType(TupleType(name -> typeToNRCType(dataType)))
        case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
      }.toMap

      val bvr = BagVarRef(datasetIdentifier, BagType(TupleType(nrcTypeMap)))

//      val tvr = TupleVarRef()
//      Union(Union(bvr, bvr), bvr)
//      Union(DeDup(bvr), DeDup(bvr))
//
//

      bvr
    case s@Sym(_) =>
      val e = env(s)
      e.asInstanceOf[Expr]
    case s@_ =>
      sys.error("Unsupported: " + s)
  }

  def planToDF(cExpr: CExpr): DataFrame = cExpr match {
    case Projection(e1, v, p, fields) =>
      val i1 = planToDF(e1)
      val i2 = planToDF(p)
      i1
    case Join(left, v, right, v2, cond, fields) =>
      val i1 = planToDF(left)
      val i2 = planToDF(right)
      i1.join(i2, fields)
    case CMerge(e1, e2) =>
      planToDF(e1).union(planToDF(e2))
    case CSelect(x, v, p) =>
      if(p == Constant(true)) {
        return planToDF(x)
      }
      getSparkSession.emptyDataFrame
    case CDeDup(in) =>
      val i1 = planToDF(in)
      i1.dropDuplicates()
    case InputRef(data, tp) =>
      val df = Context.getMapping(data)
      df
    case AddIndex(e, name) =>
      val df = planToDF(e)
      df
    case Variable(name, tp) =>
      val df = Context.getMapping(name)
      df
//    TODO - Handle Record
//    case Record(fs) =>
//      (fs.map(f => f._1 -> (f._2))).foreach(x => Context.getMapping(x._1) -> planToDF(x._2))
    case CProject(e1, field) =>
      val df = planToDF(e1)
      df.select(field)
    case CSng(e1) =>
      planToDF(e1)
    case s@_ =>
      sys.error("Unsupported: " + s)
  }


  def typeToNRCType(s: DataType): TupleAttributeType = {
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
//  override def quote(e: Expr): String = e match {
//    case c: Const if c.tp == StringType => "\"" + c.v + "\""
//    case c: Const => c.v.toString
//    case Udf(n, e1, _) => s"$n(${quote(e1)})"
//    case v: VarRef => v.name
//    case p: Project => quote(p.tuple) + "." + p.field
//    case ForeachUnion(x, e1, e2) =>
//      s"""|For ${x.name} in ${quote(e1)} Union
//          |${ind(quote(e2))}""".stripMargin
//    case Union(e1, e2) =>
//      s"""|(${quote(e1)})
//          |Union
//          |(${quote(e2)})""".stripMargin
//    case Singleton(e1) => s"{${quote(e1)}}"
//    case DeDup(e1) => s"DeDup(${quote(e1)})"
//    case Get(e1) => s"Get(${quote(e1)})"
//    case Tuple(fs) =>
//      s"( ${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(",\n   ")} )"
//    case l: Let =>
//      s"""|Let ${l.x.name} =
//          |${ind(quote(l.e1))}
//          |In ${quote(l.e2)}""".stripMargin
//    case c: Cmp => s"${quote(c.e1)} ${c.op} ${quote(c.e2)}"
//    case And(e1, e2) => s"${quote(e1)} AND ${quote(e2)}"
//    case Or(e1, e2) => s"${quote(e1)} OR ${quote(e2)}"
//    case Not(e1) => s"NOT ${quote(e1)}"
//    case i: IfThenElse =>
//      if (i.e2.isDefined)
//        s"""|If (${quote(i.cond)}) Then
//            |${ind(quote(i.e1))}
//            |Else
//            |${ind(quote(i.e2.get))}""".stripMargin
//      else
//        s"""|If (${quote(i.cond)}) Then
//            |${ind(quote(i.e1))}""".stripMargin
//    case ArithmeticExpr(op, e1, e2) => s"(${quote(e1)} $op ${quote(e2)})"
//    case Count(e1) => s"Count(${quote(e1)})"
//    case Sum(e1, fs) =>
//      s"Sum(${quote(e1)}, (${fs.mkString(", ")}))"
//    case GroupByKey(e, ks, vs, _) =>
//      s"""|GroupByKey([${ks.mkString(", ")}], [${vs.mkString(", ")}],
//          |${ind(quote(e))}
//          |)""".stripMargin
//    case ReduceByKey(e, ks, vs) =>
//      s"""|ReduceByKey([${ks.mkString(", ")}], [${vs.mkString(", ")}],
//          |${ind(quote(e))}
//          |)""".stripMargin
//
//    // Label extensions
//    case x: ExtractLabel =>
//      //      val tuple = x.lbl.tp.attrTps.keys.mkString(", ")
//      val tuple = x.lbl.tp.attrTps.map(x => x._1 + " : " + quote(x._2)).mkString(", ")
//      s"""|Extract ${quote(x.lbl)} as ($tuple) In
//          |${quote(x.e)}""".stripMargin
//    case l: NewLabel =>
//      val ps = l.params.map { case (n, p) => n + " := " + quote(p.e) }.toList
//      //      val ps = l.params.map { case (n, p) => n + " := " + quote(p.e) + " : " + quote(p.tp)}.toList
//      s"NewLabel(${(l.id :: ps).mkString(", ")})"
//
//    // Dictionary extensions
//    case EmptyDict => "Nil"
//    case BagDict(tp, flat, dict) =>
//      val params = tp.attrTps.keys.mkString(", ")
//      //      val params = tp.attrTps.map(x => x._1 + ": " + quote(x._2)).mkString(", ")
//      s"""|(($params) ->
//          |  flat :=
//          |${ind(quote(flat), 2)},
//          |  tupleDict :=
//          |${ind(quote(dict), 2)}
//          |)""".stripMargin
//    case TupleDict(fs) =>
//      s"( ${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(",\n   ")} )"
//    case TupleDictProject(v) => quote(v) + ".tupleDict"
//    case d: DictUnion =>
//      s"""|(${quote(d.dict1)})
//          |DictUnion
//          |(${quote(d.dict2)})""".stripMargin
//
//    // Shredding extensions
//    case ShredUnion(e1, e2) =>
//      s"""|(${quote(e1)})
//          |ShredUnion
//          |(${quote(e2)})""".stripMargin
//    case Lookup(lbl, dict) =>
//      s"Lookup(${quote(lbl)}, ${quote(dict)})"
//
//    // Materialization extensions
//    case KeyValueMapLookup(lbl, dict) =>
//      s"KeyValueMapLookup(${quote(lbl)}, ${quote(dict)})"
//    case BagToKeyValueMap(b) =>
//      s"BagToKeyValueMap(${quote(b)})"
//    case KeyValueMapToBag(d) =>
//      s"KeyValueMapToBag(${quote(d)})"
//
//    case _ =>
//      sys.error("Cannot print unknown expression " + e)
//  }
}

