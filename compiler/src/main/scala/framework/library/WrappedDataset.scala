package framework.library

import framework.common.{BagType, BoolType, DoubleType, IntType, LongType, OpArithmetic, OpCmp, StringType, TupleAttributeType, TupleType, VarDef}
import framework.library.WrappedDataset.{addMapping, atomicInteger, ctx}
import framework.library.utilities.SparkUtil.getSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}
import framework.plans._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.{TraversableLike, mutable}

class WrappedDataset(val inputDf: Dataset[Row]) {

  import WrappedDataset.nrc._
  val spark: SparkSession = getSparkSession

  // Workaround for limitation of auxiliary constructors needing to call original constructor
  var expr: Expr = if (inputDf != null) createNRCExpression() else expr

  def this(b: Any) {
    this(null)
    expr = b.asInstanceOf[Expr]
  }

  private def createNRCExpression(): Expr = {
    val inputIdentifier = "input_" + atomicInteger.getAndIncrement()
    WrappedDataset.addMapping(inputIdentifier, inputDf)

    val nrcTypeMap: Map[String, TupleAttributeType] = inputDf.schema.fields.map {
      case StructField(name, ArrayType(dataType, _), _, _) => "array" -> BagType(TupleType(name -> typeToNRCType(dataType)))
      case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
    }.toMap

    BagVarRef(inputIdentifier, BagType(TupleType(nrcTypeMap)))
  }

  def leaveNRC(): Dataset[Row] = {
    val plan = toPlan(expr)

    println("plan: " + plan)
    planToDataframe(plan)
  }

  def toPlan(e: Expr): CExpr = {
    translate(e)
  }

  private def planToDataframe(cExpr: CExpr): Dataset[Row] = {
    cExpr match {
      case Merge(e1, e2) => planToDataframe(e1).union(planToDataframe(e2))
      case Sng(e1) => planToDataframe(e1)
      case Comprehension(e1, v, p, e) =>
        println("COMPREHENSION:")
        println("________________")
        println("e1: " + e1)
        println("v: " + v)
        println("p: " + p)
        println("e: " + e)
        println("ctx: " + WrappedDataset.ctx)
        println("________________")

        val d1 = planToDataframe(e1)
        val d2 = planToDataframe(e)

        if (d2 != null) createFlatMapDataframe(d1, d2) else d1
      case Variable(name, _) => WrappedDataset.getMapping(name)
    }
  }

  private def createFlatMapDataframe(d1: DataFrame, d2: DataFrame): DataFrame = {
    val d3 = d1.collect().flatMap(_ => d2.collect())
    val result = spark.sparkContext.parallelize(d3)
    spark.createDataFrame(result, d2.schema)
  }

  private def typeToNRCType(s: DataType): TupleAttributeType = {
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

  //Syntactic sugar method to allow user's
  // syntax to match spark flatMap while performing operations on a WrappedDataset
  def flatMap(f: TupleExpr => WrappedDataset)(implicit d : DummyImplicit): WrappedDataset = {
    val x = VarDef("x", this.expr.asBag.tp.tp)
    new WrappedDataset(ForeachUnion(x, this.expr.asBag, f(null).expr.asInstanceOf[BagExpr]).asBag)

  }

  def flatMap(f: TupleExpr => BagExpr): WrappedDataset = {
    val tupleIdentifier = "x_" + atomicInteger.getAndIncrement()
    val tvr = TupleVarRef(tupleIdentifier, this.expr.asBag.tp.tp)
    WrappedDataset.addMapping(tvr.name, this.inputDf)

    new WrappedDataset(ForeachUnion(tvr, this.expr.asBag, f(tvr)).asBag)
  }


  // TODO
  def map(): BagExpr = {
    null
  }

}

object WrappedDataset {

  val nrc: NRCTranslator = new NRCTranslator {}

  private val atomicInteger: AtomicInteger = new AtomicInteger(1)
  val ctx: mutable.Map[String, Dataset[Row]] = collection.mutable.Map[String, Dataset[Row]]()

  private def addMapping(s: (String, Dataset[Row])): Unit = {
    ctx += s
    println(ctx)
  }

  private def getMapping(s: String): Dataset[Row] = {
    ctx.getOrElse(s, null)
  }

  case object NumericProject {
    def apply(t: nrc.TupleVarRef, f: String): nrc.NumericExpr = {
      nrc.NumericProject(t, f)
    }
  }

  case object PrimitiveProject {
    def apply(t: nrc.TupleVarRef, f: String): nrc.PrimitiveExpr = {
      nrc.PrimitiveProject(t, f)
    }
  }

  case object BagProject {
    def apply(t: nrc.TupleVarRef, f: String): nrc.BagExpr = {
      nrc.BagProject(t, f)
    }
  }

  case object ForeachUnion {
    def apply(x: VarDef, e1: nrc.BagExpr, e2: nrc.BagExpr): nrc.BagExpr = {
      nrc.ForeachUnion(x, e1, e2)
    }
  }

  case object Union {
    def apply(x: nrc.BagExpr, y: nrc.BagExpr): nrc.BagExpr = {
      nrc.Union(x, y)
    }
  }

  case object Singleton {
    def apply(x: nrc.TupleExpr): nrc.BagExpr = {
      nrc.Singleton(x)
    }
  }

  case object Get {
    def apply(e: nrc.BagExpr): nrc.TupleExpr = {
      nrc.Get(e)
    }
  }

  case object DeDup {
    def apply(e: nrc.BagExpr): nrc.BagExpr = {
      nrc.DeDup(e)
    }
  }

  case object Tuple {
    def apply(fields: Map[String, nrc.TupleAttributeExpr]): nrc.TupleExpr = {
      nrc.Tuple(fields)
    }
  }

  case object NumericLet {

    def apply(x: VarDef, e1: nrc.Expr, e2: nrc.NumericExpr): nrc.NumericExpr = {
      nrc.NumericLet(x, e1, e2)
    }
  }

  case object PrimitiveLet {

    def apply(x: VarDef, e1: nrc.Expr, e2: nrc.PrimitiveExpr): nrc.PrimitiveExpr = {
      nrc.PrimitiveLet(x, e1, e2)
    }
  }

  case object BagLet {

    def apply(x: VarDef, e1: nrc.Expr, e2: nrc.BagExpr): nrc.BagExpr = {
      nrc.BagLet(x, e1, e2)
    }
  }

  case object TupleLet {

    def apply(x: VarDef, e1: nrc.Expr, e2: nrc.TupleExpr): nrc.TupleExpr = {
      nrc.TupleLet(x, e1, e2)
    }
  }

  case object PrimitiveCmp {

    def apply(op: OpCmp, e1: nrc.PrimitiveExpr, e2: nrc.PrimitiveExpr): nrc.CondExpr = {
      nrc.PrimitiveCmp(op, e1, e2)
    }
  }

  case object And {
    def apply(e1: nrc.CondExpr, e2: nrc.CondExpr): nrc.CondExpr = {
      nrc.And(e1, e2)
    }
  }

  case object Or {
    def apply(e1: nrc.CondExpr, e2: nrc.CondExpr): nrc.CondExpr = {
      nrc.Or(e1, e2)
    }
  }

  case object Not {
    def apply(e: nrc.CondExpr): nrc.CondExpr = {
      nrc.Not(e)
    }
  }

  case object NumericIfThenElse {
    def apply(cond: nrc.CondExpr, e1: nrc.NumericExpr, e2: nrc.NumericExpr): nrc.NumericExpr = {
      nrc.NumericIfThenElse(cond, e1, e2)
    }
  }

  case object PrimitiveIfThenElse {
    def apply(cond: nrc.CondExpr, e1: nrc.PrimitiveExpr, e2: nrc.PrimitiveExpr): nrc.PrimitiveExpr = {
      nrc.PrimitiveIfThenElse(cond, e1, e2)
    }
  }

  case object BagIfThenElse {
    def apply(cond: nrc.CondExpr, e1: nrc.BagExpr, e2: Option[nrc.BagExpr]): nrc.BagExpr = {
      nrc.BagIfThenElse(cond, e1, e2)
    }
  }

  case object TupleIfThenElse {
    def apply(cond: nrc.CondExpr, e1: nrc.TupleExpr, e2: nrc.TupleExpr): nrc.TupleExpr = {
      nrc.TupleIfThenElse(cond, e1, e2)
    }
  }

  case object ArithmeticExpr {
    def apply(op: OpArithmetic, e1: nrc.NumericExpr, e2: nrc.NumericExpr): nrc.NumericExpr = {
        nrc.ArithmeticExpr(op, e1, e2)
    }
  }

  case object Count {
    def apply(e: nrc.BagExpr): nrc.NumericExpr = {
      nrc.Count(e)
    }
  }

  case object Sum {
    def apply(e: nrc.BagExpr, fields: List[String]): nrc.TupleExpr = {
      nrc.Sum(e, fields)
    }
  }

  case object GroupByKey {
    def apply(e: nrc.BagExpr, keys: List[String], values: List[String], groupAttrName: String = nrc.GROUP_ATTR_NAME): nrc.GroupByExpr = {
      nrc.GroupByKey(e, keys, values, groupAttrName)
    }
  }

  case object ReduceByKey {
    def apply(e: nrc.BagExpr, keys: List[String], values: List[String]): nrc.GroupByExpr = {
      nrc.ReduceByKey(e, keys, values)
    }
  }

  case object Assignment {
    //TODO: Check return type
    def apply(name: String, rhs: nrc.Expr): nrc.Assignment = {
      nrc.Assignment(name, rhs)
    }
  }

  case object Program {
    //TODO: Check return type

    def apply(statements: List[nrc.Assignment]): nrc.Program = {
      nrc.Program(statements)
    }
  }
}


