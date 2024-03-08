package uk.ac.ox.cs.trance

import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructField, StructType}

import scala.language.implicitConversions


case class RepRow(vals: List[(String, Rep)]) extends Rep {
  def apply(n: String): Rep = vals.find(x => x._1 == n).get._2
}

trait TraversableRep extends Rep
case class RepSeq(rr: RepRow*) extends TraversableRep

object RepSeq {
  def empty: RepSeq = RepSeq()
}

object RepRow {
  def apply(ee: (String, Rep)*)(implicit dummyImplicit: DummyImplicit): RepRow = {
    RepRow(ee.toList)
  }

  def apply(r: Any*): RepRow = {
    RepRow(r.toList.flatMap {
      case s: RepProjection => Seq(s.name -> s)
      case (str: String, rep: Rep) => Seq(str -> rep)
      case Sym(symID, schema: StructType, isUnnest) =>

        val nestedStruct = schema.fields.flatMap { g =>
          unnestTypes(g.dataType, isUnnest, g)
        }

        val projectedSym = nestedStruct.flatMap { f =>
          val newSym = Sym(symID, f)
          Seq(f.fields.head.name -> RepProjection(f.fields.head.name, newSym))
        }.toList

        projectedSym
      case Sym(symID, schema: ArrayType, isUnnest) => // Nested array case from TestFN

        val nestedStruct = schema.elementType.asInstanceOf[StructType].fields.flatMap { g =>
          unnestTypes(g.dataType, isUnnest, g)
        }

        val projectedSym = nestedStruct.flatMap { f =>
          val newSym = Sym(symID, f)
          Seq(f.fields.head.name -> RepProjection(f.fields.head.name, newSym))
        }.toList

        projectedSym
      case e@_ => sys.error("Invalid Row Argument: " + e.getClass)
    })
  }

  def empty: RepRow = RepRow(List.empty)

  private def unnestTypes(d: DataType, isUnnest: Boolean, g: StructField): Iterable[StructType] = d match {
    case a: ArrayType => unnestTypes(a.elementType, isUnnest, g)
    case s: StructType => if (!isUnnest) {
      val output1 = s.fields.map(_ => StructType(Seq(g)))
      output1
    } else {
      val output2 = s.fields.map(f => StructType(Seq(f)))
      output2
    }
    case _ => Seq(StructType(Seq(g)))
  }
}

case class RowIfThenElse(condition: Rep, thenBranch: TraversableRep, elseBranch: TraversableRep) extends Rep

object IfThenElse {
  def apply(cond: Rep)(thenBranch: TraversableRep)(elseBranch: TraversableRep): Rep = {
    RowIfThenElse(cond, thenBranch, elseBranch)
  }
}

case class RowIfThen(condition: Rep, thenBranch: TraversableRep) extends TraversableRep

object IfThen {
  def apply(cond: Rep)(thenBranch: TraversableRep): RowIfThen = {
    RowIfThen(cond, thenBranch)
  }
}

case class Concat(e1: RepElem, e2: Rep) extends Rep

case class Transform(e1: RepElem, e2: RowLiteral) extends Rep

case class RowLiteral(v: Any) extends Rep


case class Sym(symID: String, schema: DataType, isUnnest: Boolean = false) extends Rep {

  def apply(row: String): RepProjection = {
    val field = findStructField(this.schema, row).getOrElse(sys.error("No Struct Field: " + row + " available in " + this.schema))
    val r = RepProjection(row, Sym(symID, StructType(Seq(field))))
    r
  }

  private def findStructField(dataType: DataType, targetName: String): Option[StructField] = dataType match {
    case structType: StructType =>
      structType.fields.foldLeft(Option.empty[StructField]) { (acc, field) =>
        if (field.name == targetName) Some(field)
        else findStructField(field.dataType, targetName) orElse acc
      }
    case arrayType: ArrayType =>
      findStructField(arrayType.elementType, targetName)
    case _ => None
  }

}


trait BaseRepElem extends Rep {
  def name: String

}

case class RepElem(name: String, id: String) extends BaseRepElem {
  def +(e2: String): Rep = Concat(this, RowLiteral(e2))
}

case class RepProjection(name: String, r: Rep) extends BaseRepElem with WrappedCollection {
  override def map(f: Sym => Rep): WrappedCollection = {


    //TODO error handling for non collection projections

    val symID = utilities.Symbol.fresh()
    val sym = Sym(symID, schema, true)

    val out = f(sym)
    val fun = Fun(sym, out)
    Map(this, fun)
  }

  override def flatMap(f: Sym => TraversableRep): WrappedCollection = {
    val symID = utilities.Symbol.fresh()
    val sym = Sym(symID, schema, true)
    val out = f(sym)
    val fun = Fun(sym, out)
    FlatMap(this, fun)
  }
}

/**
 * Currently used in a map function to replace a RepElem with a Literal
 */
case class Alias(in: RepElem, outputString: String) extends BaseRepElem {
  override def name: String = in.name

  def id: String = in.id
}

case class As(in: Rep, name: String) extends WrappedCollection




