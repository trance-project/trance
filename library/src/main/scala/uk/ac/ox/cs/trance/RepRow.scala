package uk.ac.ox.cs.trance

import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

import scala.language.implicitConversions


trait BaseRow extends Rep

case class RepRow(vals: List[(String, Rep)]) extends Rep {
  def apply(n: String): Rep = vals.find(x => x._1 == n).get._2
}

case class RepSeq(rr: RepRow*) extends Rep

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
      case sym: Sym =>

        val nestedStruct = sym.schema.asInstanceOf[StructType].fields.flatMap{ g =>
          g.dataType match {
            case s: StructType => if (!sym.isUnnest) s.fields.map(_  => StructType(Seq(g))) else s.fields.map(f => StructType(Seq(f)))
            case _ => Seq(StructType(Seq(g)))
          }
        }

        val projectedSym = nestedStruct.flatMap { f =>
          val newSym = Sym(sym.symID, f)
          Seq(f.fields.head.name -> RepProjection(f.fields.head.name, newSym))
        }.toList

        projectedSym

      case e@_ => sys.error("Invalid Row Argument: " + e.getClass)
    })
  }

  def empty: RepRow = RepRow(List.empty)

}

//object tools {
//  def repIf(): BaseRow = {
//    def apply(condition: Rep)(thenBranch: Rep)(elseBranch: Rep)= {
//      If(condition, thenBranch, elseBranch)
//    }
//  }
//  def repIf(condition: Rep)(thenBranch: Rep): BaseRow = If(condition, thenBranch, RepRow.empty)
//
//}

case class If(condition: Rep, thenBranch: Rep, elseBranch: Rep) extends Rep

object If {
//  def apply(cond: Rep)(thenBranch: Rep): BaseRow = {
//    If(cond, thenBranch)
//  }
//
  def apply(cond: Rep)(thenBranch: Rep)(elseBranch: Rep): Rep = {
    If(cond, thenBranch, elseBranch)
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
}

/**
 * Currently used in a map function to replace a RepElem with a Literal
 */
case class Alias(in: RepElem, outputString: String) extends BaseRepElem {
  override def name: String = in.name

  def id: String = in.id
}

case class As(in: Rep, name: String) extends WrappedCollection




