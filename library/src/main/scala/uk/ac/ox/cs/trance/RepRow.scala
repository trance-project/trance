package uk.ac.ox.cs.trance

import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.language.implicitConversions


trait BaseRow extends Rep

case class RepRow(vals: List[(String, Rep)]) extends Rep {
  def apply(n: String): Rep = vals.find(x => x._1 == n).get._2
}

case class RepSeq(rr: RepRow*) extends WrappedCollection

object RepRow {
  def apply(ee: (String, Rep)*)(implicit dummyImplicit: DummyImplicit): RepRow = {
    RepRow(ee.toList)
  }

  def apply(r: Any*): RepRow = {
    RepRow(r.toList.flatMap {
      case s: RepProjection => Seq(s.name -> s)
      case (str: String, rep: Rep) => Seq(str -> rep)
      case sym: Sym =>
        val structFields = sym.schema.asInstanceOf[StructType].fields.head.dataType match {
//          case s: StructType => s.fields.map(f => StructType(Seq(StructField(f.name, f.dataType))))
          case _ => sym.schema.asInstanceOf[StructType].fields.map(f => StructType(Seq(StructField(f.name, f.dataType))))
        }
        structFields.flatMap { f =>
          val newSym = Sym(sym.symID, f)
          Seq(f.fields.head.name -> RepProjection(f.fields.head.name, newSym))
        }.toList
//      case "*" => Seq(this.asInstanceOf[RepRow].vals.map(f => f._1 -> f._2))
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

case class If(condition: Rep, thenBranch: Rep, elseBranch: Rep) extends BaseRow

object If {
//  def apply(cond: Rep)(thenBranch: Rep): BaseRow = {
//    If(cond, thenBranch)
//  }
//
  def apply(cond: Rep)(thenBranch: Rep)(elseBranch: Rep): BaseRow = {
    If(cond, thenBranch, elseBranch)
  }
}

case class Concat(e1: RepElem, e2: Rep) extends Rep

case class Transform(e1: RepElem, e2: RowLiteral) extends Rep

case class RowLiteral(v: Any) extends Rep


case class Sym(symID: String, schema: DataType) extends Rep {

  def apply(row: String): RepProjection = {
    val field = findStructField(this.schema, row).getOrElse(sys.error("No Struct Field: " + row + " available in " + this.schema))
    RepProjection(row, Sym(symID, StructType(Seq(field))))
  }

  //TODO Possibly allow choice of what bag to unnest
  def unnest(): Sym = {
    try {
      Sym(this.symID, schema.asInstanceOf[StructType].fields.head.dataType)
    }
    catch {
      case _ => sys.error("Cannot unnest further")
    }
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
//  def toCollection(implicit g: WrappedCollection): WrappedCollection = {
//    super.asInstanceOf[WrappedCollection]
//  }


}

/**
 * Currently used in a map function to replace a RepElem with a Literal
 */
case class Alias(in: RepElem, outputString: String) extends BaseRepElem {
  override def name: String = in.name

  def id: String = in.id
}

case class As(in: Rep, name: String) extends WrappedCollection



