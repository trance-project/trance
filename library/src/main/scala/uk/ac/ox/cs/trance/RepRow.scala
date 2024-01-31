package uk.ac.ox.cs.trance

import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions


trait BaseRow extends Rep
trait RepRow extends BaseRow {

  def toSeq: Seq[RepProjection] = {
    val elems: Seq[RepProjection] = this.asInstanceOf[NewSym].schema.fields.map(f => RepProjection(f.name, this))
    elems
  }

  def get(i: Int): RepProjection = {

    RepProjection(this.asInstanceOf[NewSym].schema.fields(i).name, this)
  }

  def apply(s: String): RepProjection= {
    RepProjection(s, this)
  }

}

case class RepRowInst(vals: Seq[Rep]) extends RepRow

case class RepSeq(r: Rep*) extends Rep

object RepRow {
//  def apply(row: RepProjection*): RepRow = {
//    RepRowInst(row)
//  }

//  def apply(row: Rep): RepRow = {
//    RepProjection(row)
//  }

//  def apply(row: Rep*): RepRow = {
//    val pj = row.map(f => f.name -> f.r)
//    RepRowInst(pj)
//  }

  def apply(row: Any*): RepRow = {
    RepRowInst(row.map{
      case r: Rep => r
      case s: String => RowLiteral(s)
    })
  }

//  def apply(s: String): RepProjection = {
//    RepProjection(s, null)
//  }

//  def apply(): RepRow = {
//    RepRow()
//  }

  def fromSeq(rows: Seq[RepProjection]): RepRow = {
    RepRowInst(rows)
  }

  def empty: RepRow = {
    RepRow()
  }
}
object tools {
  def repIf(condition: Rep)(thenBranch: Rep)(elseBranch: Rep): RepRow = If(condition, thenBranch, elseBranch)

}

case class Concat(e1: RepElem, e2: Rep) extends RepRow

case class Transform(e1: RepElem, e2: RowLiteral) extends RepRow

case class RowLiteral(v: Any) extends Rep

case class Sym(rows: Seq[RepElem]) extends RepRow

case class NewSym(symID: String, schema: StructType) extends RepRow {
  override def apply(row: String): RepProjection = {
    RepProjection(row, this)
  }
}

trait BaseRepElem extends Rep {
  def name: String

}
case class RepElem(name: String, id: String) extends BaseRepElem {
  def +(e2: String): Rep = Concat(this, RowLiteral(e2))
}

case class RepProjection(name: String, r: Rep) extends BaseRepElem

/**
 * Currently used in a map function to replace a RepElem with a Literal
 */
case class Alias(in: RepElem, outputString: String) extends BaseRepElem {
  override def name: String = in.name
  def id: String = in.id
}

case class As(in: Rep, name: String) extends Operation

case class If(condition: Rep, thenBranch: Rep, elseBranch: Rep) extends RepRow

// TODO - Not sure if we want/need this
//package object repextensions {
//  implicit class RepElemSeqOps(seq: Seq[BaseRepElem]) {
//    def tMap(f: Rep => Any): Seq[Rep] = {
//      seq.map(z => f(z) match {
//        case s :String => Transform(z, RowLiteral(s))
//      })
//    }
//  }
//}

