package uk.ac.ox.cs.trance

import scala.language.implicitConversions


trait BaseRow extends Rep

trait RepRow extends BaseRow {

  def toSeq: Seq[BaseRepElem] = {
    val elems: Seq[BaseRepElem] = this.asInstanceOf[Sym].rows
    elems
  }

  def get(i: Int): BaseRepElem = {
    this.asInstanceOf[Sym].rows(i)
  }

}

case class RepRowInst(vals: Seq[Rep]) extends RepRow

//object RepRowInst {
//  def apply(vals: Seq[Rep]): RepRowInst = new RepRowInst(vals)
//
//}

case class RepSeq(r: Rep*) extends Rep

object RepRow {

  def apply(row: Any*): RepRow = {
    RepRowInst(row.map{
      case r: Rep => r
      case s: String => RowLiteral(s)
    })
  }

  def fromSeq(rows: Seq[Rep]): RepRow = {
    RepRowInst(rows)
  }

  def repIf(condition: Rep)(thenBranch: Rep)(elseBranch: Rep): RepRow = If(condition, thenBranch, elseBranch)

}

case class Concat(e1: BaseRepElem, e2: Rep) extends RepRow

case class Transform(e1: BaseRepElem, e2: RowLiteral) extends RepRow

case class RowLiteral(v: Any) extends Rep

case class Sym(id: String, rows: Seq[BaseRepElem]) extends RepRow

trait BaseRepElem extends Rep {
  def name: String

  def +(e2: String): Rep = Concat(this, RowLiteral(e2))

}
case class RepElem(name: String) extends BaseRepElem

/**
 * Currently used in a map function to replace a RepElem with a Literal
 */
case class Alias(in: BaseRepElem, outputString: String) extends BaseRepElem {
  override def name = in.name
}

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

