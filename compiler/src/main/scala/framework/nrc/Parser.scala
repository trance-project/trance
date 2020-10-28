package framework.nrc

import framework.common._
import scala.util.parsing.combinator.JavaTokenParsers
import java.io.FileReader
import java.io.FileInputStream
import scala.collection.mutable.HashMap

class Parser(tbls: Map[String, BagType]) extends JavaTokenParsers with MaterializeNRC with Factory {

  var scope: Map[String, VarDef] = Map()

  def defToRef(v: String): TupleVarRef = {
    val vr = scope(v)
    TupleVarRef(vr.name, vr.tp.asInstanceOf[TupleType])
  }

  def parse(input: String, p: Parser[Expr]): ParseResult[Expr] = parseAll(p, input)

  def project: Parser[TupleAttributeExpr] = tuplevarref~"."~ident ^^
    { case (v:String)~"."~(l:String) => Project(defToRef(v), l) }

  def tuppair: Parser[(String, TupleAttributeExpr)] = ident~":="~tupleattr ^^
    { case (v:String)~":="~t => (v, t)}

  def tuplevarref: Parser[String] = ident ^^
    { case (s: String) => s}

  def bagvarref: Parser[BagVarRef] = ident ^^
    { case (s: String) => BagVarRef(s, tbls(s)) }

  def tuple: Parser[Tuple] = "("~>repsep(tuppair, ",")<~")" ^^
    { case (l: List[_]) => (Tuple(l.asInstanceOf[List[(String, TupleAttributeExpr)]].toMap)) }

  def singleton: Parser[Singleton] = "{"~>tupleexpr<~"}" ^^ 
    { case t:TupleExpr => Singleton(t) } 

  def forinit: Parser[(TupleVarRef, BagExpr)] = "for"~tuplevarref~"in"~bagexpr ^^
    {case "for"~(t:String)~"in"~(b1:BagExpr) => 
      val tr = TupleVarRef(t, b1.tp.tp)
      scope = scope + (t -> tr.varDef)
      (tr, b1)
    }

  def forunion: Parser[ForeachUnion] = forinit~"union"~bagexpr ^^ 
    {case (t:TupleVarRef, b1: BagExpr)~"union"~(b2:BagExpr) =>
      ForeachUnion(t, b1, b2) }

  def tupleexpr: Parser[TupleExpr] = tuple
  def bagexpr: Parser[BagExpr] = forunion | singleton | bagvarref
  
  def tupleattr: Parser[TupleAttributeExpr] = forunion | project | singleton | bagvarref
  def term: Parser[Expr] = forunion | singleton | tuple | project | bagvarref

}

object Parser {
  def apply(tbls: Map[String, BagType]): Parser = new Parser(tbls)
}