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


  /** Base types 
    * this does not support long, 
    * prevents floatingPointNumber from overriding wholeNumber
    **/
  def numeric: Parser[NumericConst] = floatingPointNumber ^^
    { case v if !v.contains(".") => NumericConst(v.toInt, IntType) 
      case v => NumericConst(v.toDouble, DoubleType) }
  def primitive: Parser[PrimitiveConst] = booltype | strtype
  def r(str:String) = ("(?i)" + str).r
  def booltype: Parser[PrimitiveConst] = (r("true") | r("false")) ^^
    { case v => PrimitiveConst(v.toBoolean, BoolType) }
  def strtype: Parser[PrimitiveConst] = stringLiteral ^^
    { case v => PrimitiveConst(v.toString, StringType) }
    
 
  /** Variable references
    * Numeric and Primitive var references need implemented
    **/  
  def tuplevarref: Parser[String] = ident ^^
    { case (s: String) => s }
  def bagvarref: Parser[BagVarRef] = ident ^^
    { case (s: String) => BagVarRef(s, tbls(s)) }

  def project: Parser[TupleAttributeExpr] = tuplevarref~"."~ident ^^
    { case (v:String)~"."~(l:String) => Project(defToRef(v), l) }

  def tuppair: Parser[(String, TupleAttributeExpr)] = ident~":="~tupleattr ^^
    { case (v:String)~":="~t => (v, t)}


  def singleton: Parser[Singleton] = "{"~>tupleexpr<~"}" ^^ 
    { case t:TupleExpr => Singleton(t) } 

  def eq: Parser[OpCmp] = "=" ^^ { case o => OpEq }
  def ne: Parser[OpCmp] = "!=" ^^ { case o => OpNe }
  def gt: Parser[OpCmp] = ">" ^^ { case o => OpGt }
  def ge: Parser[OpCmp] = ">=" ^^ { case o => OpGe }

  def opcmp: Parser[OpCmp] = eq | ne | ge | gt

  def cmp: Parser[PrimitiveCmp] = primexpr~opcmp~primexpr ^^
    { case (e1:PrimitiveExpr)~(o:OpCmp)~(e2:PrimitiveExpr) => PrimitiveCmp(o, e1, e2) }
  def and: Parser[And] = condexpr~"&&"~condexpr ^^
    { case (e1:CondExpr)~"&&"~(e2:CondExpr) => And(e1, e2) }
  def or: Parser[Or] = condexpr~"||"~condexpr ^^
    { case (e1:CondExpr)~"||"~(e2:CondExpr) => Or(e1, e2) }
  def not: Parser[Not] = "!"~condexpr ^^ 
    { case "!"~(e1:CondExpr) => Not(e1); case s => sys.error(s"error reading not expression $s")}

  // and, or, not broken
  def condexpr: Parser[CondExpr] = cmp | and | or | not

  def bagifthenelse: Parser[BagIfThenElse] = "if"~"("~cmp~")"~"then"~bagexpr ^^
    {case "if"~"("~(cond:CondExpr)~")"~"then"~(t:BagExpr) => BagIfThenElse(cond, t, None) }
  
  def forinit: Parser[(TupleVarRef, BagExpr)] = "for"~tuplevarref~"in"~bagexpr ^^
    {case "for"~(t:String)~"in"~(b1:BagExpr) => 
      val tr = TupleVarRef(t, b1.tp.tp)
      scope = scope + (t -> tr.varDef)
      (tr, b1)
    }

  def forunion: Parser[ForeachUnion] = forinit~"union"~bagexpr ^^ 
    {case (t:TupleVarRef, b1: BagExpr)~"union"~(b2:BagExpr) =>
      ForeachUnion(t, b1, b2) }

  def arglist: Parser[List[String]] = "{"~>repsep(ident, ",")<~"}" ^^ 
    {case (l:List[_]) => l.asInstanceOf[List[String]] }
  def sumby: Parser[ReduceByKey] = "("~>bagexpr~").sumBy("~arglist~","~arglist<~")" ^^
    {case (e1:BagExpr)~").sumBy("~(k:List[_])~","~(v:List[_]) => 
        ReduceByKey(e1, k.asInstanceOf[List[String]], v.asInstanceOf[List[String]]) 
     case _ => sys.error("sumBy parameter error") }

  def tuple: Parser[Tuple] = "("~>repsep(tuppair, ",")<~")" ^^
    { case (l: List[_]) => (Tuple(l.asInstanceOf[List[(String, TupleAttributeExpr)]].toMap)) }

  def tupleexpr: Parser[TupleExpr] = tuple
  def bagexpr: Parser[BagExpr] = 
    sumby | forunion | bagifthenelse | singleton | project.asInstanceOf[Parser[BagExpr]] | bagvarref

  //def numconst: Parser[NumericConst] = 
  //def primconst: Parser[PrimitiveConst] = 
  def primexpr: Parser[PrimitiveExpr] = project.asInstanceOf[Parser[PrimitiveExpr]] | condexpr | primitive 
  def numexpr: Parser[NumericExpr] = project.asInstanceOf[Parser[NumericExpr]]
  def arithexpr: Parser[ArithmeticExpr] = numexpr~oparith~numexpr ^^ 
    { case (e1:NumericExpr)~(op:OpArithmetic)~(e2:NumericExpr) => ArithmeticExpr(op, e1, e2) }

  def oparith: Parser[OpArithmetic] =  plus | minus | mult | divide | mod
  def plus: Parser[OpArithmetic] = "+" ^^ { case o => OpPlus }
  def minus: Parser[OpArithmetic] = "-" ^^ { case o => OpMinus }
  def mult: Parser[OpArithmetic] = "*" ^^ { case o => OpMultiply }
  def divide: Parser[OpArithmetic] = "/" ^^ { case o => OpDivide }
  def mod: Parser[OpArithmetic] = "mod" ^^ { case o => OpMod }

  def tupleattr: Parser[TupleAttributeExpr] = 
    sumby | forunion | bagifthenelse | arithexpr | project | singleton | bagvarref
  def term: Parser[Expr] = 
    sumby | forunion | bagifthenelse | singleton | tuple | project | bagvarref

}

object Parser {
  def apply(tbls: Map[String, BagType]): Parser = new Parser(tbls)
}
