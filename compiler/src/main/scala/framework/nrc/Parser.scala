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
  def primitive: Parser[PrimitiveConst] = booltype | strtype | numeric.asInstanceOf[Parser[PrimitiveConst]]
  def r(str:String) = ("(?i)" + str).r
  def booltype: Parser[PrimitiveConst] = (r("true") | r("false")) ^^
    { case v => PrimitiveConst(v.toBoolean, BoolType) }
  def strtype: Parser[PrimitiveConst] = stringLiteral ^^
    { case v => PrimitiveConst(v.toString.replace("\"", ""), StringType) }
    
 
  /** Variable references
    * Numeric and Primitive var references need implemented
    **/  
  def tuplevarref: Parser[String] = ident ^^
    { case (s: String) => s }
  def tupvarref: Parser[TupleVarRef] = ident ^^ 
    { case (s: String) => defToRef(s) }
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

  def and: Parser[And] = cmp~"&&"~condexpr ^^ 
    { case (e1:CondExpr)~"&&"~(e2:CondExpr) => And(e1, e2) }
  
  def or: Parser[Or] = cmp~"||"~condexpr ^^
    { case (e1:CondExpr)~"||"~(e2:CondExpr) => Or(e1, e2) }
  def not: Parser[Not] = "!"~"("~condexpr~")" ^^ 
    { case "!"~"("~(e1:CondExpr)~")" => Not(e1); case _ => sys.error("Not expression (!) improperly parsed.")}

  def condexpr: Parser[CondExpr] = and | or | not | cmp

  def ifthen: Parser[IfThenElse] = "if"~"("~condexpr~")"~"then"~term~(("else"~>term)?) ^^
    { case "if"~"("~(cond:CondExpr)~")"~"then"~(t:BagExpr)~Some(e:BagExpr) => BagIfThenElse(cond, t, Some(e)) 
      case "if"~"("~(cond:CondExpr)~")"~"then"~(t:BagExpr)~None => BagIfThenElse(cond, t, None) 
      case "if"~"("~(cond:CondExpr)~")"~"then"~(t:NumericExpr)~Some(e:NumericExpr) => NumericIfThenElse(cond, t, e) 
      case "if"~"("~(cond:CondExpr)~")"~"then"~(t:PrimitiveExpr)~Some(e:PrimitiveExpr) => PrimitiveIfThenElse(cond, t, e) 
      case _ => sys.error("if statement not formatted correctly") }
       
  // deprecated     
  def bagifthenelse: Parser[BagIfThenElse] = "if"~"("~condexpr~")"~"then"~bagexpr ^^
    {case "if"~"("~(cond:CondExpr)~")"~"then"~(t:BagExpr) => BagIfThenElse(cond, t, None)}
  
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
  def groupby: Parser[GroupByKey] = "("~>bagexpr~").groupBy("~arglist~","~arglist~","~stringLiteral<~")" ^^
    {case (e1:BagExpr)~").groupBy("~(k:List[_])~","~(v:List[_])~","~(s:String) => 
        GroupByKey(e1, k.asInstanceOf[List[String]], v.asInstanceOf[List[String]], s.replace("\"", "")) 
     case _ => sys.error("groupBy parameter error") }

  def tuple: Parser[Tuple] = "("~>repsep(tuppair, ",")<~")" ^^
    { case (l: List[_]) => (Tuple(l.asInstanceOf[List[(String, TupleAttributeExpr)]].toMap)) }

  def tupleexpr: Parser[TupleExpr] = tuple | tupvarref
  def bagexpr: Parser[BagExpr] = 
    groupby | sumby | forunion | ifthen.asInstanceOf[Parser[BagExpr]] | singleton | project.asInstanceOf[Parser[BagExpr]] | bagvarref

  //def numconst: Parser[NumericConst] = 
  //def primconst: Parser[PrimitiveConst] = 
  def primexpr: Parser[PrimitiveExpr] = 
    ifthen.asInstanceOf[Parser[PrimitiveExpr]] | project.asInstanceOf[Parser[PrimitiveExpr]] | primitive 
  def basenumexpr: Parser[NumericExpr] = 
    ifthen.asInstanceOf[Parser[NumericExpr]] | project.asInstanceOf[Parser[NumericExpr]] | numeric
  def numexpr: Parser[NumericExpr] =  
    arithparen.asInstanceOf[Parser[NumericExpr]] | basenumexpr

  def arithparen: Parser[ArithmeticExpr] = "("~>arithexpr<~")"
  def arithexpr: Parser[ArithmeticExpr] = numexpr~oparith~numexpr ^^ 
    { case (e1:NumericExpr)~(op:OpArithmetic)~(e2:NumericExpr) => ArithmeticExpr(op, e1, e2) }

  // this needs to handle appending to table and creating variable reference
  def assign: Parser[Assignment] = ident~"<="~term ^^ 
    { case (v:String)~"<="~t => Assignment(v, t) }

  def dedup: Parser[DeDup] = "dedup("~>bagexpr<~")" ^^
    { case (e1:BagExpr) => DeDup(e1) }

  def oparith: Parser[OpArithmetic] =  plus | minus | mult | divide | mod
  def plus: Parser[OpArithmetic] = "+" ^^ { case o => OpPlus }
  def minus: Parser[OpArithmetic] = "-" ^^ { case o => OpMinus }
  def mult: Parser[OpArithmetic] = "*" ^^ { case o => OpMultiply }
  def divide: Parser[OpArithmetic] = "/" ^^ { case o => OpDivide }
  def mod: Parser[OpArithmetic] = "mod" ^^ { case o => OpMod }

  def tupleattr: Parser[TupleAttributeExpr] = 
    groupby | sumby | dedup | forunion | arithexpr | numexpr | ifthen.asInstanceOf[Parser[TupleAttributeExpr]] | project | singleton | bagvarref
    
  def term: Parser[Expr] = 
    groupby | sumby | dedup | forunion | arithexpr | numexpr | ifthen.asInstanceOf[Parser[Expr]] | singleton | tuple | project | bagvarref | primexpr

}

object Parser {
  def apply(tbls: Map[String, BagType]): Parser = new Parser(tbls)
}
