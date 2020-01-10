package shredding.wmcc

import shredding.core._
import scala.collection.mutable.HashMap
import shredding.utils.Utils.ind

/**
  * Based compilers for WMCC and algebra data operators
  */

trait Base {
  type Rep
  def inputref(x: String, tp:Type): Rep
  def input(x: List[Rep]): Rep 
  def constant(x: Any): Rep
  def emptysng: Rep
  def unit: Rep 
  def sng(x: Rep): Rep
  def weightedsng(x: Rep, q: Rep): Rep
  def tuple(fs: List[Rep]): Rep
  def record(fs: Map[String, Rep]): Rep
  def mult(e1: Rep, e2: Rep): Rep
  def equals(e1: Rep, e2: Rep): Rep
  def lt(e1: Rep, e2: Rep): Rep
  def gt(e1: Rep, e2: Rep): Rep
  def lte(e1: Rep, e2: Rep): Rep
  def gte(e1: Rep, e2: Rep): Rep
  def and(e1: Rep, e2: Rep): Rep
  def not(e1: Rep): Rep
  def or(e1: Rep, e2: Rep): Rep
  def project(e1: Rep, field: String): Rep
  def ifthen(cond: Rep, e1: Rep, e2: Option[Rep] = None): Rep
  def merge(e1: Rep, e2: Rep): Rep
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep
  def dedup(e1: Rep): Rep
  def bind(e1: Rep, e: Rep => Rep): Rep 
  def groupby(e1: Rep, g: Rep => Rep, v: Rep => Rep): Rep
  def named(n: String, e: Rep): Rep
  def linset(e: List[Rep]): Rep
  def lookup(lbl: Rep, dict: Rep): Rep
  def emptydict: Rep
  def bagdict(lbl: Rep, flat: Rep, dict: Rep): Rep
  def tupledict(fs: Map[String, Rep]): Rep
  def dictunion(d1: Rep, d2: Rep): Rep
  def select(x: Rep, p: Rep => Rep, e: Rep => Rep): Rep
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep
  def unnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep
  def outerunnest(e1: Rep, r: List[Rep] => Rep, p: List[Rep] => Rep): Rep
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p: Rep => Rep): Rep
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep): Rep
  def lkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep
  def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep
  def cogroup(e1: Rep, es: List[Rep], ps: List[Rep] => Rep):Rep
}

trait BaseStringify extends Base{
  type Rep = String
  def inputref(x: String, tp: Type): Rep = x
  def input(x: List[Rep]): Rep = s"{${x.mkString(",")}}"
  def constant(x: Any): Rep = x.toString
  def emptysng: Rep = "{}"
  def unit: Rep = "()"
  def sng(x: Rep): Rep = s"{ $x }"
  def weightedsng(x: Rep, q: Rep): Rep = s"Weighted({$x}, $q)"
  def tuple(fs: List[Rep]) = s"(${fs.mkString(",")})"
  def record(fs: Map[String, Rep]): Rep = 
    s"(${fs.map(f => f._1 + " := " + f._2).mkString(",")})"
  def mult(e1: Rep, e2: Rep): Rep = s"(${e1} * ${e2})" 
  def equals(e1: Rep, e2: Rep): Rep = s"${e1} == ${e2}"
  def lt(e1: Rep, e2: Rep): Rep = s"${e1} < ${e2}"
  def gt(e1: Rep, e2: Rep): Rep = s"${e1} > ${e2}"
  def lte(e1: Rep, e2: Rep): Rep = s"${e1} <= ${e2}"
  def gte(e1: Rep, e2: Rep): Rep = s"${e1} >= ${e2}"
  def and(e1: Rep, e2: Rep): Rep = s"${e1}, ${e2}"
  def not(e1: Rep): Rep = s"!(${e1})"
  def or(e1: Rep, e2: Rep): Rep = s"${e1} || ${e2}"
  def project(e1: Rep, field: String): Rep = s"${e1}.${field}"
  def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = e2 match {
    case Some(a) => s"if (${cond}) then ${e1} else ${a}"
    case _ => s"if (${cond}) then ${e1}"
  }
  def merge(e1: Rep, e2: Rep): Rep = s"${e1} U ${e2}"
  def bind(e1: Rep, e: Rep => Rep): Rep = {
    val x = Variable.fresh(StringType)
    s"{ ${e(x.quote)} | ${x.quote} := ${e1} }"
  }
  def groupby(e1: Rep, g: Rep => Rep, v: Rep => Rep): Rep = {
    val v2 = Variable.fresh(StringType)
    s"(${e1}).groupBy(${g(v2.quote)}, ${v(v2.quote)})"
  }
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = { 
    val x = Variable.fresh(StringType)
    p(x.quote) match {
      case "true" => s"{ ${e(x.quote)} | ${x.quote} <- ${e1} }"
      case px => s"{ ${e(x.quote)} | ${x.quote} <- ${e1}, ${px} }"
    } 
  }
  def dedup(e1: Rep): Rep = s"DeDup(${e1})"
  def named(n: String, e: Rep): Rep = s"${n} := ${e}"
  def linset(e: List[Rep]): Rep = e.mkString("\n\n")
  def lookup(lbl: Rep, dict: Rep): Rep = s"Lookup(${lbl}, ${dict})"
  def emptydict: Rep = s"Nil"
  def bagdict(lbl: Rep, flat: Rep, dict: Rep): Rep = s"(${lbl} -> ${flat}, ${dict})"
  def tupledict(fs: Map[String, Rep]): Rep =
    s"(${fs.map(f => f._1 + " := " + f._2).mkString(",")})"
  def dictunion(d1: Rep, d2: Rep): Rep = s"${d1} U ${d2}"
  def select(x: Rep, p: Rep => Rep, e: Rep => Rep): Rep = { 
    val v = Variable.fresh(StringType).quote
    s""" | SELECT[ ${p(v)}, ${e(v)} ](${x} )""".stripMargin
  }
  def reduce(x: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = { 
    val v = Variable.fresh(StringType)
    s""" | REDUCE[ ${f(List(v.quote))} / ${p(List(v.quote))} ](${x})""".stripMargin
  }
  def unnest(x: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(StringType)
    s""" | UNNEST[ ${f(List(v1.quote))} / ${p(List(v.quote))} ](${x})""".stripMargin
  }
  def nest(x: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep): Rep = {
    val v1 = Variable.fresh(StringType) 
    val v2 = Variable.fresh(StringType)
    val acc = e(List(v1.quote)) match { case "1" => "+"; case _ => "U" }
    s""" | NEST[ ${acc} / ${e(List(v1.quote))} / ${f(List(v2.quote))}, ${p(List(v2.quote))} ](${x})""".stripMargin
  }
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) JOIN[${p1(List(v1.quote))} = ${p2(v2.quote)}]( 
         | ${ind(e2)})""".stripMargin
  }
  def outerunnest(x: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(StringType)
    s""" | OUTERUNNEST[ ${f(List(v1.quote))} / ${p(List(v.quote))} ](${x})""".stripMargin
  }
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) OUTERJOIN[${p1(List(v1.quote))} = ${p2(v2.quote)}]( 
         | ${ind(e2)})""".stripMargin
  }
  def lkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) LOOKUP[${p1(List(v1.quote))} = ${p2(v2.quote)}, ${p3(List(v1.quote, v2.quote))}](
         | ${ind(e2)})""".stripMargin
  }
  def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) OUTERLOOKUP[${p1(List(v1.quote))} = ${p2(v2.quote)}, ${p3(List(v1.quote, v2.quote))}](
         | ${ind(e2)})""".stripMargin
  }
  def cogroup(e1: Rep, es: List[Rep], ps: List[Rep] => Rep): Rep = {
    s"""| ($e1) LOOKUP(${es.mkString(",")})"""
  }

}

/**
  * Generates expression nodes defined in Expr.scala
  */

trait BaseCompiler extends Base {
  type Rep = CExpr 
  def inputref(x: String, tp: Type): Rep = tp match {
    case BagDictCType(BagCType(TTupleType(List(EmptyCType, BagCType(tup)))), tdict) =>
      tdict match {
        case EmptyDictCType => InputRef(x, BagCType(tup))
        case TupleDictCType(fs) if fs.values.toSet == Set(EmptyDictCType) => 
          InputRef(x, BagDictCType(BagCType(tup), EmptyDictCType))
        case _ => InputRef(x, tp)
      }
    case _ => InputRef(x, tp)
  }
  def input(x: List[Rep]): Rep = Input(x)
  def constant(x: Any): Rep = Constant(x)
  def emptysng: Rep = EmptySng
  def unit: Rep = CUnit
  def sng(x: Rep): Rep = Sng(x)
  def weightedsng(x: Rep, q: Rep): Rep = WeightedSng(x, q)
  def tuple(fs: List[Rep]): Rep = Tuple(fs)
  def record(fs: Map[String, Rep]): Rep = Record(fs)
  def mult(e1: Rep, e2: Rep): Rep = Multiply(e1, e2)
  def equals(e1: Rep, e2: Rep): Rep = Equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = Lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = Gt(e1, e2)
  def lte(e1: Rep, e2: Rep): Rep = Lte(e1, e2)
  def gte(e1: Rep, e2: Rep): Rep = Gte(e1, e2)
  def and(e1: Rep, e2: Rep): Rep = And(e1, e2)
  def not(e1: Rep): Rep = Not(e1)
  def or(e1: Rep, e2: Rep): Rep = Or(e1, e2)
  def project(e1: Rep, e2: String): Rep = Project(e1, e2)
  def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = If(cond, e1, e2)
  def merge(e1: Rep, e2: Rep): Rep = Merge(e1, e2)
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    val v = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
    Comprehension(e1, v, p(v), e(v))
  }
  def dedup(e1: Rep): Rep = {
    assert(!e1.tp.isInstanceOf[PrimitiveType])
    CDeDup(e1)
  }
  def bind(e1: Rep, e: Rep => Rep): Rep = {
      val v = Variable.fresh(e1.tp)
      Bind(v, e1, e(v)) 
  }
  def groupby(e1: Rep, g: Rep => Rep, v: Rep => Rep): Rep = {
    val v2 = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
    CGroupBy(e1, v2, g(v2), v(v2)) 
  }
  def named(n: String, e: Rep): Rep = CNamed(n, e)
  def linset(e: List[Rep]): Rep = LinearCSet(e)
  def lookup(lbl: Rep, dict: Rep): Rep = CLookup(lbl, dict)
  def emptydict: Rep = EmptyCDict
  def bagdict(lbl: Rep, flat: Rep, dict: Rep): Rep = BagCDict(lbl, flat, dict)
  def tupledict(fs: Map[String, Rep]): Rep = TupleCDict(fs)
  def dictunion(d1: Rep, d2: Rep): Rep = DictCUnion(d1, d2)
  def select(x: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    val v = Variable.freshFromBag(x.tp) 
    Select(x, v, p(v), e(v))
  }
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    val v = e1.tp match {
      case BagDictCType(flat, tdict) => vars(flat.tp)
      case _ => vars(e1.tp.asInstanceOf[BagCType].tp)
    }
    Reduce(e1, v, f(v), p(v))
  }
  def unnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    val v1 = e1.tp match {
      case BagDictCType(flat, dict) => vars(flat.tp)//._1.tp
      case btp:BagCType => vars(btp.tp)
    }
    val fv = f(v1) 
    val v = Variable.freshFromBag(fv.tp)
    Unnest(e1, v1, fv, v, p(v1 :+ v))
  }
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep): Rep = {
    val v1 = e1.tp match {
      case BagDictCType(flat, dict) => vars(flat.tp) 
      case btp:BagCType => vars(btp.tp)
    }
    val fv = f(v1) // groups
    val ev = e(v1) 
    val v = ev.tp match {
      case p:PrimitiveType =>
        fv.tp match {
          case RecordCType(fs) => Variable.fresh(RecordCType(fs + ("_2" -> ev.tp)))
          case fvtp =>Variable.fresh(TTupleType(List(fvtp, ev.tp)))
        }
      case _ => 
        Variable.fresh(TTupleType(List(fv.tp, BagCType(ev.tp))))
    }
    Nest(e1, v1, fv, ev, v, p(v1:+v), g(v1))
  }
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val v1 = e1.tp match {
      case BagDictCType(flat, dict) => vars(flat.tp)
      case BagCType(tup) => vars(tup) 
    }
    val v2 = Variable.freshFromBag(e2.tp)
    Join(e1, e2, v1, p1(v1), v2, p2(v2))
  }
  def outerunnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    val v1 = e1.tp match {
      case BagDictCType(flat, dict) => vars(flat.tp)//._1.tp
      case btp:BagCType => vars(btp.tp)
    }
    val fv = f(v1) 
    val v = Variable.freshFromBag(fv.tp)
    OuterUnnest(e1, v1, fv, v, p(v1 :+ v))
  }
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val v1 = e1.tp match {
      case BagDictCType(flat, dict) => vars(flat.tp)
      case BagCType(tup) => vars(tup) 
    }
    val v2 = Variable.freshFromBag(e2.tp)
    OuterJoin(e1, e2, v1, p1(v1), v2, p2(v2))
  }
  def lkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = {
    val v1 = e1.tp match {
      case BagDictCType(flat, dict) => vars(flat.tp) 
      case BagCType(tup) => vars(tup) 
    }
    val v2 = Variable.freshFromBag(e2.tp) 
    Lookup(e1, e2, v1, p1(v1), v2, p2(v2), p3(v1 :+ v2))
  }
  def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = {
    val v1 = e1.tp match {
      case BagDictCType(flat, dict) => vars(flat.tp) 
      case BagCType(tup) => vars(tup) 
    }
    val v2 = Variable.freshFromBag(e2.tp) 
    OuterLookup(e1, e2, v1, p1(v1), v2, p2(v2), p3(v1 :+ v2))
  }
  def cogroup(e1: Rep, es: List[Rep], ps: List[Rep] => Rep): Rep = {
    val vs = (e1.tp match {
      case BagDictCType(flat, dict) => vars(flat.tp)
      case BagCType(tup) => vars(tup)
    }) ++ es.map(v => Variable.freshFromBag(v.tp))
    CoGroup(e1, es, vs, ps(vs))
  }

  /**
    * Why am I using this still??
    */
  def vars(e: Type): List[Variable] = e match {
    case TTupleType(List(EmptyCType, BagCType(tup))) => List(Variable.fresh(tup))
    //case BagDictCType(flat, dict) => List(Variable.fresh(flat.tp))
    case TTupleType(tps) if tps.head.isInstanceOf[LabelType] => 
      List(Variable.fresh(e)) // won't catch all cases
    case TTupleType(tps) if (tps.head.isInstanceOf[RecordCType] && tps.last.isInstanceOf[PrimitiveType] && tps.size == 2) => 
      List(Variable.fresh(e)) // fix this
    case TTupleType(tps) => tps.flatMap(vars(_)) 
    case _ => List(Variable.fresh(e))
  }

}

/**
  * This is named the base indexer, but maybe I will make this the base 
  * compiler for pre-anf'ed bottom up optimizations. 
  */
trait BaseDictNameIndexer extends BaseCompiler {

  def isDictType(tp: Type): (Type, Boolean) = tp match {
    case BagCType(TTupleType(List(EmptyCType, BagCType(tup)))) => (tup, true)
    case BagDictCType(BagCType(rct @ RecordCType(_)), tdict) => (rct, true)
    case BagDictCType(flat, dict) => isDictType(flat)
    case _ => (tp, false)
  }
  
  override def project(e1: Rep, f: String): Rep = e1 match {
    // this case needs work
    case Project(InputRef(n, TupleDictCType(fs)), "_2") => 
      InputRef(n+"_2"+f, fs(f))
    case InputRef(n, BagCType(TTupleType(List(EmptyCType, btp)))) => 
      InputRef(n+f, btp)
    case InputRef(n, BagDictCType(BagCType(TTupleType(List(EmptyCType, btp))), tdict)) if f == "_1" => 
      InputRef(n+f, btp)
    case InputRef(n, BagDictCType(flat, dict)) => 
      if (f == "_1") InputRef(n+f, flat)
      else if (f == "_2") project(InputRef(n, dict), f)
      else super.project(e1, f)
    case _ => super.project(e1, f) 
  }

  override def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = e1 match {        
      case OuterLookup(e3, e4, v1, pa, v2, pb, pc) => (e2, e4) match {
        case (InputRef(n1, _), InputRef(n2, _)) if n1.split("_").size == n2.split("_").size =>
          CoGroup(e3, List(e2, e4), v1 :+ v2, p1(v1 :+ v2))
        case _ => super.outerlkup(e1, e2, p1, p2, p3)
      } 
    case _ => 
      super.outerlkup(e1, e2, p1, p2, p3)
  }

}

trait BasePlanOptimizer extends BaseCompiler {
  // reduce conditionals
  override def equals(e1: Rep, e2: Rep): Rep = (e1, e2) match {
    case (Constant(x1), Constant(x2)) => constant(x1 == x2)
    case _ => super.equals(e1, e2)
  }

  override def not(e1: Rep): Rep = e1 match {
    case Constant(true) => super.constant(false)
    case Constant(false) => super.constant(true)
    case _ => super.not(e1)
  }

  override def and(e1: Rep, e2: Rep): Rep  = (e1, e2) match {
    case (Constant(true), Constant(true)) => super.constant(true)
    case (Constant(false), _) => super.constant(false)
    case (_, Constant(false)) => super.constant(false)
    case (Constant(true), e3) => e3
    case (e3, Constant(true)) => e3
    case _ => super.and(e1, e2)
  }

  override def or(e1: Rep, e2: Rep): Rep  = (e1, e2) match {
    case (Constant(false), Constant(false)) => super.constant(false)
    case (Constant(true), _) => super.constant(true)
    case (_, Constant(true)) => super.constant(true)
    case _ => super.and(e1, e2)
  }

  override def bind(e1: Rep, e: Rep => Rep): Rep = e(e1)

  override def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = cond match {
    case Constant(true) => e1
    case Constant(false) => e2 match {
      case Some(a) =>  a
      case _ => super.emptysng
    }
    case _ => super.ifthen(cond, e1, e2)
  }

}

/**
  * Scala evaluation 
  */
trait BaseScalaInterp extends Base{
  type Rep = Any
  val ctx = scala.collection.mutable.Map[Any, Any]()
  var doteq = true
  def inputref(x: String, tp: Type): Rep = ctx(x)
  def input(x: List[Rep]): Rep = x
  def constant(x: Any): Rep = x
  def emptysng: Rep = Nil
  def unit: Rep = ()
  def sng(x: Rep): Rep = List(x)
  def weightedsng(x: Rep, q: Rep) = {
    if (q.asInstanceOf[Int] > 0) { (1 to q.asInstanceOf[Int]).map(w => x) } else { emptysng }
  }
  def tuple(x: List[Rep]): Rep = x
  def record(fs: Map[String, Rep]): Rep = {
    if (doteq) RecordValue(fs.asInstanceOf[Map[String, Rep]], RecordValue.newId)
    else Rec(fs.asInstanceOf[Map[String, Rep]])
  }
  def mult(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Double] * e2.asInstanceOf[Double]
  def equals(e1: Rep, e2: Rep): Rep = e1 == e2
  def lt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] < e2.asInstanceOf[Int]
  def gt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] > e2.asInstanceOf[Int]
  def lte(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] <= e2.asInstanceOf[Int]
  def gte(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] >= e2.asInstanceOf[Int]
  def and(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Boolean] && e2.asInstanceOf[Boolean]
  def not(e1: Rep): Rep = !e1.asInstanceOf[Boolean]
  def or(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Boolean] || e2.asInstanceOf[Boolean]
  def project(e1: Rep, f: String) = f match {
    //case "_1" if e1.isInstanceOf[List[(Int, List[_])]] => e1.asInstanceOf[List[(Int, List[_])]].head._2 //no
    case "_1" => e1.asInstanceOf[Product].productElement(0)
    case "_2" if e1.isInstanceOf[RecordValue] => e1.asInstanceOf[RecordValue].map("v")
    case "_2" => e1.asInstanceOf[Product].productElement(1)
    case f => e1 match {
      case None => None
      case m:Rec => m.map.get("map") match {
        case Some(a) => a.asInstanceOf[Map[String, Any]](f)
        case _ => m.map(f)
      }
      case m:RecordValue => m.map(f)
      case c:CaseClassRecord => 
        val field = c.getClass.getDeclaredFields.find(_.getName == f).get
        field.setAccessible(true)
        field.get(c)
      //case m:HashMap[_,_] => m(f.asInstanceOf[_])
      case l:List[_] => l.map(project(_,f))
      case p:Product => p.productElement(f.toInt)
      case t => sys.error(s"unsupported projection type ${t.getClass} for object:\n$t") 
    }
  }
  def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = e2 match {
    case Some(a) => if (cond.asInstanceOf[Boolean]) { e1 } else { a }
    case _ => if (cond.asInstanceOf[Boolean]) { e1 } else { Nil }
  } 
  def merge(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[List[_]] ++ e2.asInstanceOf[List[_]]
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    e1 match {
      case Nil => e(Nil) match { case i:Int => 0; case i:Double => 0.0; case _ => Nil }
      case data @ (head :: tail) => e(head) match {
        case i:Int =>
          data.withFilter(p.asInstanceOf[Rep => Boolean]).map(e).asInstanceOf[List[Int]].sum
        case i:Double =>
          data.withFilter(p.asInstanceOf[Rep => Boolean]).map(e).asInstanceOf[List[Double]].sum
	case _ => 
          data.withFilter(p.asInstanceOf[Rep => Boolean]).
            flatMap(e.asInstanceOf[Rep => scala.collection.GenTraversableOnce[Rep]])
      }
    }
  }
  def dedup(e1: Rep): Rep = {
    val data = e1.asInstanceOf[List[_]]
    data.head match {
      case r:Rec => data.distinct
      case r:RecordValue => data.map(_.asInstanceOf[RecordValue].toRec).distinct
    }
  }
  def named(n: String, e: Rep): Rep = {
    ctx(n) = e
    println(n+" := "+e+"\n")
    e
  }
  def linset(e: List[Rep]): Rep = e
  def bind(e1: Rep, e: Rep => Rep): Rep = e(e1) //ctx.getOrElseUpdate(e1, e(e1))
  def groupby(e1: Rep, g: Rep => Rep, v: Rep => Rep): Rep = {
    val grp = e1.asInstanceOf[List[_]].groupBy(g)
    e1.asInstanceOf[List[_]].head match {
      case Int => grp.map{ case (g2, v2) => 
        (g2, v2.asInstanceOf[List[_]].map(v).asInstanceOf[List[Int]].sum) }
      case Double => grp.map{ case (g2, v2) => 
        (g2, v2.asInstanceOf[List[_]].map(v).asInstanceOf[List[Double]].sum) }
      case _ => grp.map{ case (g2, v2) => (g2, v2.asInstanceOf[List[_]].map(v)) }
    }
  }
  def lookup(lbl: Rep, dict: Rep): Rep = dict match {
    case (flat, tdict) => flat match {
      case (head:Map[_,_]) :: tail => flat
      case _ => flat.asInstanceOf[List[(_,_)]].withFilter(_._1 == lbl).map(_._2)
    }
    case _ => dict // (flat, ())
  }
  def emptydict: Rep = ()
  def bagdict(lbl: Rep, flat: Rep, dict: Rep): Rep = (flat.asInstanceOf[List[_]].map(v => (lbl, v)), dict)
  def tupledict(fs: Map[String, Rep]): Rep = fs
  def dictunion(d1: Rep, d2: Rep): Rep = d1 // TODO
  def select(x: Rep, p: Rep => Rep, e: Rep => Rep): Rep = { 
    x.asInstanceOf[List[_]].filter(p.asInstanceOf[Rep => Boolean]).map(e)
  }
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    e1.asInstanceOf[List[_]].map(v2 => f(tupleVars(v2))) 
  }
  def unnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    e1.asInstanceOf[List[_]].flatMap{
      v =>
        val v1 = tupleVars(v)
        f(v1).asInstanceOf[List[_]].map{ v2 => 
          val nv = v1 :+ v2
          if (p.asInstanceOf[Rep => Boolean](nv)) { nv } else { Nil }
      }
    }
  }
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    outerjoin(e1, e2, p1, p2)
  } 
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep): Rep = {
    val grps = e1.asInstanceOf[List[_]].groupBy(v => f(tupleVars(v)))
    val res = e1 match {
      case Nil => e(Nil) match { case i:Int => 0; case _ => Nil }
      case head :: tail => e(head.asInstanceOf[List[_]]) match {
        case i:Int => 
          grps.map(x1 => x1._1.asInstanceOf[List[_]] :+ x1._2.foldLeft(0)((acc, v1) => { 
            // this should be if g(x2) != None 
            if (g(v1.asInstanceOf[List[_]]) != None && p(v1.asInstanceOf[List[_]]).asInstanceOf[Boolean]) { 
              val nacc = e(v1.asInstanceOf[List[_]]) match { case Nil => 0; case c => c.asInstanceOf[Int] }
              acc + nacc 
            } else { acc } 
           })).toList
        case i:Double =>
          grps.map(x1 => x1._1.asInstanceOf[List[_]] :+ x1._2.foldLeft(0.0)((acc, v1) => { 
            // this should be if g(x2) != None  
            if (g(v1.asInstanceOf[List[_]]) != None && p(v1.asInstanceOf[List[_]]).asInstanceOf[Boolean]) { 
              acc + e(v1.asInstanceOf[List[_]]).asInstanceOf[Double] } else { acc } 
           })).toList
	case _ => 
          grps.map(x1 => x1._1.asInstanceOf[List[_]] :+ x1._2.flatMap(v => { 
            val v2 = tupleVars(v)
            if (!g(v2).asInstanceOf[List[_]].contains(None) && p(v2).asInstanceOf[Boolean]) { 
              List(e(v2)) 
            } else { Nil } })).toList
        }
     }
     //println(res)
     res
  }
  def outerunnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    e1.asInstanceOf[List[_]].flatMap{
      v =>
        val v1 = tupleVars(v)
        f(v1).asInstanceOf[List[_]].map{ v2 => 
          val nv = v1 :+ v2
          if (p(nv).asInstanceOf[Boolean]) { nv } else { v1 :+ None }
      }
    }
  }
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val hm = e1.asInstanceOf[List[_]].groupBy(v => p1(tupleVars(v)))
    e2.asInstanceOf[List[_]].flatMap(v2 => hm.get(p2(v2)) match {
      case Some(v1) => v1.map(v => tupleVars(v) :+ v2)
      case _ => Nil
    })
  }
  // TODO
  def lkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = {
    val hm = mapVars(e2)
    e1.asInstanceOf[List[_]].flatMap(v2 => { 
                val vs = tupleVars(v2)
                hm.get(p1(vs)) match {
                  case Some(v1) => v1.asInstanceOf[List[_]].withFilter(v3 => 
                    p3(vs).equals(p2(v3))).map(v3 => vs :+ v3)
                  case _ => Nil
                }})
  }
  def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = {
    val hm = mapVars(e2)
    e1.asInstanceOf[List[_]].flatMap(v2 => { 
                val vs = tupleVars(v2)
                hm.get(p1(vs)) match {
                  case Some(v1) => v1.asInstanceOf[List[_]].map(v3 => {
                    if (p3(vs).equals(p2(v3))) { vs :+ v3 } else { vs :+ None }
                  })
                  case _ => Nil  
                }})
  }
  def cogroup(e1: Rep, es: List[Rep], ps: List[Rep] => Rep): Rep = e1
  
  // keys and flattens input tuples
  def tupleVars(k: Any): List[Rep] = k match {
    case c:CaseClassRecord => List(k).asInstanceOf[List[Rep]]
    case c:Rec => List(k).asInstanceOf[List[Rep]]
    case _ => k.asInstanceOf[List[Rep]]
  }
  def mapVars(k: Any): Map[Any, Any] = k match {
    case l:List[_] if l.head.isInstanceOf[RecordValue] =>
      l.asInstanceOf[List[RecordValue]].map{ case rv => (rv.map("k"), rv.map("v")) }.toMap
    case _ => k.asInstanceOf[List[(Any, Any)]].toMap
  }

}

/**
  * ANF compiler for generating scala code,
  * uses common subexpression elimination (CSE) 
  */
trait BaseANF extends Base {

  val compiler = new BaseCompiler {}

  case class Def(e: CExpr) 

  type Rep = Def

  implicit def defToExpr(d: Def): CExpr = {
    d.e
  }
  
  //implicit def lstToDef(e: List[CExpr]): List[Def] = e.map(Def(_))

  implicit def funcDefToExpr(fd: Def => Def): CExpr => CExpr = {
    (x: CExpr) => reifyBlock { fd(Def(x)) }
  }

  implicit def funcDefsToExpr(fd: List[Def] => Def): List[CExpr] => CExpr = {
    (x: List[CExpr]) => reifyBlock { fd(x.map(x2 => Def(x2))) }
  }

  implicit def exprToDef(e: CExpr): Def = {
    state.get(e) match {
      case Some(v) => // CSE!
        Def(v)
      case None =>
        e match {
          case Constant(_) | InputRef(_, _) => Def(e)
	  case _ => 
            val v = Variable.fresh(e.tp)
            vars = vars :+ v
            state = state + (e -> v)
            stateInv = stateInv + (v -> e)
            Def(v)
        }
    }
  }

  var state: Map[CExpr, Variable] = Map()
  var stateInv: Map[Variable, CExpr] = Map()
  var vars: Seq[Variable] = Seq()
  var varMaps: Map[String, Variable] = Map()

  def reset = {
    state = Map()
    stateInv = Map()
    vars = Seq()
    varMaps = Map()
  }

  // TODO: CSE doesn't go beyond a scope.
  def reifyBlock(b: => Rep): CExpr = {
    val oldState = state
    val oldStateInv = stateInv
    val oldVars = vars
    state = Map()
    stateInv = Map()
    vars = Seq()
    val e = b.e
    val res = vars.foldRight(e)((cur, acc) => Bind(cur, stateInv(cur), acc))
    state = oldState
    stateInv = oldStateInv
    vars = oldVars
    res
  }

  def anf(d: Rep): CExpr = 
    vars.foldRight(d.e)((cur, acc) => Bind(cur, stateInv(cur), acc))

  def inputref(x: String, tp:Type): Rep = compiler.inputref(x, tp)
  def input(x: List[Rep]): Rep = ??? 
  def constant(x: Any): Rep = compiler.constant(x)
  def emptysng: Rep = compiler.emptysng
  def unit: Rep = compiler.unit
  def sng(x: Rep): Rep = compiler.sng(x)
  def weightedsng(x: Rep, q: Rep): Rep = { // weighted singleton is actually a context
    // (x: Rep => Rep, q: Rep => Rep)
    // where the variables get passed through the context
    val t = compiler.weightedsng(x, q)
    t
  }
  def tuple(fs: List[Rep]): Rep = compiler.tuple(fs.map(defToExpr(_)))
  def record(fs: Map[String, Rep]): Rep = compiler.record(fs.map(x => (x._1, defToExpr(x._2))))
  def mult(e1: Rep, e2: Rep): Rep = compiler.mult(e1, e2)
  def equals(e1: Rep, e2: Rep): Rep = compiler.equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = compiler.lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = compiler.gt(e1, e2)
  def lte(e1: Rep, e2: Rep): Rep = compiler.lte(e1, e2)
  def gte(e1: Rep, e2: Rep): Rep = compiler.gte(e1, e2)
  def and(e1: Rep, e2: Rep): Rep = compiler.and(e1, e2)
  def not(e1: Rep): Rep = compiler.not(e1)
  def or(e1: Rep, e2: Rep): Rep = compiler.or(e1, e2)
  def project(e1: Rep, field: String): Rep = {
    compiler.project(e1, field)
  }
  def ifthen(cond: Rep, e1: Rep, e2: Option[Rep] = None): Rep = e2 match {
    case Some(a) => compiler.ifthen(cond, e1, Some(a)) 
    case _ => compiler.ifthen(cond, e1, None)
  }
  def merge(e1: Rep, e2: Rep): Rep = compiler.merge(e1, e2)
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = compiler.comprehension(e1, p, e)
  def dedup(e1: Rep): Rep = compiler.dedup(e1)
  def bind(e1: Rep, e: Rep => Rep): Rep = compiler.bind(e1, e)
  def groupby(e1: Rep, g: Rep => Rep, v: Rep => Rep): Rep = compiler.groupby(e1, g, v)
  def named(n: String, e: Rep): Rep = {
    val d = compiler.named(n, e)
    varMaps = varMaps + (n -> d.e.asInstanceOf[Variable])
    d
  }
  def linset(e: List[Rep]): Rep = compiler.linset(e.map(defToExpr(_)))
  def lookup(lbl: Rep, dict: Rep): Rep = compiler.lookup(lbl, dict)
  def emptydict: Rep = compiler.emptydict
  def bagdict(lbl: Rep, flat: Rep, dict: Rep): Rep = compiler.bagdict(lbl, flat, dict)
  def tupledict(fs: Map[String, Rep]): Rep = compiler.tupledict(fs.map(f => (f._1, defToExpr(f._2))))
  def dictunion(d1: Rep, d2: Rep): Rep = compiler.dictunion(d1, d2)
  def select(x: Rep, p: Rep => Rep, e: Rep => Rep): Rep = compiler.select(x, p, e)
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = compiler.reduce(e1, f, p)
  def unnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = compiler.unnest(e1, f, p)
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = compiler.join(e1, e2, p1, p2)
  def outerunnest(e1: Rep, r: List[Rep] => Rep, p: List[Rep] => Rep): Rep = compiler.outerunnest(e1, r, p)
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p: Rep => Rep): Rep = compiler.outerjoin(e1, e2, p1, p)
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep): Rep = compiler.nest(e1, f, e, p, g)
  def lkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = compiler.lkup(e1, e2, p1, p2, p3)
  def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = compiler.outerlkup(e1, e2, p1, p2, p3)
  def cogroup(e1: Rep, es: List[Rep], ps: List[Rep] => Rep): Rep = compiler.cogroup(e1, es.map(defToExpr(_)), ps)
  
  def vars(e: Type): List[Variable] = e match {
    case BagCType(tp:RecordCType) => List(Variable.fresh(tp))
    case BagCType(tp @ TTupleType(tps)) => tps.map(Variable.fresh(_))
    case _ => ???
  }
}

class Finalizer(val target: Base){
  var variableMap: Map[CExpr, target.Rep] = Map[CExpr, target.Rep]()
  def withMap[T](m: (CExpr, target.Rep))(f: => T): T = {
    val old = variableMap
    variableMap = variableMap + m
    val res = f
    variableMap = old
    res
  }
  def withMapList[T](m: List[(CExpr, target.Rep)])(f: => T): T = {
    val old = variableMap
    variableMap = variableMap ++ m
    val res = f
    variableMap = old
    res
  }
  def finalize(e: CExpr): target.Rep = e match {
    case InputRef(x, tp) => target.inputref(x, tp)
    case Input(x) => target.input(x.map(finalize(_)))
    case Constant(x) => target.constant(x)
    case EmptySng => target.emptysng
    case CUnit => target.unit
    case Sng(x) => x.tp match {
      case EmptyCType => target.emptysng
      case _ => target.sng(finalize(x))
    }
    case WeightedSng(x, q) => target.weightedsng(finalize(x), finalize(q))
    case Tuple(fs) if fs.isEmpty => target.unit
    case Tuple(fs) => target.tuple(fs.map(f => finalize(f)))
    case Record(fs) if fs.isEmpty => target.unit
    case Record(fs) => target.record(fs.map(f => f._1 -> finalize(f._2)))
    case Multiply(e1, e2) => target.mult(finalize(e1), finalize(e2))
    case Equals(e1, e2) => target.equals(finalize(e1), finalize(e2))
    case Lt(e1, e2) => target.lt(finalize(e1), finalize(e2))
    case Gt(e1, e2) => target.gt(finalize(e1), finalize(e2))
    case Lte(e1, e2) => target.lte(finalize(e1), finalize(e2))
    case Gte(e1, e2) => target.gte(finalize(e1), finalize(e2))
    case And(e1, e2) => target.and(finalize(e1), finalize(e2))
    case Not(e1) => target.not(finalize(e1))
    case Or(e1, e2) => target.or(finalize(e1), finalize(e2))
    case If(cond, e1, e2) => e2 match {
      case Some(a) => target.ifthen(finalize(cond), finalize(e1), Option(finalize(a)))
      case _ => target.ifthen(finalize(cond), finalize(e1))
    }
    case Merge(e1, e2) => target.merge(finalize(e1), finalize(e2))
    case Project(e1, pos) => target.project(finalize(e1), pos)
    case Comprehension(e1, v, p, e) =>
      target.comprehension(finalize(e1), (r: target.Rep) => withMap(v -> r)(finalize(p)), 
        (r: target.Rep) => withMap(v -> r)(finalize(e)))
    case CDeDup(e1) => target.dedup(finalize(e1))
    case Bind(x, e1, e) =>
      target.bind(finalize(e1), (r: target.Rep) => withMap(x -> r)(finalize(e)))
    case CGroupBy(e1, v2, g, v) => 
      target.groupby(finalize(e1), (r: target.Rep) => withMap(v2 -> r)(finalize(g)), 
        (r: target.Rep) => withMap(v2 -> r)(finalize(v)))
    case CNamed(n, e) => target.named(n, finalize(e))
    case LinearCSet(exprs) => 
      target.linset(exprs.map(finalize(_)))
    case CLookup(l, d) => 
      target.lookup(finalize(l), finalize(d))
    case EmptyCDict => target.emptydict
    case BagCDict(l, f, d) => 
      target.bagdict(finalize(l), finalize(f), finalize(d))
    case TupleCDict(fs) => target.tupledict(fs.map(f => f._1 -> finalize(f._2)))
    case DictCUnion(d1, d2) => target.dictunion(finalize(d1), finalize(d2))
    case Select(x, v, p, e) =>
      target.select(finalize(x), (r: target.Rep) => 
        withMap(v -> r)(finalize(p)), (r: target.Rep) => withMap(v -> r)(finalize(e)))
    case Reduce(e1, v, e2, p) =>
      target.reduce(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: List[target.Rep]) => withMapList(v zip r)(finalize(p)))
    case Unnest(e1, v, e2, v2, p) => 
      target.unnest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p)))
    case Nest(e1, v, f, e, v2, p, g) =>
      target.nest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(f)),
        (r: List[target.Rep]) => withMapList(v zip r)(finalize(e)), 
          (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p)), (r: List[target.Rep]) => withMapList(v zip r)(finalize(g)))
    case Join(e1, e2, v, p1, v2, p2) =>
      target.join(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)))
    case OuterUnnest(e1, v, e2, v2, p) => 
      target.outerunnest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p)))
    case OuterJoin(e1, e2, v, p1, v2, p2) =>
      target.outerjoin(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)))
    case Lookup(e1, e2, v, p1, v2, p2, p3) =>
      target.lkup(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)), (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p3)))
    case OuterLookup(e1, e2, v, p1, v2, p2, p3) =>
      target.outerlkup(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)), (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p3)))
    case CoGroup(e1, es, vs, ps) => 
      target.cogroup(finalize(e1), es.map(finalize(_)), (r: List[target.Rep]) => withMapList(vs zip r)(finalize(ps)))
    case v @ Variable(_, _) => variableMap.getOrElse(v, target.inputref(v.name, v.tp))
  }

}

