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
  def named(n: String, e: Rep): Rep
  def linset(e: List[Rep]): Rep
  def lookup(lbl: Rep, dict: Rep): Rep
  def emptydict: Rep
  def bagdict(lbl: Rep, flat: Rep, dict: Rep): Rep
  def tupledict(fs: Map[String, Rep]): Rep
  def dictunion(d1: Rep, d2: Rep): Rep
  def select(x: Rep, p: Rep => Rep): Rep
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep
  def unnest(e1: Rep, f: List[Rep] => Rep, p: Rep => Rep): Rep
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep
  def outerunnest(e1: Rep, r: List[Rep] => Rep, p: Rep => Rep): Rep
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p: Rep => Rep): Rep
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: Rep => Rep): Rep
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
  def select(x: Rep, p: Rep => Rep): Rep = { 
    s""" | SELECT[ ${p(Variable.fresh(StringType).quote)} ](${x} )""".stripMargin
  }
  def reduce(x: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = { 
    val v = Variable.fresh(StringType)
    s""" | REDUCE[ ${f(List(v.quote))} / ${p(List(v.quote))} ](${x})""".stripMargin
  }
  def unnest(x: Rep, f: List[Rep] => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(StringType)
    s""" | UNNEST[ ${f(List(v1.quote))} / ${p(v.quote)} ](${x})""".stripMargin
  }
  def nest(x: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(StringType) 
    val v2 = Variable.fresh(StringType)
    val acc = e(List(v1.quote)) match { case "1" => "+"; case _ => "U" }
    s""" | NEST[ ${acc} / ${e(List(v1.quote))} / ${f(List(v2.quote))}, ${p(v2.quote)} ](${x})""".stripMargin
  }
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) JOIN[${p1(List(v1.quote))} = ${p2(v2.quote)}]( 
         | ${ind(e2)})""".stripMargin
  }
  def outerunnest(x: Rep, f: List[Rep] => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(StringType)
    s""" | OUTERUNNEST[ ${f(List(v1.quote))} / ${p(v.quote)} ](${x})""".stripMargin
  }
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) OUTERJOIN[${p1(List(v1.quote))} = ${p2(v2.quote)}]( 
         | ${ind(e2)})""".stripMargin
  }

}

/**
  * Generates expression nodes defined in Expr.scala
  */

trait BaseCompiler extends Base {
  type Rep = CExpr 
  def inputref(x: String, tp: Type): Rep = InputRef(x, tp)
  def input(x: List[Rep]): Rep = Input(x)
  def constant(x: Any): Rep = Constant(x)
  def emptysng: Rep = EmptySng
  def unit: Rep = CUnit
  def sng(x: Rep): Rep = Sng(x)
  def weightedsng(x: Rep, q: Rep): Rep = WeightedSng(x, q)
  def tuple(fs: List[Rep]): Rep = Tuple(fs)
  def record(fs: Map[String, Rep]): Rep = Record(fs)
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
  def dedup(e1: Rep): Rep = CDeDup(e1)
  def bind(e1: Rep, e: Rep => Rep): Rep = {
      val v = Variable.fresh(e1.tp)
      Bind(v, e1, e(v)) 
  }
  def named(n: String, e: Rep): Rep = CNamed(n, e)
  def linset(e: List[Rep]): Rep = LinearCSet(e)
  def lookup(lbl: Rep, dict: Rep): Rep = CLookup(lbl, dict)
  def emptydict: Rep = EmptyCDict
  def bagdict(lbl: Rep, flat: Rep, dict: Rep): Rep = BagCDict(lbl, flat, dict)
  def tupledict(fs: Map[String, Rep]): Rep = TupleCDict(fs)
  def dictunion(d1: Rep, d2: Rep): Rep = DictCUnion(d1, d2)
  def select(x: Rep, p: Rep => Rep): Rep = {
    val v = Variable.fresh(x.tp.asInstanceOf[BagCType].tp)
    p(v) match {
      case Constant(true) => x
      case _ => Select(x, v, p(v))
    }
  }
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    val v = e1.wvars
    Reduce(e1, v, f(v), p(v))
  }
  def unnest(e1: Rep, f: List[Rep] => Rep, p: Rep => Rep): Rep = {
    val v1 = e1 match {
      case InputRef(_,tp) => List(Variable.fresh(tp.asInstanceOf[BagCType].tp))
      case _ => e1.wvars
    }
    val fv = f(v1) 
    val v = Variable.fresh(fv.tp.asInstanceOf[BagCType].tp)
    Unnest(e1, v1, fv, v, p(v))
  }
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: Rep => Rep): Rep = {
    val v1 = e1.asInstanceOf[CExpr].wvars 
    val fv = f(v1) // groups
    val ev = e(v1) // pattern
    val v = ev.tp match {
      case IntType => 
        Variable.fresh(TTupleType(fv.tp.asInstanceOf[TTupleType].attrTps :+ ev.tp))
      case _ => 
        Variable.fresh(TTupleType(fv.tp.asInstanceOf[TTupleType].attrTps :+ BagCType(ev.tp)))
    }
    Nest(e1, v1, fv, ev, v, p(v))
  }
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val v1 = e1 match {
      case InputRef(_,tp) => List(Variable.fresh(e1.tp.asInstanceOf[BagCType].tp))
      case _ => e1.wvars
    }
    val v2 = Variable.fresh(e2.tp.asInstanceOf[BagCType].tp)
    Join(e1, e2, v1, p1(v1), v2, p2(v2))
  }
  def outerunnest(e1: Rep, f: List[Rep] => Rep, p: Rep => Rep): Rep = {
    val v1 = e1 match {
      case InputRef(_,tp) => List(Variable.fresh(tp.asInstanceOf[BagCType].tp))
      case _ => e1.wvars
    }
    val fv = f(v1) 
    val v = Variable.fresh(fv.tp.asInstanceOf[BagCType].tp)
    OuterUnnest(e1, v1, fv, v, p(v))
  }
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    val v1 = e1 match {
      case InputRef(_,tp) => List(Variable.fresh(e1.tp.asInstanceOf[BagCType].tp))
      case _ => e1.wvars
    }
    val v2 = Variable.fresh(e2.tp.asInstanceOf[BagCType].tp)
    OuterJoin(e1, e2, v1, p1(v1), v2, p2(v2))
  }
}

/**
  * Scala evaluation 
  */
trait BaseScalaInterp extends Base{
  type Rep = Any
  var ctx = scala.collection.mutable.Map[String, Any]()
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
  def record(fs: Map[String, Rep]): Rep = RecordValue(fs.asInstanceOf[Map[String, Rep]])
  def equals(e1: Rep, e2: Rep): Rep = e1 == e2
  def lt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] < e2.asInstanceOf[Int]
  def gt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] > e2.asInstanceOf[Int]
  def lte(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] <= e2.asInstanceOf[Int]
  def gte(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] >= e2.asInstanceOf[Int]
  def and(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Boolean] && e2.asInstanceOf[Boolean]
  def not(e1: Rep): Rep = !e1.asInstanceOf[Boolean]
  def or(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Boolean] || e2.asInstanceOf[Boolean]
  def project(e1: Rep, f: String) = f match {
    case "_1" => e1.asInstanceOf[Product].productElement(0)
    case "_2" => e1.asInstanceOf[Product].productElement(1)
    case f => e1 match {
      case m:RecordValue => m.map(f)
      case c:CaseClassRecord => 
        val field = c.getClass.getDeclaredFields.find(_.getName == f).get
        field.setAccessible(true)
        field.get(c)
      //case m:HashMap[Any,_] => m(f.asInstanceof[Any])
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
      case Nil => e(Nil) match { case i:Int => 0; case _ => Nil }
      case data @ (head :: tail) => e(head) match {
        case i:Int =>
          data.withFilter(p.asInstanceOf[Rep => Boolean]).map(e).asInstanceOf[List[Int]].sum
        case _ => 
          data.withFilter(p.asInstanceOf[Rep => Boolean]).
            flatMap(e.asInstanceOf[Rep => scala.collection.GenTraversableOnce[Rep]])
      }
    }
  }
  def dedup(e1: Rep): Rep = e1.asInstanceOf[List[_]].distinct
  def named(n: String, e: Rep): Rep = {
    ctx(n) = e
    println(n+" := "+e+"\n")
    e
  }
  def linset(e: List[Rep]): Rep = e
  def bind(e1: Rep, e: Rep => Rep): Rep = ctx.getOrElseUpdate(e1.asInstanceOf[String], e(e1))
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
  def select(x: Rep, p: Rep => Rep): Rep = { 
    x.asInstanceOf[List[_]].filter(p.asInstanceOf[Rep => Boolean])
  }
  // TODO fix filter
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    println("reduce")
    val res = e1.asInstanceOf[List[List[_]]].map(f)//.filter(p.asInstanceOf[Rep => Boolean])
    res.foreach(println(_))
    res
  }
  def unnest(e1: Rep, f: List[Rep] => Rep, p: Rep => Rep): Rep = {
    val d = e1.asInstanceOf[List[List[_]]].flatMap{
      v => f(v).asInstanceOf[List[_]].map{ v2 => v :+ v2 }
    }.filter{p.asInstanceOf[Rep => Boolean]}
    d.foreach(println(_))
    d
  }
  // e1.map(p1).join(e2.map(p2))
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    println("joining")
    println(e1)
    println(e2)
    val d = e1.asInstanceOf[List[_]].flatMap(v1 =>
      e2.asInstanceOf[List[_]].filter{ v2 => p1(List(v1)) == p2(v2) }.map(v2 => List(v1, v2)))
    d.foreach(println(_))
    d
  }
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: Rep => Rep): Rep = {
    println("nest")
    val grpd = e1.asInstanceOf[List[List[_]]].map(v => (f(v), e(v))).groupBy(_._1)
    val result = e(e1.asInstanceOf[List[List[_]]].head) match {
      case i:Int => 
        grpd.map{ f2 => 
          f2._1.asInstanceOf[List[_]] :+ f2._2.map(_._2).asInstanceOf[List[Int]].sum 
        }.filter(p.asInstanceOf[Rep => Boolean])
      case _ => grpd.map(f2 => f2._1.asInstanceOf[List[_]] :+ f2._2 ).filter(p.asInstanceOf[Rep => Boolean])
    }
    result.foreach(println(_))
    result
  }
  def outerunnest(e1: Rep, f: List[Rep] => Rep, p: Rep => Rep): Rep = {
    println("outerunnest")
    val d =e1.asInstanceOf[List[List[_]]].flatMap{
      v => f(v).asInstanceOf[List[_]].map{ v2 => v :+ v2 }
    }.filter{p.asInstanceOf[Rep => Boolean]}
    d.foreach(println(_))
    d
  }
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = {
    println("outer-joining")
    println(e1)
    println(e2)
    val d = e1.asInstanceOf[List[_]].flatMap(v1 =>
      e2.asInstanceOf[List[_]].filter{ v2 => p1(List(v1)) == p2(v2) }.map(v2 => List(v1, v2)))
    d.foreach(println(_))
    d
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

  implicit def funcDefToExpr(fd: Def => Def): CExpr => CExpr = {
    (x: CExpr) => reifyBlock { fd(Def(x)) }
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

  def inputref(x: String, tp:Type): Rep = {
    val name = varMaps.get(x).map(_.name).getOrElse(x)
    compiler.inputref(name, tp)
  }
  def input(x: List[Rep]): Rep = ??? 
  def constant(x: Any): Rep = compiler.constant(x)
  def emptysng: Rep = compiler.emptysng
  def unit: Rep = compiler.unit
  def sng(x: Rep): Rep = compiler.sng(x)
  def weightedsng(x: Rep, q: Rep): Rep = compiler.weightedsng(x, q)
  def tuple(fs: List[Rep]): Rep = compiler.tuple(fs.map(defToExpr(_)))
  def record(fs: Map[String, Rep]): Rep = compiler.record(fs.map(x => (x._1, defToExpr(x._2))))
  def equals(e1: Rep, e2: Rep): Rep = compiler.equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = compiler.lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = compiler.gt(e1, e2)
  def lte(e1: Rep, e2: Rep): Rep = compiler.lte(e1, e2)
  def gte(e1: Rep, e2: Rep): Rep = compiler.gte(e1, e2)
  def and(e1: Rep, e2: Rep): Rep = compiler.and(e1, e2)
  def not(e1: Rep): Rep = compiler.not(e1)
  def or(e1: Rep, e2: Rep): Rep = compiler.or(e1, e2)
  def project(e1: Rep, field: String): Rep = compiler.project(e1, field)
  def ifthen(cond: Rep, e1: Rep, e2: Option[Rep] = None): Rep = e2 match {
    case Some(a) => compiler.ifthen(cond, e1, Some(a)) 
    case _ => compiler.ifthen(cond, e1, None)
  }
  def merge(e1: Rep, e2: Rep): Rep = compiler.merge(e1, e2)
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = compiler.comprehension(e1, p, e)
  def dedup(e1: Rep): Rep = compiler.dedup(e1)
  def bind(e1: Rep, e: Rep => Rep): Rep = compiler.bind(e1, e)
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
  def select(x: Rep, p: Rep => Rep): Rep = ???
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = ???
  def unnest(e1: Rep, f: List[Rep] => Rep, p: Rep => Rep): Rep = ???
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep): Rep = ???
  def outerunnest(e1: Rep, r: List[Rep] => Rep, p: Rep => Rep): Rep = ???
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p: Rep => Rep): Rep = ???
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: Rep => Rep): Rep = ???
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
    case Select(x, v, p) =>
      target.select(finalize(x), (r: target.Rep) => withMap(v -> r)(finalize(p)))
    case Reduce(e1, v, e2, p) => 
      target.reduce(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: List[target.Rep]) => withMapList(v zip r)(finalize(p)))
    case Unnest(e1, v, e2, v2, p) => 
      target.unnest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: target.Rep) => withMap(v2 -> r)(finalize(p)))
    case Nest(e1, v, f, e, v2, p) => 
      target.nest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(f)),
        (r: List[target.Rep]) => withMapList(v zip r)(finalize(e)), 
          (r: target.Rep) => withMap(v2 -> r)(finalize(p)))
    case Join(e1, e2, v, p1, v2, p2) =>
      target.join(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)))
    case OuterUnnest(e1, v, e2, v2, p) => 
      target.outerunnest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: target.Rep) => withMap(v2 -> r)(finalize(p)))
    case OuterJoin(e1, e2, v, p1, v2, p2) =>
      target.outerjoin(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)))
    case v @ Variable(_, _) => variableMap.getOrElse(v, target.inputref(v.name, v.tp) )
  }

}

