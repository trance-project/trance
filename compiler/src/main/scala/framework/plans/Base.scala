package framework.plans

import framework.common._
import scala.collection.mutable.HashMap
import framework.utils.Utils.ind

/**
  * Base structure for comprehension compilers.
  */
trait Base {
  type Rep
  def inputref(x: String, tp:Type): Rep
  def input(x: List[Rep]): Rep 
  def constant(x: Any): Rep
  def emptysng: Rep
  def unit: Rep 
  def sng(x: Rep): Rep
  def get(x: Rep): Rep
  def tuple(fs: List[Rep]): Rep
  def record(fs: Map[String, Rep]): Rep
  def label(fs: Map[String, Rep]): Rep 
  def mathop(op: OpArithmetic, e1: Rep, e2: Rep): Rep
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
  def groupby(e1: Rep, g: List[String], v: List[String]): Rep
  def reduceby(e1: Rep, g: List[String], v: List[String]): Rep
  def named(n: String, e: Rep): Rep
  def linset(e: List[Rep]): Rep
  def lookup(lbl: Rep, dict: Rep): Rep
  def emptydict: Rep
  def bagdict(lblTp: LabelType, flat: Rep, dict: Rep): Rep
  def tupledict(fs: Map[String, Rep]): Rep
  def dictunion(d1: Rep, d2: Rep): Rep
  def select(x: Rep, p: Rep => Rep, e: Rep => Rep): Rep
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep
  def unnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep, value: List[Rep] => Rep): Rep
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, proj1: List[Rep] => Rep, proj2: Rep => Rep): Rep
  def outerunnest(e1: Rep, r: List[Rep] => Rep, p: List[Rep] => Rep, value: List[Rep] => Rep): Rep
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p: Rep => Rep, proj1: List[Rep] => Rep, proj2: Rep => Rep): Rep
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep, dk: Boolean): Rep
  def lkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep
  def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep
  def cogroup(e1: Rep, e2: Rep, k1: List[Rep] => Rep, k2: Rep => Rep, value: List[Rep] => Rep): Rep
  def flatdict(e1: Rep): Rep
  def groupdict(e1: Rep): Rep

  def addindex(in: Rep, name: String): Rep 
  def dfproject(in: Rep, filter: Rep => Rep, fields: List[String]): Rep
  def dfunnest(in: Rep, path: String, filter: Rep => Rep, fields: List[String]): Rep
  def dfounnest(in: Rep, path: String, filter: Rep => Rep, fields: List[String]): Rep
  def dfjoin(left: Rep, right: Rep, cond: (Rep, Rep) => Rep, fields: List[String]): Rep
  def dfojoin(left: Rep, right: Rep, cond: (Rep, Rep) => Rep, fields: List[String]): Rep
  def dfnest(in: Rep, key: List[String], value: Rep => Rep, filter: Rep => Rep, nulls: List[String]): Rep
  def dfreduceby(in: Rep, key: List[String], values: List[String]): Rep
}

/** Base compiler for string types.
  * This works bottom-up and is not often used. 
  * The primary printer, which works top-down, is in Printer.scala
  */
trait BaseStringify extends Base{
  type Rep = String
  def inputref(x: String, tp: Type): Rep = x
  def input(x: List[Rep]): Rep = s"{${x.mkString(",")}}"
  def constant(x: Any): Rep = x.toString
  def emptysng: Rep = "{}"
  def unit: Rep = "()"
  def sng(x: Rep): Rep = s"{ $x }"
  def get(x: Rep): Rep = s"get($x)"
  def tuple(fs: List[Rep]) = s"(${fs.mkString(",")})"
  def record(fs: Map[String, Rep]): Rep = 
    s"(${fs.map(f => f._1 + " := " + f._2).mkString(",")})"
  def label(fs: Map[String, Rep]): Rep = 
    s"Label(${fs.map(f => f._1 + " := " + f._2).mkString(",")})"
  def mathop(op: OpArithmetic, e1: Rep, e2: Rep): Rep = s"$e1 $op $e2"
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
  def groupby(e1: Rep, g: List[String], v: List[String]): Rep = {
    val v2 = Variable.fresh(StringType)
    s"(${e1}).groupBy(${g.mkString(",")}, ${v.mkString(",")})"
  }
  def reduceby(e1: Rep, g: List[String], v: List[String]): Rep = {
    val v2 = Variable.fresh(StringType)
    s"(${e1}).reduceBy(${g.mkString(",")}, ${v.mkString(",")})"
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
  def bagdict(lblTp: LabelType, flat: Rep, dict: Rep): Rep = s"(${lblTp} -> ${flat}, ${dict})"
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
  def unnest(x: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep, value: List[Rep] => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(StringType)
    s""" | UNNEST[ ${f(List(v1.quote))} / ${p(List(v.quote))} / ${value(List(v.quote))} ](${x})""".stripMargin
  }
  def nest(x: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep, dk: Boolean): Rep = {
    val v1 = Variable.fresh(StringType) 
    val v2 = Variable.fresh(StringType)
    val acc = e(List(v1.quote)) match { case "1" => "+"; case _ => "U" }
    s""" | NEST[ ${acc} / ${e(List(v1.quote))} / ${f(List(v2.quote))}, ${p(List(v2.quote))} ](${x})""".stripMargin
  }
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, proj1: List[Rep] => Rep, proj2: Rep => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) JOIN[${p1(List(v1.quote))} = ${p2(v2.quote)}]( 
         | ${ind(e2)})""".stripMargin
  }
  def outerunnest(x: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep, value: List[Rep] => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(StringType)
    s""" | OUTERUNNEST[ ${f(List(v1.quote))} / ${p(List(v.quote))} / ${value(List(v.quote))} ](${x})""".stripMargin
  }
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, proj1: List[Rep] => Rep, proj2: Rep => Rep): Rep = {
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
  def cogroup(e1: Rep, e2: Rep, k1: List[Rep] => Rep, k2: Rep => Rep, value: List[Rep] => Rep): Rep = {
    s"""| ($e1) COGROUP[${k1(List(e1))} = ${k2(e2)} / ${value(List(e1, e2))} ] 
        | ${ind(e2)})""".stripMargin
  }
  def flatdict(e1: Rep): Rep = s"FLAT($e1)"
  def groupdict(e1: Rep): Rep = s"GROUP($e1)"

  def addindex(in: Rep, name: String): Rep = ""
  def dfproject(in: Rep, filter: Rep => Rep, fields: List[String]): Rep = ""
  def dfunnest(in: Rep, path: String, filter: Rep => Rep, fields: List[String]): Rep = ""
  def dfounnest(in: Rep, path: String, filter: Rep => Rep, fields: List[String]): Rep = ""
  def dfjoin(left: Rep, right: Rep, cond: (Rep, Rep) => Rep, fields: List[String]): Rep = ""
  def dfojoin(left: Rep, right: Rep, cond: (Rep, Rep) => Rep, fields: List[String]): Rep = ""
  def dfnest(in: Rep, key: List[String], value: Rep => Rep, filter: Rep => Rep, nulls: List[String]): Rep = ""
  def dfreduceby(in: Rep, key: List[String], values: List[String]): Rep = ""
}

/** Base compiler for generating nodes defined in the 
  * comprehension representation (Expr.scala)
  */
trait BaseCompiler extends Base {

  val ext = new Extensions{}
  type Rep = CExpr 

  def inputref(x: String, tp: Type): Rep = InputRef(x, tp)
  def input(x: List[Rep]): Rep = Input(x)
  def constant(x: Any): Rep = Constant(x)
  def emptysng: Rep = EmptySng
  def unit: Rep = CUnit
  def sng(x: Rep): Rep = Sng(x)
  def get(x: Rep): Rep = CGet(x)
  def tuple(fs: List[Rep]): Rep = Tuple(fs)
  def record(fs: Map[String, Rep]): Rep = Record(fs)
  def label(fs: Map[String, Rep]): Rep = Label(fs)
  def mathop(op: OpArithmetic, e1: Rep, e2: Rep): Rep = MathOp(op, e1, e2)
  def equals(e1: Rep, e2: Rep): Rep = Equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = Lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = Gt(e1, e2)
  def lte(e1: Rep, e2: Rep): Rep = Lte(e1, e2)
  def gte(e1: Rep, e2: Rep): Rep = Gte(e1, e2)
  def and(e1: Rep, e2: Rep): Rep = And(e1, e2)
  def not(e1: Rep): Rep = Not(e1)
  def or(e1: Rep, e2: Rep): Rep = Or(e1, e2)
  def project(e1: Rep, f: String): Rep = e1 match {
    case InputRef(n, _) if n.contains("Dict") => e1
    case _ => Project(e1, f)
  }
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
  def groupby(e1: Rep, g: List[String], v: List[String]): Rep = {
    val v2 = Variable.freshFromBag(e1.tp)
    CGroupBy(e1, v2, g, v) 
  }
  def reduceby(e1: Rep, g: List[String], v: List[String]): Rep = {
    val v2 = Variable.freshFromBag(e1.tp)
    CReduceBy(e1, v2, g, v) 
  }
  def named(n: String, e: Rep): Rep = CNamed(n, e)
  def linset(e: List[Rep]): Rep = LinearCSet(e)
  def lookup(lbl: Rep, dict: Rep): Rep = CLookup(lbl, dict)
  def emptydict: Rep = EmptyCDict
  def bagdict(lblTp: LabelType, flat: Rep, dict: Rep): Rep = BagCDict(lblTp, flat, dict)
  def tupledict(fs: Map[String, Rep]): Rep = TupleCDict(fs)
  def dictunion(d1: Rep, d2: Rep): Rep = DictCUnion(d1, d2)
  def select(x: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    val v = Variable.freshFromBag(x.tp)
    Select(x, v, p(v), e(v))
  }
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = {
    val v = vars(e1.tp)
    Reduce(e1, v, f(v), p(v))
  }
  def unnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep, value: List[Rep] => Rep): Rep = {
    val v1 = vars(e1.tp)
    val fv = f(v1) 
    val v = Variable.freshFromBag(fv.tp)
    Unnest(e1, v1, fv, v, p(v1 :+ v), value(v1 :+ v))
  }
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep, dk: Boolean): Rep = {
    val v1 = vars(e1.tp)
    val fv = f(v1) // groups
    val ev = e(v1)
    val v = fv.tp match {
      case TTupleType(fs) if !dk => Variable.fresh(fv.tp, BagCType(ev.tp))
      case _ => Variable.fresh(fv.tp, ev.tp)
    }
    Nest(e1, v1, fv, ev, v, p(v1:+v), g(v1), dk)
  }
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, proj1: List[Rep] => Rep, proj2: Rep => Rep): Rep = {
    val v1 = vars(e1.tp)
    val v2 = Variable.freshFromBag(e2.tp)
    Join(e1, e2, v1, p1(v1), v2, p2(v2), proj1(v1), proj2(v2))
  }
  def outerunnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep, value: List[Rep] => Rep): Rep = {
    val v1 = vars(e1.tp)
    val fv = f(v1) 
    val v = Variable.freshFromBag(fv.tp)
    OuterUnnest(e1, v1, fv, v, p(v1 :+ v), value(v1 :+ v))
  }
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, proj1: List[Rep] => Rep, proj2: Rep => Rep): Rep = {
    val v1 = vars(e1.tp)
    val v2 = Variable.freshFromBag(e2.tp)
    OuterJoin(e1, e2, v1, p1(v1), v2, p2(v2), proj1(v1), proj2(v2))
  }
  def lkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = {
    val v1 = vars(e1.tp)
    val v2 = e2.tp match {
      case MatDictCType(lbl, bag) => Variable.freshFromBag(bag)//Variable.fresh(bag)
      case _ => Variable.freshFromBag(e2.tp) 
    } 
    Lookup(e1, e2, v1, p1(v1), v2, p2(v2), p3(v1 :+ v2))
  }
  def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = {
    val v1 = vars(e1.tp)
    val v2 = Variable.freshFromBag(e2.tp) 
    OuterLookup(e1, e2, v1, p1(v1), v2, p2(v2), p3(v1 :+ v2))
  }
  def cogroup(e1: Rep, e2: Rep, k1: List[Rep] => Rep, k2: Rep => Rep, value: List[Rep] => Rep): Rep = {
    val vs = vars(e1.tp)
    val v2 = Variable.freshFromBag(e2.tp)
    CoGroup(e1, e2, vs, v2, k1(vs), k2(v2), value(vs :+ v2))
  }
  def flatdict(e1: Rep): Rep = FlatDict(e1)
  def groupdict(e1: Rep): Rep = GroupDict(e1)

  def addindex(in: Rep, name: String): Rep = in match {
    case AddIndex(in1, _) => AddIndex(in, name)
    case _ => AddIndex(in, name)  
  }
  def dfproject(in: Rep, filter: Rep => Rep, fields: List[String]): Rep = {
    val v1 = Variable.freshFromBag(in.tp)
    val nr = filter(v1)
    val fs = ext.collect(nr)
    DFProject(in, v1, nr, fs.toList)
  }
  def dfunnest(in: Rep, path: String, filter: Rep => Rep, fields: List[String]): Rep = {
    val v = Variable.freshFromBag(in.tp)
    val v2 = Variable.freshFromBag(v.tp.asInstanceOf[RecordCType].attrTps(path))
    DFUnnest(in, v, path, v2, filter(v2), fields)
  }
  def dfounnest(in: Rep, path: String, filter: Rep => Rep, fields: List[String]): Rep = {
    val v = Variable.freshFromBag(in.tp)
    val v2 = Variable.fresh(v.tp.asInstanceOf[RecordCType].attrTps(path).outer)
    DFOuterUnnest(in, v, path, v2, filter(v2), fields)  
  }
  def dfjoin(left: Rep, right: Rep, cond: (Rep, Rep) => Rep, fields: List[String]): Rep = {
    val v = Variable.freshFromBag(left.tp)
    val v2 = Variable.freshFromBag(right.tp)
    DFJoin(left, v, right, v2, cond(v, v2), fields)
  }
  def dfojoin(left: Rep, right: Rep, cond: (Rep, Rep) => Rep, fields: List[String]): Rep = {
    val v = Variable.freshFromBag(left.tp)
    val v2 = Variable.freshFromBag(right.tp)
    DFOuterJoin(left, v, right, v2, cond(v, v2), fields)
  }
  def dfnest(in: Rep, key: List[String], value: Rep => Rep, filter: Rep => Rep, nulls: List[String]): Rep = {
    val v = Variable.freshFromBag(in.tp)
    val nr = value(v)
    DFNest(in, v, key, nr, filter(v), nr.inputColumns.toList)
  }
  def dfreduceby(in: Rep, key: List[String], values: List[String]): Rep = {
    val v = Variable.freshFromBag(in.tp)
    DFReduceBy(in, v, key, values)
  }

  /** Generates a list of variables referencing the stream from the downstream operators
    * This was important for compiling plans from tuple stream style plans produced.
    *
    * @param e type of the subplan
    * @param top optional boolean flag true if we are at top level
    * @return a set of variables representative of the tuple stream
    */
  def vars(e: Type, top: Boolean = true): List[Variable] = e match {
    case BagCType(tup) if top => vars(tup, false)
    case BagDictCType(BagCType(tup), _) if top => List(Variable.fresh(tup))
    case RecordCType(ms) if ms.keySet == Set("_1", "_2") => ms.values.toList.flatMap(f => vars(f, false))
    case TTupleType(List(EmptyCType, BagCType(tup))) => List(Variable.fresh(tup))
    case TTupleType(tps) if tps.head.isInstanceOf[LabelType] =>
      List(Variable.fresh(e))
    case TTupleType(tps) => tps.flatMap(vars(_, false)) 
    case _ => List(Variable.fresh(e))
  }

}

trait ShredOptimizer extends BaseCompiler {
  override def addindex(in: Rep, name: String): Rep = in
}

/**
  * Simplistic version of ANF compiler that only 
  * works for plan operators. This is more compatible 
  * with a relational representation (ie. Datasets)
  */
trait BaseDFANF extends BaseANF {

  override val compiler = new BaseCompiler {}

  override implicit def exprToDef(e: CExpr): Def = {
    state.get(e) match {
      case Some(v) => Def(v)
      case None =>
        // arbitrary CSE call
        def updateState(c: CExpr): Def = {
          val v = Variable.fresh(e.tp)
          vars = vars :+ v
          state = state + (e -> v)
          stateInv = stateInv + (v -> e)
          Def(v)
        }
        e match {
          case r:Reduce => updateState(r)
          case s:Select => updateState(s)
          case j:Join => updateState(j)
          case o:OuterJoin => updateState(o)
          case l:Lookup => updateState(l)
          case n:CNamed => updateState(n)
          case c:CoGroup => updateState(c)
          case u:Unnest => updateState(u)
          case ou:OuterUnnest => updateState(ou)
          case n:Nest => updateState(n)
          case cr:CReduceBy => updateState(cr)
          case dfp:DFProject => updateState(dfp)
          case dfu:DFUnnest => updateState(dfu)
          case dfuo:DFOuterUnnest => updateState(dfuo)
          case dfj:DFJoin => updateState(dfj)
          case dfjo:DFOuterJoin => updateState(dfjo)
          case dfn:DFNest => updateState(dfn)
          case dfr:DFReduceBy => updateState(dfr)
          case dfi:AddIndex => updateState(dfi)
          case _ => Def(e)
      }
    }
  }

}

/**
  * ANF compiler for generating scala and Spark RDD code,
  * uses common subexpression elimination (CSE).
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

  implicit def funcTupDefToExpr(fd: (Def, Def) => Def): (CExpr, CExpr) => CExpr = {
    (x: CExpr, y: CExpr) => reifyBlock { fd(Def(x), Def(y)) }
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
  def get(x: Rep): Rep = compiler.get(x)
  def tuple(fs: List[Rep]): Rep = compiler.tuple(fs.map(defToExpr(_)))
  def record(fs: Map[String, Rep]): Rep = compiler.record(fs.map(x => (x._1, defToExpr(x._2))))
  def label(fs: Map[String, Rep]): Rep = compiler.label(fs.map(x => (x._1, defToExpr(x._2))))
  def mathop(op: OpArithmetic, e1: Rep, e2: Rep) = compiler.mathop(op, e1, e2)
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
  def groupby(e1: Rep, g: List[String], v: List[String]): Rep = compiler.groupby(e1, g, v)
  def reduceby(e1: Rep, g: List[String], v: List[String]): Rep = compiler.reduceby(e1, g, v)
  def named(n: String, e: Rep): Rep = {
    val d = compiler.named(n, e)
    varMaps = varMaps + (n -> d.e.asInstanceOf[Variable])
    d
  }
  def linset(e: List[Rep]): Rep = compiler.linset(e.map(defToExpr(_)))
  def lookup(lbl: Rep, dict: Rep): Rep = compiler.lookup(lbl, dict)
  def emptydict: Rep = compiler.emptydict
  def bagdict(lblTp: LabelType, flat: Rep, dict: Rep): Rep = compiler.bagdict(lblTp, flat, dict)
  def tupledict(fs: Map[String, Rep]): Rep = compiler.tupledict(fs.map(f => (f._1, defToExpr(f._2))))
  def dictunion(d1: Rep, d2: Rep): Rep = compiler.dictunion(d1, d2)
  def select(x: Rep, p: Rep => Rep, e: Rep => Rep): Rep = compiler.select(x, p, e)
  def reduce(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep): Rep = compiler.reduce(e1, f, p)
  def unnest(e1: Rep, f: List[Rep] => Rep, p: List[Rep] => Rep, value: List[Rep] => Rep): Rep = 
    compiler.unnest(e1, f, p, value)
  def join(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, proj1: List[Rep] => Rep, proj2: Rep => Rep): Rep = 
    compiler.join(e1, e2, p1, p2, proj1, proj2)
  def outerunnest(e1: Rep, r: List[Rep] => Rep, p: List[Rep] => Rep, value: List[Rep] => Rep): Rep = 
    compiler.outerunnest(e1, r, p, value)
  def outerjoin(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p: Rep => Rep, proj1: List[Rep] => Rep, proj2: Rep => Rep): Rep = 
    compiler.outerjoin(e1, e2, p1, p, proj1, proj2)
  def nest(e1: Rep, f: List[Rep] => Rep, e: List[Rep] => Rep, p: List[Rep] => Rep, g: List[Rep] => Rep, dk: Boolean): Rep = 
    compiler.nest(e1, f, e, p, g, dk)
  def lkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = 
    compiler.lkup(e1, e2, p1, p2, p3)
  def outerlkup(e1: Rep, e2: Rep, p1: List[Rep] => Rep, p2: Rep => Rep, p3: List[Rep] => Rep): Rep = 
    compiler.outerlkup(e1, e2, p1, p2, p3)
  def cogroup(e1: Rep, e2: Rep, k1: List[Rep] => Rep, k2: Rep => Rep, value: List[Rep] => Rep): Rep = 
    compiler.cogroup(e1, e2, k1, k2, value)
  def flatdict(e1: Rep): Rep = compiler.flatdict(e1)
  def groupdict(e1: Rep): Rep = compiler.groupdict(e1)
  
  def addindex(in: Rep, name: String) = compiler.addindex(in, name)
  def dfproject(in: Rep, filter: Rep => Rep, fields: List[String]): Rep = 
    compiler.dfproject(in, filter, fields)
  def dfunnest(in: Rep, path: String, filter: Rep => Rep, fields: List[String]): Rep = 
    compiler.dfunnest(in, path, filter, fields)
  def dfounnest(in: Rep, path: String, filter: Rep => Rep, fields: List[String]): Rep = 
    compiler.dfounnest(in, path, filter, fields)
  def dfjoin(left: Rep, right: Rep, cond: (Rep, Rep) => Rep, fields: List[String]): Rep = 
    compiler.dfjoin(left, right, cond, fields)
  def dfojoin(left: Rep, right: Rep, cond: (Rep, Rep) => Rep, fields: List[String]): Rep = 
    compiler.dfojoin(left, right, cond, fields)
  def dfnest(in: Rep, key: List[String], value: Rep => Rep, filter: Rep => Rep, nulls: List[String]): Rep = 
    compiler.dfnest(in, key, value, filter, nulls)
  def dfreduceby(in: Rep, key: List[String], values: List[String]): Rep = 
    compiler.dfreduceby(in, key, values)

  def vars(e: Type): List[Variable] = e match {
    case BagCType(tp:RecordCType) => List(Variable.fresh(tp))
    case BagCType(tp @ TTupleType(tps)) => tps.map(Variable.fresh(_))
    case _ => ???
  }
}

/** Finalizes a comprehension expression for a specific compiler.
  *
  */
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
    case CGet(x) => target.get(finalize(x))
    case Tuple(fs) if fs.isEmpty => target.unit
    case Tuple(fs) => target.tuple(fs.map(f => finalize(f)))
    case Record(fs) if fs.isEmpty => target.unit
    case Record(fs) => target.record(fs.map(f => f._1 -> finalize(f._2)))
    case Label(fs) => target.label(fs.map(f => f._1 -> finalize(f._2)))
    case MathOp(op, e1, e2) => target.mathop(op, finalize(e1), finalize(e2))
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
    case CGroupBy(e1, v2, g, v) => target.groupby(finalize(e1), g, v)
    case CReduceBy(e1, v2, g, v) => target.reduceby(finalize(e1), g, v)
    case CNamed(n, e) => target.named(n, finalize(e))
    case LinearCSet(exprs) => 
      target.linset(exprs.map(finalize(_)))
    case CLookup(l, d) => 
      target.lookup(finalize(l), finalize(d))
    case EmptyCDict => target.emptydict
    case BagCDict(l, f, d) => 
      target.bagdict(l, finalize(f), finalize(d))
    case TupleCDict(fs) => target.tupledict(fs.map(f => f._1 -> finalize(f._2)))
    case DictCUnion(d1, d2) => target.dictunion(finalize(d1), finalize(d2))
    case Select(x, v, p, e) =>
      target.select(finalize(x), (r: target.Rep) => 
        withMap(v -> r)(finalize(p)), (r: target.Rep) => withMap(v -> r)(finalize(e)))
    case Reduce(e1, v, e2, p) =>
      target.reduce(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: List[target.Rep]) => withMapList(v zip r)(finalize(p)))
    case Unnest(e1, v, e2, v2, p, value) => 
      target.unnest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p)),
        (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(value)))
    case Nest(e1, v, f, e, v2, p, g, dk) =>
      target.nest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(f)),
        (r: List[target.Rep]) => withMapList(v zip r)(finalize(e)), 
          (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p)), 
          (r: List[target.Rep]) => withMapList(v zip r)(finalize(g)), dk)
    case Join(e1, e2, v, p1, v2, p2, proj1, proj2) =>
      target.join(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)), 
        (r: List[target.Rep]) => withMapList(v zip r)(finalize(proj1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(proj2)))
    case OuterUnnest(e1, v, e2, v2, p, value) => 
      target.outerunnest(finalize(e1), (r: List[target.Rep]) => withMapList(v zip r)(finalize(e2)), 
        (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p)),
        (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(value)))
    case OuterJoin(e1, e2, v, p1, v2, p2, proj1, proj2) =>
      target.outerjoin(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)),
        (r: List[target.Rep]) => withMapList(v zip r)(finalize(proj1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(proj2)))
    case Lookup(e1, e2, v, p1, v2, p2, p3) =>
      target.lkup(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)), (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p3)))
    case OuterLookup(e1, e2, v, p1, v2, p2, p3) =>
      target.outerlkup(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v zip r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)), (r: List[target.Rep]) => withMapList((v :+ v2) zip r)(finalize(p3)))
    case CoGroup(e1, e2, v1, v2, k1, k2, value) =>
      target.cogroup(finalize(e1), finalize(e2), (r: List[target.Rep]) => withMapList(v1 zip r)(finalize(k1)), 
        (r: target.Rep) => withMap(v2 -> r)(finalize(k2)), (r: List[target.Rep]) => withMapList(v1 :+ v2 zip r)(finalize(value)))

    case AddIndex(in, name) => target.addindex(finalize(in), name)    
    case DFProject(in, v, filter, fields) => 
      target.dfproject(finalize(in), (r: target.Rep) => withMap(v -> r)(finalize(filter)), fields)
    case DFUnnest(in, v, path, v2, filter, fields) => 
      target.dfunnest(finalize(in), path, (r: target.Rep) => withMap(v2 -> r)(finalize(filter)), fields)
    case DFOuterUnnest(in, v, path, v2, filter, fields) => 
      target.dfounnest(finalize(in), path, (r: target.Rep) => withMap(v2 -> r)(finalize(filter)), fields)
    case DFJoin(left, v, right, v2, cond, fields) =>
      target.dfjoin(finalize(left), finalize(right), 
        (r: target.Rep, s: target.Rep) => withMapList(List((v,r), (v2,s)))(finalize(cond)), fields)
    case DFOuterJoin(left, v, right, v2, cond, fields) =>
      target.dfojoin(finalize(left), finalize(right), 
        (r: target.Rep, s: target.Rep) => withMapList(List((v,r), (v2,s)))(finalize(cond)), fields)
    case DFNest(in, v, key, value, filter, nulls) =>
      target.dfnest(finalize(in), key, (r: target.Rep) => withMap(v -> r)(finalize(value)), 
        (r: target.Rep) => withMap(v -> r)(finalize(filter)), nulls)
    case DFReduceBy(in, v, keys, values) => 
      target.dfreduceby(finalize(in), keys, values)

    case FlatDict(e1) => target.flatdict(finalize(e1))
    case GroupDict(e1) => target.groupdict(finalize(e1))
    case v @ Variable(_, _) => variableMap.getOrElse(v, target.inputref(v.name, v.tp))
  }

}

