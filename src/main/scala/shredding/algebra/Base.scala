package shredding.algebra

import shredding.core._
import shredding.Utils.ind

trait Base {
  type Rep
  def inputref(x: String, tp:Type): Rep
  def input(x: List[Rep]): Rep 
  def constant(x: Any): Rep
  def emptysng: Rep
  def unit: Rep 
  def sng(x: Rep): Rep
  def tuple(fs: Rep*): Rep
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
  def bind(e1: Rep, e: Rep => Rep): Rep 
  def named(n: String, e: Rep): Rep
  def linset(e: List[Rep]): Rep
  def label(id: Int, vars: Map[String, Rep]): Rep
  def lookup(lbl: Rep, dict: Rep => Rep): Rep
  def emptydict: Rep
  def bagdict(flat: Rep => Rep, dict: Rep): Rep
  def tupledict(fs: Map[String, Rep]): Rep
  def dictunion(d1: Rep, d2: Rep): Rep
  def select(x: Rep, p: Rep => Rep): Rep
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep
  def unnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep
  def outerunnest(e1: Rep, r: Rep => Rep, p: Rep => Rep): Rep
  def outerjoin(e1: Rep, e2: Rep, p1: Rep => Rep, p: Rep => Rep): Rep
  def nest(e1: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep
}

trait BaseStringify extends Base{
  type Rep = String
  def inputref(x: String, tp: Type): Rep = x
  def input(x: List[Rep]): Rep = s"{${x.mkString(",")}}"
  def constant(x: Any): Rep = x.toString
  def emptysng: Rep = "{}"
  def unit: Rep = "()"
  def sng(x: Rep): Rep = s"{ $x }"
  def tuple(fs: Rep*) = s"(${fs.mkString(",")})"
  def record(fs: Map[String, Rep]): Rep = 
    s"(${fs.map(f => f._1 + " := " + f._2).mkString(",")})"
  def equals(e1: Rep, e2: Rep): Rep = s"${e1} = ${e2}"
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
  def named(n: String, e: Rep): Rep = s"${n} := ${e}"
  def linset(e: List[Rep]): Rep = e.mkString("\n")
  def label(id: Int, vars: Map[String, Rep]): Rep = 
    s"Label${id}(${vars.map(f => f._2).mkString(",")})"
  def lookup(lbl: Rep, dict: Rep => Rep): Rep = s"Lookup(${lbl}, ${dict(lbl)})"
  def emptydict: Rep = s"Nil"
  def bagdict(flat: Rep => Rep, dict: Rep): Rep = {
    val lbl = Label.fresh()
    s"(${lbl.quote} -> ${flat(lbl.quote)}, ${dict})"
  }
  def tupledict(fs: Map[String, Rep]): Rep =
    s"(${fs.map(f => f._1 + " := " + f._2).mkString(",")})"
  def dictunion(d1: Rep, d2: Rep): Rep = s"${d1} U ${d2}"
  def select(x: Rep, p: Rep => Rep): Rep = { 
    s""" | SELECT[ ${p(Variable.fresh(StringType).quote)} ](${x} )""".stripMargin
  }
  def reduce(x: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v = Variable.fresh(StringType)
    s""" | REDUCE[ ${f(v.quote)} / ${p(v.quote)} ](${x})""".stripMargin
  }
  def unnest(x: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(KVTupleCType(StringType, StringType))
    s""" | UNNEST[ ${f(v1.quote)} / ${p(v.quote)} ](${x})""".stripMargin
  }
  def nest(x: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(StringType) 
    val v2 = Variable.fresh(StringType)
    val acc = e(v1.quote) match { case "1" => "+"; case _ => "U" }
    s""" | NEST[ ${acc} / ${e(v1.quote)} / ${f(v2.quote)}, ${p(v2.quote)} ](${x})""".stripMargin
  }
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) JOIN[${p1(v1.quote)} = ${p2(v2.quote)}]( 
         | ${ind(e2)})""".stripMargin
  }
  def outerunnest(x: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(KVTupleCType(StringType, StringType))
    s""" | OUTERUNNEST[ ${f(v1.quote)} / ${p(v.quote)} ](${x})""".stripMargin
  }
  def outerjoin(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s""" | (${e1}) OUTERJOIN[${p1(v1.quote)} = ${p2(v2.quote)}]( 
         | ${ind(e2)})""".stripMargin
  }

}

trait BaseUnnester extends BaseCompiler {
  override def record(fs: Map[String, Rep]): Rep = {
    Record(fs.map(f => f._2 match {
      case Reduce(d1, v1, e1, p1) =>  // Reduce (o7 := w.m, o8 := Reduce(w.k, v0, 1, true)) group by v1 and 
        d1 match {
          case Project(e, field) => 
            val v2 = Variable.fresh(KVTupleCType(v1.tp, d1.tp.asInstanceOf[BagCType].tp))
            val v3 = Variable.fresh(KVTupleCType(v2.tp, e1.tp))
            f._1 -> Nest(OuterUnnest(e, v1, d1, v2, constant(true)), v2, v2, e1, v3, p1)
          case _ => 
            val v2 = Variable.fresh(KVTupleCType(v1.tp, e1.tp))
            f._1 -> Nest(d1, v1, v1, e1, v2, p1)   
        }
      case f2 => f._1 -> f2
    }))
  }
  override def comprehension(d1: Rep, p1: Rep => Rep, e1: Rep => Rep): Rep = {
    val v1 = Variable.fresh(d1.tp.asInstanceOf[BagCType].tp)
    p1(v1) match {
      case Constant(true) => 
        e1(v1) match {
          case Reduce(d2, v2, e2, p2) => 
            // todo pushing the p2 condition to the join
            Join(Select(d1, v1, p1(v1)), Select(d2, v2, p2), v1, constant(true), v2, constant(true))
          case body => Reduce(d1, v1, body, p1(v1))
        }
      case _ => comprehension(Select(d1, v1, p1(v1)), (i: Rep) => constant(true), e1)   
    }
  }    
}


trait BaseCompiler extends Base {
  type Rep = CExpr 
  def inputref(x: String, tp: Type): Rep = InputRef(x, tp)
  def input(x: List[Rep]): Rep = Input(x)
  def constant(x: Any): Rep = Constant(x)
  def emptysng: Rep = EmptySng
  def unit: Rep = CUnit
  def sng(x: Rep): Rep = Sng(x)
  def tuple(fs: Rep*): Rep = Tuple(fs:_*)
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
  def bind(e1: Rep, e: Rep => Rep): Rep = {
      val v = Variable.fresh(e1.tp)
      Bind(v, e1, e(v)) 
  }
  def named(n: String, e: Rep): Rep = CNamed(n, e)
  def linset(e: List[Rep]): Rep = LinearCSet(e)
  def label(id: Int, vars: Map[String, Rep]): Rep = Label(id, vars)
  def lookup(lbl: Rep, dict: Rep => Rep): Rep = CLookup(lbl, dict(lbl))
  def emptydict: Rep = EmptyCDict
  def bagdict(flat: Rep => Rep, dict: Rep): Rep = { 
    val lbl = Label.fresh()
    BagCDict(lbl, flat(lbl), dict)
  }
  def tupledict(fs: Map[String, Rep]): Rep = TupleCDict(fs)
  def dictunion(d1: Rep, d2: Rep): Rep = DictCUnion(d1, d2)
  def select(x: Rep, p: Rep => Rep): Rep = {
    val v = Variable.fresh(x.tp.asInstanceOf[BagCType].tp)
    Select(x, v, p(v))
  }
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v = Variable.fresh(e1.tp)
    Reduce(e1, v, f(v), p(v))
  }
  def unnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
    val fv = f(v1)
    val v = Variable.fresh(KVTupleCType(v1.tp, fv.tp.asInstanceOf[BagCType].tp))
    Unnest(e1, v1, fv, v, p(v))
  }
  def nest(e1: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
    val fv = f(v1) // groups
    val ev = e(v1) // pattern
    val v = Variable.fresh(KVTupleCType(fv.tp, ev.tp))
    Nest(e1, v1, fv, ev, v, p(v))
  }
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    val v1 = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
    val v2 = Variable.fresh(e2.tp.asInstanceOf[BagCType].tp)
    Join(e1, e2, v1, p1(v1), v2, p2(v2))
  }
  def outerunnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
    val fv = f(v1)
    val v = Variable.fresh(KVTupleCType(v1.tp, fv.tp.asInstanceOf[BagCType].tp))
    OuterUnnest(e1, v1, fv, v, p(v))
  }
  def outerjoin(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    val v1 = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
    val v2 = Variable.fresh(e2.tp.asInstanceOf[BagCType].tp)
    OuterJoin(e1, e2, v1, p1(v1), v2, p2(v2))
  }

}

trait BaseNormalizer extends BaseCompiler {

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
    case _ => super.and(e1, e2)
  }

  override def or(e1: Rep, e2: Rep): Rep  = (e1, e2) match {
    case (Constant(false), Constant(false)) => super.constant(false)
    case (Constant(true), _) => super.constant(true)
    case (_, Constant(true)) => super.constant(true)
    case _ => super.and(e1, e2)
  }

  // N1, N2
  override def bind(e1: Rep, e: Rep => Rep): Rep = e(e1)

  // N3 (a := e1, b: = e2).a = e1
  override def project(e1: Rep, f: String): Rep = e1 match {
    case t:Tuple => t(f.toInt)
    case t:Record => t(f)
    case t:Label => t(f)
    case t:TupleCDict => t(f)
    case t:BagCDict => t(f)
    case _ => super.project(e1, f)
  }

  override def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = cond match {
    case Constant(true) => e1
    case Constant(false) => e2 match {
      case Some(a) =>  a
      case _ => super.emptysng
    }
    case _ => super.ifthen(cond, e1, e2)
  }

  // { e(v) | v <- e1, p(v) }
  // where fegaras and maier does: { e | q, v <- e1, s } 
  // this has { { { e | s } | v <- e1 } | q }
  // the normalization rules reduce generators, which is why I match on e1
  // N10 is automatically handled in this representation
  override def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    e1 match {
      case If(cond, e3, e4 @ Some(a)) => //N4
        If(cond, comprehension(e3, p, e), Some(comprehension(a, p, e)))
      case EmptySng => e1 // N5 
      case Sng(t) => ifthen(p(t), Sng(e(t))) // N6
      case Merge(e1, e2) => Merge(comprehension(e1, p, e), comprehension(e2, p, e))  //N7
      case Variable(_,_) => ifthen(p(e1), Sng(e(e1))) // input relation
      case Comprehension(e2, v2, p2, e3) => //N8 
        // { e(v) | v <- { e3 | v2 <- e2, p2 }, p(v) }
        // { { e(v) | v <- e3 } | v2 <- e2, p2 }
        Comprehension(e2, v2, p2, comprehension(e3, p, e))
      case _ => // standard case (return self)
        val v = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
        Comprehension(e1, v, p(v), e(v))
      }
    }
}

trait BaseScalaInterp extends Base{
  type Rep = Any
  var ctx = scala.collection.mutable.Map[String, Any]()
  def inputref(x: String, tp: Type): Rep = ctx(x)
  def input(x: List[Rep]): Rep = x
  def constant(x: Any): Rep = x
  def emptysng: Rep = Nil
  def unit: Rep = ()
  def sng(x: Rep): Rep = List(x)
  def tuple(x: Rep*): Rep = x
  def record(fs: Map[String, Rep]): Rep = fs.asInstanceOf[Map[String, Rep]]
  def kvtuple(e1: Rep, e2: Rep): Rep = (e1, e2)
  def equals(e1: Rep, e2: Rep): Rep = e1 == e2
  def lt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] < e2.asInstanceOf[Int]
  def gt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] > e2.asInstanceOf[Int]
  def lte(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] <= e2.asInstanceOf[Int]
  def gte(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] >= e2.asInstanceOf[Int]
  def and(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Boolean] && e2.asInstanceOf[Boolean]
  def not(e1: Rep): Rep = !e1.asInstanceOf[Boolean]
  def or(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Boolean] || e2.asInstanceOf[Boolean]
  def project(e1: Rep, field: String) = e1 match {
    case ms:Map[_,_] => ms.asInstanceOf[Map[String,_]].get(field).get
    case ms:List[_] => ms(field.toInt)
    case ms:Product => field match {
      case "key" => ms.asInstanceOf[Product].productElement(0)
      case "value" => ms.asInstanceOf[Product].productElement(1)
      case _ => ms.asInstanceOf[Product].productElement(field.toInt)
    }
  }
  // check this with the notes
  def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = e2 match {
    case Some(a) => if (cond.asInstanceOf[Boolean]) { e1 } else { a }
    case _ => if (cond.asInstanceOf[Boolean]) { e1 } else { Nil }
  } 
  def merge(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[List[_]] ++ e2.asInstanceOf[List[_]]
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    val data = e1.asInstanceOf[List[_]]
    e(data.head) match {
      case i:Map[_,Rep] if i.isEmpty => data
      case i:Int => data.filter(p.asInstanceOf[Rep => Boolean]).map(e).asInstanceOf[List[Int]].sum
      case _ => data.filter(p.asInstanceOf[Rep => Boolean]).map(e)
    }
  }
  def named(n: String, e: Rep): Rep = {
    ctx(n) = e
    e
  }
  def linset(e: List[Rep]): Rep = e
  def bind(e1: Rep, e: Rep => Rep): Rep = e(e1)
  def label(id: Int, fs: Map[String, Rep]): Rep = (id, fs.map(f => f._2))
  def lookup(lbl: Rep, dict: Rep => Rep): Rep = dict(lbl).asInstanceOf[Map[Rep,Rep]](lbl)
  def emptydict: Rep = ()
  def bagdict(flat: Rep => Rep, dict: Rep): Rep = (flat, dict)
  def tupledict(fs: Map[String, Rep]): Rep = fs
  def dictunion(d1: Rep, d2: Rep): Rep = d1 // TODO
  def select(x: Rep, p: Rep => Rep): Rep = 
    x.asInstanceOf[List[_]].filter(p.asInstanceOf[Rep => Boolean])
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = { 
    e1.asInstanceOf[List[_]].map(f).filter(p.asInstanceOf[Rep => Boolean]) match {
      case l @ ((head: Int) :: tail) => l.asInstanceOf[List[Int]].sum
      case l => l
    } 
  }
  def unnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    e1.asInstanceOf[List[_]].flatMap{
      v => f(v).asInstanceOf[List[_]].map{ v2 => (v, v2) }
    }.filter{p.asInstanceOf[Rep => Boolean]}
  }
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    e1.asInstanceOf[List[_]].map(v1 => 
      e2.asInstanceOf[List[_]].filter{ v2 => p1(v1) == p2(v2) }.map(v2 => (v1, v2)))
  }
  def nest(e1: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep = {
    val grpd = e1.asInstanceOf[List[_]].map(v => (f(v), e(v))).groupBy(_._1).toList match {
      case l @ ((head: (_, List[_])) :: tail) => // fix erasure 
        l.asInstanceOf[List[_]].map{case (k,v:List[_]) => (k, v.size)} // sum
      case l => l
    }
    grpd.filter(p.asInstanceOf[Rep => Boolean])
  }
  def outerunnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    e1.asInstanceOf[List[_]].flatMap{
      v => f(v).asInstanceOf[List[_]].map{ v2 => (v, v2) }
    }.filter{p.asInstanceOf[Rep => Boolean]}
  }
  def outerjoin(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    e1.asInstanceOf[List[_]].map(v1 => 
      e2.asInstanceOf[List[_]].filter{ v2 => p1(v1) == p2(v2) }.map(v2 => (v1, v2)))
  }

}

class Finalizer(val target: Base){
  var variableMap: Map[Variable, target.Rep] = Map[Variable, target.Rep]()
  var labelMap: Map[target.Rep, target.Rep] = Map[target.Rep, target.Rep]()
  def withMap[T](m: (Variable, target.Rep))(f: => T): T = {
    val old = variableMap
    variableMap = variableMap + m
    val res = f
    variableMap = old
    res
  }
  def withLMap[T](m: (target.Rep, target.Rep))(f: => T): T = {
    val old = labelMap
    labelMap = labelMap + m
    val res = f
    labelMap = old
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
    case t:Tuple if t.fields.isEmpty => target.unit
    case t:Tuple => target.tuple(t.fields.map(f => finalize(f)):_*)
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
    case Bind(x, e1, e) =>
      target.bind(finalize(e1), (r: target.Rep) => withMap(x -> r)(finalize(e)))
    case CNamed(n, e) => target.named(n, finalize(e))
    case LinearCSet(exprs) => target.linset(exprs.map(finalize(_)))
    case Label(id, vars) => {
      val lbl = target.label(id, vars.withFilter(f => !f._2.isInstanceOf[InputRef]).map(f => 
                  f._1 -> finalize(f._2)))
      labelMap.getOrElse(lbl, lbl)
    }
    case CLookup(l, d) => 
      val lbl = finalize(l)
      target.lookup(lbl, (r: target.Rep) => withLMap(lbl -> r)(finalize(d)))
    case EmptyCDict => target.emptydict
    case BagCDict(l, f, d) => target.bagdict((r: target.Rep) => withLMap(finalize(l) -> r)(finalize(f)), finalize(d))
    case TupleCDict(fs) => target.tupledict(fs.map(f => f._1 -> finalize(f._2)))
    case DictCUnion(d1, d2) => target.dictunion(finalize(d1), finalize(d2))
    case Select(x, v, p) =>
      target.select(finalize(x), (r: target.Rep) => withMap(v -> r)(finalize(p)))
    case Reduce(e1, v, e2, p) => 
      target.reduce(finalize(e1), (r: target.Rep) => withMap(v -> r)(finalize(e2)), 
        (r: target.Rep) => withMap(v -> r)(finalize(p)))
    case Unnest(e1, v1, e2, v2, p) => 
      target.unnest(finalize(e1), (r: target.Rep) => withMap(v1 -> r)(finalize(e2)), 
        (r: target.Rep) => withMap(v2 -> r)(finalize(p)))
    case Nest(e1, v1, f, e, v2, p) => 
      target.nest(finalize(e1), (r: target.Rep) => withMap(v1 -> r)(finalize(f)),
        (r: target.Rep) => withMap(v1 -> r)(finalize(e)), 
          (r: target.Rep) => withMap(v2 -> r)(finalize(p)))
    case Join(e1, e2, v1, p1, v2, p2) =>
      target.join(finalize(e1), finalize(e2), (r: target.Rep) => withMap(v1 -> r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)))
    case OuterUnnest(e1, v1, e2, v2, p) => 
      target.outerunnest(finalize(e1), (r: target.Rep) => withMap(v1 -> r)(finalize(e2)), 
        (r: target.Rep) => withMap(v2 -> r)(finalize(p)))
    case OuterJoin(e1, e2, v1, p1, v2, p2) =>
      target.outerjoin(finalize(e1), finalize(e2), (r: target.Rep) => withMap(v1 -> r)(finalize(p1)),
        (r: target.Rep) => withMap(v2 -> r)(finalize(p2)))
    case v @ Variable(_, _) => variableMap.getOrElse(v, target.inputref(v.name, v.tp) )
  }
}


