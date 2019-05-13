package shredding.algebra

import shredding.core._

trait Base {
  type Rep
  def input(x: List[Rep]): Rep
  def const(x: Any): Rep
  def sng(x: Rep): Rep
  def tuple(fs: Map[String, Rep]): Rep 
  def kvtuple(e1: Rep, e2: Rep): Rep
  def equals(e1: Rep, e2: Rep): Rep
  def lt(e1: Rep, e2: Rep): Rep
  def gt(e1: Rep, e2: Rep): Rep
  def project(e1: Rep, field: String): Rep
  def select(x: Rep, p: Rep => Rep): Rep
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep
  def unnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep
  def nest(e1: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep
}

trait BaseScalaInterp extends Base{
  type Rep = Any
  def input(x: List[Rep]): Rep = x
  def const(x: Any): Rep = x
  def sng(x: Rep): Rep = x.asInstanceOf[List[_]]
  def tuple(fs: Map[String, Rep]): Rep = fs.asInstanceOf[Map[String, Rep]]
  def kvtuple(e1: Rep, e2: Rep): Rep = (e1, e2)
  def equals(e1: Rep, e2: Rep): Rep = e1 == e2
  def lt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] < e2.asInstanceOf[Int]
  def gt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] > e2.asInstanceOf[Int]
  def project(e1: Rep, field: String) = e1 match {
    case ms:Map[String,Rep] => ms.get(field).get
    case ms:Product => field match {
      case "key" => ms.asInstanceOf[Product].productElement(0)
      case "value" => ms.asInstanceOf[Product].productElement(1)
    }
  }
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
      case l @ ((head: (Int, List[_])) :: tail) => 
        l.asInstanceOf[List[_]].map{case (k,v:List[_]) => (k, v.size)} // sum
      case l => l
    }
    grpd.filter(p.asInstanceOf[Rep => Boolean])
  }
}

trait BaseStringify extends Base{
  type Rep = String
  def input(x: List[Rep]): Rep = s"{${x.mkString(",")}}"
  def const(x: Any): Rep = x.toString
  def sng(x: Rep): Rep = s"{ $x }"
  def tuple(fs: Map[String, Rep]): Rep = s"(${fs.map(f => f._1 + " := " + f._2).mkString(",")})"  
  def kvtuple(e1: Rep, e2: Rep): Rep = s"(${e1}, ${e2})"
  def equals(e1: Rep, e2: Rep): Rep = s"${e1} = ${e2}"
  def lt(e1: Rep, e2: Rep): Rep = s"${e1} < ${e2}"
  def gt(e1: Rep, e2: Rep): Rep = s"${e1} > ${e2}"
  def project(e1: Rep, field: String): Rep = s"${e1}.${field}"
  def select(x: Rep, p: Rep => Rep): Rep = { 
    s"SELECT[ ${p(Variable.fresh(StringType).quote)} ](\n${x})"
  }
  def reduce(x: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v = Variable.fresh(StringType)
    s"REDUCE[ ${f(v.quote)} / ${p(v.quote)} ](\n${x})"
  }
  def unnest(x: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(StringType)
    val v = Variable.fresh(KVTupleType(StringType, StringType))
    s"UNNEST[ ${f(v1.quote)} / ${p(v.quote)} ](\n${x})"
  }
  def nest(x: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(StringType) // todo P
    val acc = e(v1.quote) match { case "1" => "+"; case _ => "U" }
    s"NEST[ ${acc} / ${f(v1.quote)} / true ](\n${x})"
  }
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    val (v1,v2) = (Variable.fresh(StringType), Variable.fresh(StringType))
    s"(${e1}) JOIN[${p1(v1.quote)} = ${p2(v2.quote)}] (${e2})"
  }
}

trait BaseCompiler extends Base{
  type Rep = Expr
  def input(x: List[Rep]): Rep = Input(x)
  def const(x: Any): Rep = Const(x)
  def sng(x: Rep): Rep = Sng(x)
  def tuple(fs: Map[String, Rep]): Rep = Tuple(fs)
  def kvtuple(e1: Rep, e2: Rep): Rep = KVTuple(e1, e2)
  def equals(e1: Rep, e2: Rep): Rep = Equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = Lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = Gt(e1, e2)
  def project(e1: Rep, e2: String): Rep = Project(e1, e2)
  def select(x: Rep, p: Rep => Rep): Rep = {
    val v = Variable.fresh(x.tp.asInstanceOf[BagType].tp)
    Select(x, v, p(v))
  }
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v = Variable.fresh(e1.tp)
    Reduce(e1, v, f(v), p(v))
  }
  def unnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(e1.tp.asInstanceOf[BagType].tp)
    val fv = f(v1)
    val v = Variable.fresh(KVTupleType(v1.tp, fv.tp.asInstanceOf[BagType].tp))
    Unnest(e1, v1, fv, v, p(v))
  }
  def nest(e1: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = Variable.fresh(e1.tp.asInstanceOf[BagType].tp)
    val fv = f(v1) // groups
    val ev = e(v1) // pattern
    val v = Variable.fresh(KVTupleType(fv.tp, ev.tp))
    Nest(e1, v1, fv, ev, v, p(v))
  }
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    val v1 = Variable.fresh(e1.tp.asInstanceOf[BagType].tp)
    val v2 = Variable.fresh(e2.tp.asInstanceOf[BagType].tp)
    Join(e1, e2, v1, p1(v1), v2, p2(v2))
  }
}

class Finalizer(val target: Base){
  var currentMap: Map[Variable, target.Rep] = Map[Variable, target.Rep]()
  def withMap[T](m: (Variable, target.Rep))(f: => T): T = {
    val old = currentMap
    currentMap = currentMap + m
    val res = f
    currentMap = old
    res
  }
  def finalize(e: Expr): target.Rep = e match {
    case Input(x) => target.input(x.map(finalize(_)))
    case Const(x) => target.const(x)
    case Sng(x) => target.sng(finalize(x))
    case Tuple(fs) => target.tuple(fs.map(f => f._1 -> finalize(f._2)))
    case KVTuple(e1, e2) => target.kvtuple(finalize(e1), finalize(e2))
    case Equals(e1, e2) => target.equals(finalize(e1), finalize(e2))
    case Lt(e1, e2) => target.lt(finalize(e1), finalize(e2))
    case Gt(e1, e2) => target.gt(finalize(e1), finalize(e2))
    case Project(e1, pos) => target.project(finalize(e1), pos)
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
    case v @ Variable(_, _) => currentMap(v)
  }
}
