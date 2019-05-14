package shredding.calc

import shredding.types._

trait Base {
  type Rep
  def input(x: List[Rep]): Rep 
  def const(x: Any): Rep
  def sng(x: Rep): Rep
  def tuple(fs: Map[String, Rep]): Rep
  def equals(e1: Rep, e2: Rep): Rep
  def lt(e1: Rep, e2: Rep): Rep
  def gt(e1: Rep, e2: Rep): Rep
  def project(e1: Rep, field: String): Rep
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep
  def select(x: Rep, p: Rep => Rep): Rep
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep
  def unnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep
  def nest(e1: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep
}

// trait BaseUnnester extends Base{
//   import shredding.algebra.{Expr => Alg, BaseCompiler => AlgCompiler} 
//   val compiler = new AlgCompiler{}
//   type Rep = Alg
//   def const(x: Any): Rep = compiler.const(x)
//   def input(x: List[Rep]): Rep = compiler.input(x)
//   def sng(x: Rep): Rep = compiler.sng(x)
//   def tuple(fs: Map[String, Rep]): Rep = compiler.tuple(fs)
//   def equals(e1: Rep, e2: Rep): Rep = compiler.equals(e1, e2)
//   def lt(e1: Rep, e2: Rep): Rep = compiler.lt(e1, e2)
//   def gt(e1: Rep, e2: Rep): Rep = compiler.gt(e1, e2)
//   def project(e1: Rep, field: String): Rep = compiler.project(e1, field)
//   def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = e1 match {
//     case shredding.algebra.Input(_) => compiler.reduce(compiler.select(e1, p), e, x => compiler.const(true))
//     case _ => compiler.reduce(e1, e, p) 
//   }

//   /**e match {
//     case comp @ ((Rep) => shredding.algebra.Tuple(_)) => compiler.reduce(e1, e, p)
//     case comp @ ((Rep) => _) => comprehension(compiler.select(e1, p), e, x, compiler.const(true))
//     case _ => compiler.reduce(e1, e, p)
//   }**/
// }

trait BaseStringify extends Base{
  type Rep = String
  def input(x: List[Rep]): Rep = s"{${x.mkString(",")}}"
  def const(x: Any): Rep = x.toString
  def sng(x: Rep): Rep = s"{ $x }"
  def tuple(fs: Map[String, Rep]): Rep = 
    s"(${fs.map(f => f._1 + " := " + f._2).mkString(",")})"
  def equals(e1: Rep, e2: Rep): Rep = s"${e1} = ${e2}"
  def lt(e1: Rep, e2: Rep): Rep = s"${e1} < ${e2}"
  def gt(e1: Rep, e2: Rep): Rep = s"${e1} > ${e2}"
  def project(e1: Rep, field: String): Rep = s"${e1}.${field}"
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = { 
    val x = VarDef.fresh(StringType)
    s"{ ${e(x.quote)} | ${x.quote} <- ${e1}, ${p(x.quote)} }" 
  }
  def select(x: Rep, p: Rep => Rep): Rep = { 
    s"SELECT[ ${p(VarDef.fresh(StringType).quote)} ](\n${x})"
  }
  def reduce(x: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v = VarDef.fresh(StringType)
    s"REDUCE[ ${f(v.quote)} / ${p(v.quote)} ](\n${x})"
  }
  def unnest(x: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = VarDef.fresh(StringType)
    val v = VarDef.fresh(KVTupleType(StringType, StringType))
    s"UNNEST[ ${f(v1.quote)} / ${p(v.quote)} ](\n${x})"
  }
  def nest(x: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = VarDef.fresh(StringType) // todo P
    val acc = e(v1.quote) match { case "1" => "+"; case _ => "U" }
    s"NEST[ ${acc} / ${f(v1.quote)} / true ](\n${x})"
  }
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    val (v1,v2) = (VarDef.fresh(StringType), VarDef.fresh(StringType))
    s"(${e1}) JOIN[${p1(v1.quote)} = ${p2(v2.quote)}] (${e2})"
  }
}

trait BaseCompiler extends Base {
  type Rep = Expr 
  def input(x: List[Rep]): Rep = Input(x)
  def const(x: Any): Rep = Const(x)
  def sng(x: Rep): Rep = Sng(x)
  def tuple(fs: Map[String, Rep]): Rep = Tuple(fs)
  def equals(e1: Rep, e2: Rep): Rep = Equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = Lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = Gt(e1, e2)
  def project(e1: Rep, e2: String): Rep = Project(e1, e2)
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    val v = VarDef.fresh(e1.tp.asInstanceOf[BagType].tp)
    Comprehension(e1, v, p(v), e(v))
  }
  def select(x: Rep, p: Rep => Rep): Rep = {
    val v = VarDef.fresh(x.tp.asInstanceOf[BagType].tp)
    Select(x, v, p(v))
  }
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v = VarDef.fresh(e1.tp)
    Reduce(e1, v, f(v), p(v))
  }
  def unnest(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = VarDef.fresh(e1.tp.asInstanceOf[BagType].tp)
    val fv = f(v1)
    val v = VarDef.fresh(KVTupleType(v1.tp, fv.tp.asInstanceOf[BagType].tp))
    Unnest(e1, v1, fv, v, p(v))
  }
  def nest(e1: Rep, f: Rep => Rep, e: Rep => Rep, p: Rep => Rep): Rep = {
    val v1 = VarDef.fresh(e1.tp.asInstanceOf[BagType].tp)
    val fv = f(v1) // groups
    val ev = e(v1) // pattern
    val v = VarDef.fresh(KVTupleType(fv.tp, ev.tp))
    Nest(e1, v1, fv, ev, v, p(v))
  }
  def join(e1: Rep, e2: Rep, p1: Rep => Rep, p2: Rep => Rep): Rep = {
    val v1 = VarDef.fresh(e1.tp.asInstanceOf[BagType].tp)
    val v2 = VarDef.fresh(e2.tp.asInstanceOf[BagType].tp)
    Join(e1, e2, v1, p1(v1), v2, p2(v2))
  }
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
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = ???
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

trait BaseUnnester extends BaseCompiler {
  override def comprehension(d1: Rep, p1: Rep => Rep, e1: Rep => Rep): Rep = {
    val v1 = VarDef.fresh(d1.tp.asInstanceOf[BagType].tp)
    e1(v1) match {
      // case Comprehension(d2, v2, p2, e2) =>
      case Reduce(d2, v2, e2, p2) =>
        Join(Select(d1, v1, p1(v1)), Select(d2, v2, p2), v1, const(true), v2, const(true)) // TODO pushing the p2 condition to the join
      case body => Reduce(d1, v1, body, p1(v1))
    }
  } 
  /**e match {
    case comp @ ((Rep) => shredding.algebra.Tuple(_)) => compiler.reduce(e1, e, p)
    case comp @ ((Rep) => _) => comprehension(compiler.select(e1, p), e, x, compiler.const(true))
    case _ => compiler.reduce(e1, e, p)
  }**/
}

class Finalizer(val target: Base){
  var currentMap: Map[VarDef, target.Rep] = Map[VarDef, target.Rep]()
  def withMap[T](m: (VarDef, target.Rep))(f: => T): T = {
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
    case Equals(e1, e2) => target.equals(finalize(e1), finalize(e2))
    case Lt(e1, e2) => target.lt(finalize(e1), finalize(e2))
    case Gt(e1, e2) => target.gt(finalize(e1), finalize(e2))
    case Project(e1, pos) => target.project(finalize(e1), pos)
    case Comprehension(e1, v, p, e) =>
      target.comprehension(finalize(e1), (r: target.Rep) => withMap(v -> r)(finalize(p)), 
        (r: target.Rep) => withMap(v -> r)(finalize(e)))
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
    case v @ VarDef(_, _) => currentMap(v)
  }
}
