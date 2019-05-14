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
}

trait BaseUnnester extends Base{
  import shredding.algebra.{Expr => Alg, BaseCompiler => AlgCompiler} 
  val compiler = new AlgCompiler{}
  type Rep = Alg
  def const(x: Any): Rep = compiler.const(x)
  def input(x: List[Rep]): Rep = compiler.input(x)
  def sng(x: Rep): Rep = compiler.sng(x)
  def tuple(fs: Map[String, Rep]): Rep = compiler.tuple(fs)
  def equals(e1: Rep, e2: Rep): Rep = compiler.equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = compiler.lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = compiler.gt(e1, e2)
  def project(e1: Rep, field: String): Rep = compiler.project(e1, field)
  def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = e1 match {
    case shredding.algebra.Input(_) => compiler.reduce(compiler.select(e1, p), e, x => compiler.const(true))
    case _ => compiler.reduce(e1, e, p) 
  }

  /**e match {
    case comp @ ((Rep) => shredding.algebra.Tuple(_)) => compiler.reduce(e1, e, p)
    case comp @ ((Rep) => _) => comprehension(compiler.select(e1, p), e, x, compiler.const(true))
    case _ => compiler.reduce(e1, e, p)
  }**/
}

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
    case v @ VarDef(_, _) => currentMap(v)
  }
}
