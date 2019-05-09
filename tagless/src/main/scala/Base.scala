package shredding.algebra

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
  def select(x: Rep, p: Rep => Rep): Rep
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep
}

trait BaseScalaInterp extends Base{
  type Rep = Any
  def input(x: List[Rep]): Rep = x
  def const(x: Any): Rep = x
  def sng(x: Rep): Rep = x.asInstanceOf[List[_]]
  def tuple(fs: Map[String, Rep]): Rep = fs.asInstanceOf[Map[String, Rep]]
  def equals(e1: Rep, e2: Rep): Rep = e1 == e2
  def lt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] < e2.asInstanceOf[Int]
  def gt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] > e2.asInstanceOf[Int]
  def project(e1: Rep, field: String) = e1.asInstanceOf[Map[String, Rep]].get(field).get
  def select(x: Rep, p: Rep => Rep): Rep = 
    x.asInstanceOf[List[_]].filter(p.asInstanceOf[Rep => Boolean])
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = { 
    e1.asInstanceOf[List[_]].map(f).filter(p.asInstanceOf[Rep => Boolean]) 
  }
}

trait BaseSparkInterp extends Base{

}

trait BaseCompiler extends Base{
  type Rep = Expr
  def input(x: List[Rep]): Rep = Input(x)
  def const(x: Any): Rep = Const(x)
  def sng(x: Rep): Rep = Sng(x)
  def tuple(fs: Map[String, Rep]): Rep = Tuple(fs)
  def equals(e1: Rep, e2: Rep): Rep = Equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = Lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = Gt(e1, e2)
  def project(e1: Rep, e2: String): Rep = Project(e1, e2)
  def select(x: Rep, p: Rep => Rep): Rep = {
    val v = VarDef.fresh(x.tp.asInstanceOf[BagType].tp)
    Select(x, v, p(v))
  }
  def reduce(e1: Rep, f: Rep => Rep, p: Rep => Rep): Rep = {
    val v = VarDef.fresh(e1.tp.asInstanceOf[BagType].tp)
    Reduce(e1, v, f(v), p(v))
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
    case Select(x, v, p) =>
      target.select(finalize(x), (r: target.Rep) => withMap(v -> r)(finalize(p)))
    case Reduce(e1, v, e2, p) => 
      target.reduce(finalize(e1), (r: target.Rep) => withMap(v -> r)(finalize(e2)), 
        (r: target.Rep) => withMap(v -> r)(finalize(p)))
    case v@VarDef(_, _) => currentMap(v)
  }
}
