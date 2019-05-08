package shredding.algebra

trait Base {
  type Rep
  def input(x: Any): Rep 
  def equals(e1: Rep, e2: Rep): Rep
  def lt(e1: Rep, e2: Rep): Rep
  def gt(e1: Rep, e2: Rep): Rep
  def project(e1: Rep, pos: Int): Rep
  def select(x: Rep, p: Rep => Rep): Rep
  def reduce(x: Rep, p: Rep => Rep): Rep
  def plan(e1: Rep, e2: Rep): Rep
}

trait BaseScalaInterp extends Base{
  type Rep = Any
  def input(x: Rep): Rep = x
  def equals(e1: Rep, e2: Rep): Rep = e1 == e2
  def lt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] < e2.asInstanceOf[Int]
  def gt(e1: Rep, e2: Rep): Rep = e1.asInstanceOf[Int] > e2.asInstanceOf[Int]
  def project(e1: Rep, pos: Int) = e1.asInstanceOf[Product].productElement(pos)
  def select(x: Rep, p: Rep => Rep): Rep = x.asInstanceOf[List[_]].filter(p.asInstanceOf[Rep => Boolean])
  def reduce(e: Rep, p: Rep => Rep): Rep = p(e)
  def plan(e1: Rep, e2: Rep): Rep = e2
}

trait BaseSparkInterp extends Base{

}

trait BaseCompiler extends Base{
  type Rep = Expr
  def input(x: Any): Rep = Input(x)
  def equals(e1: Rep, e2: Rep): Rep = Equals(e1, e2)
  def lt(e1: Rep, e2: Rep): Rep = Lt(e1, e2)
  def gt(e1: Rep, e2: Rep): Rep = Gt(e1, e2)
  def project(e1: Rep, e2: Int): Rep = Project(e1, e2)
  def select(x: Rep, p: Rep => Rep): Rep = {
    val tp = x.tp
    val v = VarDef.fresh(tp)
    Select(x, v, p(v))
  }
  def reduce(x: Rep, p: Rep => Rep): Rep = Reduce(x, p)
  def plan(e1: Rep, e2: Rep): Rep = Plan(e1, e2)
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
    case Input(x) => target.input(x)
    case Equals(e1, e2) => target.equals(finalize(e1), finalize(e2))
    case Lt(e1, e2) => target.lt(finalize(e1), finalize(e2))
    case Gt(e1, e2) => target.gt(finalize(e1), finalize(e2))
    case Project(e1, pos) => target.project(finalize(e1), pos)
    case Select(x, v, p) => {
      target.select(finalize(x), (r: target.Rep) => withMap(v -> r)(finalize(p)))
    }
    case Reduce(x, p) => target.reduce(finalize(x), (r: target.Rep) => r)
    case Plan(e1, e2) => target.plan(finalize(e1), finalize(e2))
    case v@VarDef(_, _) => currentMap(v)
  }
}
