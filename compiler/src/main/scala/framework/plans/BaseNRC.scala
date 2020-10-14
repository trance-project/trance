package framework.plans

import framework.common._
import framework.utils._
import framework.nrc.{NRC, MaterializeNRC}
import framework.nrc.{Label => NRCLabel}

trait BaseNRC extends NRC with NRCLabel with MaterializeNRC {

  type Rep

  def vref(n: String, tp: Type): Rep
  def const(e: Any): Rep
  def project(e: Rep, f: String): Rep
  def forunion(e1: Rep, e2: Rep => Rep): Rep 
  def sng(e: Rep): Rep
  def tuple(fs: Map[String, Rep]): Rep
  def cmp(op: OpCmp, e1: Rep, e2: Rep): Rep 
  def ifst(cond: Rep, e1: Rep, e2: Option[Rep] = None): Rep
  def let(e1: Rep, e2: Rep => Rep): Rep 

  def lbl(fs: Map[String, Rep], id: Int): Rep
  def projectlbl(e: Rep): Rep 
  def bagtodict(e: Rep): Rep
  def dicttobag(e: Rep): Rep
  def lookup(lbl: Rep, dict: Rep): Rep

}

trait BaseNRCCompiler extends BaseNRC {

  type Rep = Expr

  def vref(n: String, tp: Type): Rep = tp match {
    case t:NumericType => NumericVarRef(n, t)
    case t:PrimitiveType => PrimitiveVarRef(n, t)
    case t:BagType => BagVarRef(n, t)
    case t:TupleType => TupleVarRef(n, t)
    case t:MatDictType => MatDictVarRef(n, t)
    case _ => sys.error(s"unsupported type $tp")
  }

  def const(e: Any): Rep = e match {
    case _:Double => NumericConst(e.asInstanceOf[AnyVal], DoubleType)
    case _:Int => NumericConst(e.asInstanceOf[AnyVal], IntType)
    case _:Long => NumericConst(e.asInstanceOf[AnyVal], LongType)
    case _:String => PrimitiveConst(e, StringType)
    case _:Boolean => PrimitiveConst(e, BoolType)
    case tp => sys.error(s"unsupported type $tp")
  }

  def project(e: Rep, f: String): Rep = {
    val tup = e.asInstanceOf[TupleVarRef]
    tup.tp(f) match {
      case _:NumericType => NumericProject(tup, f)
      case _:PrimitiveType => PrimitiveProject(tup, f)
      case _:BagType => BagProject(tup, f)
      case _:LabelType => LabelProject(tup, f)
    }
  }

  def forunion(e1: Rep, e2: Rep => Rep): Rep = {
    val v = fresh(e1.tp)
    val vd = v.asInstanceOf[VarRef].varDef
    ForeachUnion(vd, e1.asInstanceOf[BagExpr], e2(v).asInstanceOf[BagExpr])
  }

  def sng(e: Rep): Rep = 
    Singleton(e.asInstanceOf[TupleExpr])

  def tuple(fs: Map[String, Rep]): Rep = 
    Tuple(fs.asInstanceOf[Map[String, TupleAttributeExpr]])

  def cmp(op: OpCmp, e1: Rep, e2: Rep): Rep = Cmp(op, e1, e2)

  // bag only for now
  def ifst(cnd: Rep, e1: Rep, e2: Option[Rep] = None): Rep = {
    val vcnd = cnd.asInstanceOf[CondExpr]
    val ve2 = e2 match {
      case Some(x) => Some(x.asInstanceOf[BagExpr])
      case None => None
    }
    BagIfThenElse(vcnd, e1.asInstanceOf[BagExpr], ve2)
  }

  def let(e1: Rep, e2: Rep => Rep): Rep = {
    val v = vref(Utils.Symbol.fresh(), e1.tp)
    val vd = v.asInstanceOf[VarRef].varDef
    e2(v) match {
      case x:NumericExpr => NumericLet(vd, e1, x)
      case x:PrimitiveExpr => PrimitiveLet(vd, e1, x) 
      case x:BagExpr => BagLet(vd, e1, x)
      case x:TupleExpr => TupleLet(vd, e1, x)
      case x => sys.error(s"unsupported expression $x")
    }
  }


  def lbl(fs: Map[String, Rep], id: Int): Rep = 
    NewLabel(fs.asInstanceOf[Map[String, LabelParameter]], id)

  def projectlbl(e: Rep): Rep = 
    ProjectLabelParameter(e.asInstanceOf[Expr with Project])

  def bagtodict(e: Rep): Rep = 
    BagToMatDict(e.asInstanceOf[BagExpr])

  def dicttobag(e: Rep): Rep = 
    MatDictToBag(e.asInstanceOf[MatDictExpr])

  def lookup(lbl: Rep, dict: Rep): Rep = 
    MatDictLookup(lbl.asInstanceOf[LabelExpr], dict.asInstanceOf[MatDictExpr])

  // get a fresh variable representing 
  // a tuple inside of a bag
  def fresh(tp: Type): Rep = tp match {
    case BagType(tup) => vref(Utils.Symbol.fresh(), tup)
    case _ => sys.error(s"unsupported type $tp")
  }

}

trait BaseNRCNormalizer extends BaseNRCCompiler {

  // need to support tuples that are expressions and not 
  // just variables
  // override def let(e1: Rep, e2: Rep => Rep): Rep = e2(e1)

  override def forunion(e1: Rep, e2: Rep => Rep): Rep = e1 match {

    // patterns in the head

    // N4
    case BagIfThenElse(cond, e3, Some(e4)) => 
      ifst(cond, forunion(e3, e2), Some(forunion(e4, e2)))
    case BagIfThenElse(cond, e3, None) => 
      ifst(cond, forunion(e3, e2))

    // N5
    case es @ Singleton(Tuple(fs)) if fs.isEmpty => es

    // N6
    case Singleton(t) => let(t, e2)

    // N8
    case ForeachUnion(x, e3, e4) => 
      ForeachUnion(x, e3, forunion(e4, e2).asInstanceOf[BagExpr])

    case _ => super.forunion(e1, e2)
  }

}

class NRCFinalizer(val target: BaseNRC) extends NRC with MaterializeNRC with NRCLabel {

  var variableMap: Map[VarDef, target.Rep] = Map[VarDef, target.Rep]()
  
  def withMap[T](m: (VarDef, target.Rep))(f: => T): T = {
    val old = variableMap
    variableMap = variableMap + m
    val res = f
    variableMap = old
    res
  }

  def finalize(e: Expr): target.Rep = e match {

    case Singleton(e1) => target.sng(finalize(e1))
    case Tuple(fs) => target.tuple(fs.map(f => f._1 -> finalize(f._2)))

    case ForeachUnion(v, e1, e2) =>
      target.forunion(finalize(e1), (r: target.Rep) => withMap(v -> r)(finalize(e2)))

    case i:IfThenElse => i.e2 match {
      case Some(x) => target.ifst(finalize(i.cond), finalize(i.e1), Some(finalize(x)))
      case _ => target.ifst(finalize(i.cond), finalize(i.e1), None)
    }

    case l:Let => target.let(finalize(l.e1), 
      (r: target.Rep) => withMap(l.x -> r)(finalize(l.e2)))

    case c:Cmp => target.cmp(c.op, finalize(c.e1), finalize(c.e2))
    case p:Project => target.project(finalize(p.tuple), p.field)
    case c:Const => target.const(c.v)
    case v:VarRef => variableMap.getOrElse(v.varDef, target.vref(v.name, v.tp))
 
    case NewLabel(fs, id) => target.lbl(fs.map(f => f._1 -> finalize(f._2)), id)    
    case p:ProjectLabelParameter => target.projectlbl(finalize(p.e)) 
    case BagToMatDict(bag) => target.bagtodict(finalize(bag))
    case MatDictToBag(dict) => target.dicttobag(finalize(dict))
    case MatDictLookup(lbl, dict) => target.lookup(finalize(lbl), finalize(dict))

  }

}
