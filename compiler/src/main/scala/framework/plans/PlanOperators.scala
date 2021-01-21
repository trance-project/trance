package framework.plans

import framework.common._

case class COption(e: CExpr) extends CExpr {
  def tp: OptionType = OptionType(e.tp)
}

/** Operators of the plan language **/ 

case class Select(x: CExpr, v: Variable, p: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp match {
    case rt:RecordCType => BagCType(rt)
    case _ => x.tp
  }
}

case class AddIndex(e: CExpr, name: String) extends CExpr {
  def tp: BagCType = BagCType(RecordCType(e.tp.attrs ++ Map(name -> LongType)))
}

// rename filter
case class Projection(in: CExpr, v: Variable, filter: CExpr, fields: List[String]) extends CExpr {

  def tp: BagCType = BagCType(filter.tp)
}

/** Unnest operators **/

trait UnnestOp extends CExpr {

  def tp: BagCType
  val in: CExpr
  val v: Variable
  val path: String 
  val v2: Variable
  val filter: CExpr
  val fields: List[String]

  val outer: Boolean 

  val topAttrs: Map[String, CExpr] = 
    v.tp.project(fields).attrs.map(f => f._1 -> Project(v, f._1)) - path

  val nextAttrs: Map[String, CExpr] = 
    v2.tp.project(fields).attrs.map(f => f._1 -> Project(v2, f._1))

}

case class Unnest(in: CExpr, v: Variable, path: String, v2: Variable, filter: CExpr, fields: List[String]) extends UnnestOp {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp).project(fields))
  val outer: Boolean = false
}

case class OuterUnnest(in: CExpr, v: Variable, path: String, v2: Variable, filter: CExpr, fields: List[String]) extends UnnestOp {
  
  val index = Map(path+"_index" -> LongType)
  
  def tp: BagCType = 
    BagCType(RecordCType((v.tp.attrs - path) ++ index).merge(v2.tp.outer).project(fields))
  
  val outer: Boolean = true
  
  override val nextAttrs: Map[String, CExpr] = 
    v2.tp.project(fields).attrs.map(f => f._1 -> COption(Project(v2, f._1)))

}

/** Join operators **/

trait JoinOp extends CExpr {

  def tp: BagCType

  val left: CExpr
  val v: Variable

  def p1: String = cond match {
    case Equals(Project(_, f1), Project(_, f2)) =>
      if (v.tp.attrs.contains(f1)) f1
      else f2
    case _ => sys.error(s"can't be called on a non-equijoin $cond") 
  }

  val right: CExpr
  val v2: Variable
  def p2: String = cond match {
    case Equals(Project(_, f1), Project(_, f2)) =>
      if (v2.tp.attrs.contains(f1)) f1
      else f2
    case _ => sys.error(s"can't be called on a non-equijoin $cond") 
  }

  val cond: CExpr

  val fields: List[String]
  
  val jtype: String

  val isEquiJoin: Boolean = cond match {
    // case And(e1:Equals, e2:Equals) => true
    case Equals(_:Project,_:Project) => true
    case _ => false
  }

}

case class Join(left: CExpr, v: Variable, right: CExpr, v2: Variable, cond: CExpr, fields: List[String]) extends JoinOp {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp).project(fields))
  val jtype = "inner"
}

case class OuterJoin(left: CExpr, v: Variable, right: CExpr, v2: Variable, cond: CExpr, fields: List[String]) extends JoinOp {
  def tp: BagCType = { (cond, right.tp.isDict) match {
    case (Equals(Project(_, p1), Project(_, p2 @ "_1")), true) =>
      val nvtp = RecordCType(v.tp.attrs - p1)
      val nv2tp = RecordCType(v2.tp.attrs - "_1")
      BagCType(nvtp.merge(nv2tp).project(fields))
    case _ => 
      BagCType(v.tp.merge(v2.tp.outer).project(fields))
    }
  }
  val jtype = "left_outer"
}

case class Nest(in: CExpr, v: Variable, key: List[String], value: CExpr, filter: CExpr, nulls: List[String], ctag: String) extends CExpr {
  def tp: BagCType = value.tp match {
    case _:NumericType => BagCType(RecordCType(v.tp.project(key).attrTps ++ Map(ctag -> DoubleType)))
    case _ => BagCType(RecordCType(v.tp.project(key).attrTps ++ Map(ctag -> BagCType(value.tp.unouter))))
  }
}

case class Reduce(in: CExpr, v: Variable, keys: List[String], values: List[String]) extends CExpr {
  def tp: BagCType = BagCType(v.tp.project(keys).merge(v.tp.project(values)))
}








