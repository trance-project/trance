package framework.plans

import framework.common._

/** Operators of the plan language **/ 

case class Select(x: CExpr, v: Variable, p: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp match {
    case rt:RecordCType => BagCType(rt)
    case _ => x.tp
  }

  override def wvars = List(v)

}

case class AddIndex(e: CExpr, name: String) extends CExpr {
  def tp: BagCType = BagCType(RecordCType(e.tp.attrs ++ Map(name -> LongType)))
}

// rename filter
case class Projection(in: CExpr, v: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  
  override def inputColumns: Set[String] = v.tp.attrs.keySet

  def rename: Map[String, String] = filter match {
    case Record(fs) => fs.toList.flatMap{
      case (key, Project(_, fp)) if key != fp => List((key, fp))
      case (key, Label(fs)) => fs.head match {
        case (_, Project(_, fp)) if key != fp => List((key, fp))
        case _ => Nil
      }
      case _ => Nil
    }.toMap
    case _ => Map()
  }

  def makeCols: Map[String, CExpr] = filter match {
    case r:Record => 
      val fields = r.inputColumns ++ rename.keySet
      r.fields.filter(f => !fields(f._1))
    case _ => Map()
  }

  def tp: BagCType = BagCType(filter.tp)
}

case class Unnest(in: CExpr, v: Variable, path: String, v2: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp).project(fields))
}

case class OuterUnnest(in: CExpr, v: Variable, path: String, v2: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  val index = Map(path+"_index" -> LongType)
  def tp: BagCType = 
    BagCType(RecordCType((v.tp.attrs - path) ++ index).merge(v2.tp.outer).project(fields))
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
    case _ => sys.error("can't be called on a non-equijoin") 
  }

  val right: CExpr
  val v2: Variable
  def p2: String = cond match {
    case Equals(Project(_, f1), Project(_, f2)) =>
      if (v2.tp.attrs.contains(f1)) f1
      else f2
    case _ => sys.error("can't be called on a non-equijoin") 
  }

  val cond: CExpr

  val fields: List[String]
  val jtype: String

  val isEquiJoin: Boolean = cond match {
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

