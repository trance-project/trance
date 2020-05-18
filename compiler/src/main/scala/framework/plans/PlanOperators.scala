package framework.plans

import framework.common._

/** Batch processing operators **/

case class AddIndex(e: CExpr, name: String) extends CExpr {
  def tp: BagCType = BagCType(RecordCType(e.tp.attrs ++ Map(name -> LongType)))
}

// rename filter
case class DFProject(in: CExpr, v: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  
  def rename: Map[String, String] = filter match {
    case Record(fs) => fs.toList.flatMap{
      case (key, Project(_, fp)) if key != fp => List((key, fp))
      case _ => Nil
    }.toMap
    case _ => Map()
  }

  def tp: BagCType = BagCType(filter.tp.project(fields))
}

case class DFUnnest(in: CExpr, v: Variable, path: String, v2: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp).project(fields))
}

case class DFOuterUnnest(in: CExpr, v: Variable, path: String, v2: Variable, filter: CExpr, fields: List[String]) extends CExpr {
  val index = Map(path+"_index" -> LongType)
  def tp: BagCType = 
    BagCType(RecordCType((v.tp.attrs - path) ++ index).merge(v2.tp.outer).project(fields))
}

case class DFJoin(left: CExpr, v: Variable, p1: String, right: CExpr, v2: Variable, p2: String, fields: List[String]) extends CExpr {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp).project(fields))
}

case class DFOuterJoin(left: CExpr, v: Variable, p1: String, right: CExpr, v2: Variable, p2: String, fields: List[String]) extends CExpr {
  def tp: BagCType = BagCType(v.tp.merge(v2.tp.outer).project(fields))
}

case class DFNest(in: CExpr, v: Variable, key: List[String], value: CExpr, filter: CExpr, nulls: List[String]) extends CExpr {
  def tp: BagCType = BagCType(RecordCType(v.tp.project(key).attrTps ++ Map("_2" -> BagCType(value.tp))))
}

case class DFReduceBy(in: CExpr, v: Variable, keys: List[String], values: List[String]) extends CExpr {
  // cast to double for sum
  val valueTps = RecordCType(v.tp.project(values).attrs.map(a => a._1 -> DoubleType))
  def tp: BagCType = BagCType(v.tp.project(keys).merge(valueTps))
}