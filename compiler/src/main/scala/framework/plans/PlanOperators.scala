package framework.plans

/** Operators of the plan language 
 *  All operators have an accompanied compiler in Base.scala
 *  
 *  Each main operator will have at least one input (first element), 
 *  and a variable (second element) - the variable is a binding
 *  to the tuples coming from the input: variable <- input. 
 * 
 **/ 

import framework.common._

trait UnaryOp{ 
  val in: CExpr
}

/**
 * Used for nullable types - this is isn't an operator but 
 * an expression that is only present in the plan language
 */
case class COption(e: CExpr) extends CExpr {
  def tp: OptionType = OptionType(e.tp)
}

/** 
 * Operator for removing nulls when moving from set to bag comprehensions 
 * QUERY: what does this mean? 
 */
case class RemoveNulls(e1: CExpr) extends CExpr {
  def tp: Type = e1.tp
}

/**
 *  Append an index to an input collection (used for dot-equality), 
 * important for maintaining multiplicites in bag comprehensions
 */
case class AddIndex(e: CExpr, name: String) extends CExpr with UnaryOp {
  def tp: BagCType = BagCType(RecordCType(e.tp.attrs ++ Map(name -> LongType)))
  val in: CExpr = e
  override def vstr: String = s"AddIndex(${e.vstr}, $name)" 
  override val isCacheUnfriendly: Boolean = true
}



/**
 *  Selection operator
 * 
 *  x: Input
 *  v: Variable bound to tuples from x
 *  p: filter - a function that returns x when true
 * 
 *  Note that FM have a combined projection and selection 
 *  operator (\delta). This is \delta_{p} only. 
 * 
 *  \delta_{p} (Input)
 * 
 */
case class Select(x: CExpr, v: Variable, p: CExpr) extends CExpr with UnaryOp { self =>

  def tp:Type = x.tp 

  val in: CExpr = x
  override def vstr: String = s"""Select(${x.vstr}, ${p.vstr})"""

}


/**
 *  Projection operator
 * 
 *  in: Input
 *  v: Variable bound to tuples from in
 *  pattern: record representing the head of an empty comprehension, 
 *           includes the renaming (e in FM)
 *  fields: the attributes from Input (in) that will be persisted before 
 *          applying renaming and additional primitive transformations
 * 
 * ex: when pattern is: (a := x.a, b := x.d * 2.0) 
 *     fields {a, d} are projected from in, x.d * 2.0 is evaluated and renamed to b
 * 
 *  Note that FM have a combined projection and selection 
 *  operator (\delta). This is \delta^{e} only. 
 * 
 *  \delta^{e} (Input)
 * 
 */
case class Projection(in: CExpr, v: Variable, pattern: CExpr, fields: List[String]) extends CExpr with UnaryOp { self =>

  def tp: BagCType = BagCType(pattern.tp)
  
  override def vstr: String = 
    s"""Projection(${in.vstr}, {${pattern.vstr}}, ${fields.toSet.mkString(",")})""" 

}

/**
 *  Unnest operator base trait 
 *  inherited by standard and outer unnest
 * 
 *  in: Input
 *  v: Variable bound to tuples from in 
 *  path: name of the bag type attribute to be unnested
 *  v2: Variable bound from tuples from v.path
 *  filter: local selection (p[v]) and projection applied to v2
 *  fields: the attributes from Input (in) - belonging to v and v2 - 
 *          that will remain after applying this operator. 
 * 
 *  \mu^{path}_{filter} (Input)
 * 
 */
trait UnnestOp extends CExpr with UnaryOp {

  def tp: BagCType

  // required attributes
  val in: CExpr
  val v: Variable
  val path: String 
  val v2: Variable
  val filter: CExpr
  val fields: List[String]

  val outer: Boolean 

  // helper functions
  val topAttrs: Map[String, CExpr] = {
    v.tp.project(fields).attrs.map(f => f._1 -> Project(v, f._1)) - path
  }

  val nextAttrs: Map[String, CExpr] = {
    v2.tp.project(fields).attrs.map(f => f._1 -> Project(v2, f._1))
  }

  override def vstr: String = {
    val lbl = if (outer) "Outer" else "" 
    s"""Unnest$lbl(${in.vstr}, $path, ${fields.toSet.mkString(",")})""" 
  }

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

/**
 *  Join operator base trait 
 *  inherited by standard and outer left join
 * 
 *  left: left-side Input
 *  v: Variable bound to tuples from left
 *  right: right-side Input
 *  v2: Variable bound from tuples from right
 *  cond: the join condition
 *  fields: the attributes from left and right that will be 
 *          persisted after the join
 * 
 *  (left) \join_{cond} (right)
 * 
 */
trait JoinOp extends CExpr { 

  def tp: BagCType

  // required attributes
  val left: CExpr
  val v: Variable
  val right: CExpr
  val v2: Variable
  val cond: CExpr
  val fields: List[String]
  
  // inner or outer
  val jtype: String

  val ext = new Extensions{}
  // val ps = ext.collect(cond)
  val ps = ext.collectFromAnd(cond)

  // get around join with domain
  def p1s: Set[String] = v.tp match {
    case RecordCType(ms) => ms.get("_LABEL") match {
      case Some(LabelType(fs)) =>
        val keys = fs.keySet 
        ps.filter(s => keys(s))
      case _ => ps.filter(s => v.tp.attrs.contains(s))
    }
    case _ => ps.filter(s => v.tp.attrs.contains(s))
  }

  def p1: String = p1s.mkString(",")

  def p2s: Set[String] = ps.filter(s => v2.tp.attrs.contains(s))
  def p2: String = p2s.mkString(",")

  val isEquiJoin: Boolean = ps.nonEmpty && (p1s.size == p2s.size)

  // this needs fixed
  override def vstr: String = s"Join$jtype(${left.vstr}, ${right.vstr}, $p1=$p2)"
  override val isCacheUnfriendly: Boolean = true

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

/**
 *  Nest operator (\Gamma^{bag union}) used for groupings
 * 
 *  in: Input
 *  v: Variable bound to tuples from Input
 *  key: list of attributes in the key 
 *  value: expression representing the contents of the groups
 *  filter: a filter to apply on the groups
 *  nulls: list of attributes to convert from null to zero
 *  ctag: the attribute name belonging to the group (default _2)
 * 
 *  \Gamma^{ctag := value / key}_{filter / nulls}
 * 
 */ 
case class Nest(in: CExpr, v: Variable, key: List[String], value: CExpr, filter: CExpr, nulls: List[String], ctag: String) extends CExpr { //with UnaryOp {
  def tp: BagCType = value.tp match {
    case _:NumericType => BagCType(RecordCType(v.tp.project(key).attrTps ++ Map(ctag -> DoubleType)))
    case _ => BagCType(RecordCType(v.tp.project(key).attrTps ++ Map(ctag -> BagCType(value.tp.unouter))))
  }

  override def vstr: String = 
    s"""Nest(${in.vstr}, key = ${key.mkString(",")}, value = ${key.mkString(",")})""" 

  override val isCacheUnfriendly: Boolean = true

}

/**
 *  Reduce operator (\Gamma^{+}) used for sum-aggregation
 * 
 *  in: Input
 *  v: Variable bound to tuples from Input
 *  key: list of attributes in the key 
 *  value: list of attributes in the value (to be summed)
 * 
 *  Note that this is a simplified generic FM Gamma that made 
 *  generating sumBy less complicated than with the Nest operator
 * 
 *  \Gamma^{value / key}
 * 
 */ 
case class Reduce(in: CExpr, v: Variable, keys: List[String], values: List[String]) extends CExpr with UnaryOp {
  def tp: BagCType = BagCType(v.tp.project(keys).merge(v.tp.project(values)))
  override def vstr: String = 
    s"""Reduce(${in.vstr}, keys = ${keys.mkString(",")}, values = ${values.mkString(",")})""" 
}








