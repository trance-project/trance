package shredding.nrc2

import shredding.core._

/**
  * Base NRC expressions
  */
trait BaseExpr {

  sealed trait Expr {
    def tp: Type
  }

  trait TupleAttributeExpr extends Expr {
    def tp: TupleAttributeType
  }

  trait LabelAttributeExpr extends TupleAttributeExpr {
    def tp: LabelAttributeType
  }

  trait PrimitiveExpr extends LabelAttributeExpr {
    def tp: PrimitiveType
  }

  trait BagExpr extends TupleAttributeExpr {
    def tp: BagType
  }

  trait TupleExpr extends Expr {
    def tp: TupleType
  }

  trait LabelExpr extends LabelAttributeExpr {
    def tp: LabelAttributeType
  }

}

trait NRC extends BaseExpr with Dictionary {

  /**
    * NRC constructs
    */
  case class Const(v: Any, tp: PrimitiveType) extends PrimitiveExpr

  trait VarRef extends Expr {
    def varDef: VarDef

    def name: String = varDef.name

    def tp: Type = varDef.tp
  }

  case object VarRef {
    def apply(varDef: VarDef): VarRef = varDef.tp match {
      case _: PrimitiveType => PrimitiveVarRef(varDef)
      case _: BagType => BagVarRef(varDef)
      case _: TupleType => TupleVarRef(varDef)
      case _: LabelType => LabelVarRef(varDef)
      case _ => sys.error("Cannot create VarRef for type " + varDef.tp)
    }
  }

  case class PrimitiveVarRef(varDef: VarDef) extends PrimitiveExpr with VarRef {
    override val tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  case class LabelVarRef(varDef: VarDef) extends LabelExpr with VarRef {
    override val tp: LabelType = super.tp.asInstanceOf[LabelType]
  }

  case class BagVarRef(varDef: VarDef) extends BagExpr with VarRef {
    override val tp: BagType = super.tp.asInstanceOf[BagType]
  }

  case class TupleVarRef(varDef: VarDef) extends TupleExpr with VarRef {
    override val tp: TupleType = super.tp.asInstanceOf[TupleType]
  }

  trait Project extends Expr {
    def tuple: TupleExpr

    def field: String
  }

  case object Project {
    def apply(tuple: TupleExpr, field: String): TupleAttributeExpr = tuple.tp.attrs(field) match {
      case _: PrimitiveType => ProjectToPrimitive(tuple, field)
      case _: BagType => ProjectToBag(tuple, field)
      case _: LabelType => ProjectToLabel(tuple, field)
      case t => sys.error("Unknown type in Project.apply: " + t)
    }
  }

  case class ProjectToPrimitive(tuple: TupleExpr, field: String) extends PrimitiveExpr with Project {
    val tp: PrimitiveType = tuple.tp.attrs(field).asInstanceOf[PrimitiveType]
  }

  case class ProjectToBag(tuple: TupleExpr, field: String) extends BagExpr with Project {
    val tp: BagType = tuple.tp.attrs(field).asInstanceOf[BagType]
  }

  case class ProjectToLabel(tuple: TupleExpr, field: String) extends LabelExpr with Project {
    val tp: LabelType = tuple.tp.attrs(field).asInstanceOf[LabelType]
  }

  case class ForeachUnion(x: VarDef, e1: BagExpr, e2: BagExpr) extends BagExpr {
    assert(x.tp == e1.tp.tp)

    val tp: BagType = e2.tp
  }

  case class Union(e1: BagExpr, e2: BagExpr) extends BagExpr {
    assert(e1.tp == e2.tp)

    val tp: BagType = e1.tp
  }

  case class Singleton(e: TupleExpr) extends BagExpr {
    val tp: BagType = BagType(e.tp)
  }

  case class Tuple(fields: Map[String, TupleAttributeExpr]) extends TupleExpr {
    val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
  }

  object Tuple {
    def apply(fs: (String, TupleAttributeExpr)*): Tuple = Tuple(Map(fs: _*))
  }

  case class Let(x: VarDef, e1: Expr, e2: Expr) extends Expr {
    assert(x.tp == e1.tp)

    val tp: Type = e2.tp
  }

  case class Mult(e1: TupleExpr, e2: BagExpr) extends PrimitiveExpr {
    assert(e1.tp == e2.tp.tp)

    val tp: PrimitiveType = IntType
  }

  case class Cond(op: OpCmp, e1: TupleAttributeExpr, e2: TupleAttributeExpr)

  case class IfThenElse(cond: Cond, e1: BagExpr, e2: Option[BagExpr] = None) extends BagExpr {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: BagType = e1.tp
  }

  case class Relation(n: String, tuples: List[Any], tp: BagType) extends BagExpr

  /**
    * Shredding NRC extension
    */
  case class NewLabel(free: Map[String, VarRef]) extends LabelExpr {
    val tp: LabelType = LabelType(free.map(f => f._1 -> f._2.tp.asInstanceOf[LabelAttributeType]))
  }

  case class Lookup(lbl: LabelExpr, dict: BagDict) extends BagExpr {
    def tp: BagType = dict.flatBagTp
  }

  /**
    * Runtime labels appearing in shredded input relations
    */
  object LabelId {
    private var currId = 0

    implicit def orderingById: Ordering[LabelId] = Ordering.by(e => e.id)
  }

  case class LabelId() extends LabelExpr {
    val id: Int = {
      LabelId.currId += 1; LabelId.currId
    }

    def tp: LabelType = LabelType("id" -> IntType)

    override def equals(that: Any): Boolean = that match {
      case that: LabelId => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

    override def toString: String = s"LabelId($id)"
  }

}

trait ShreddingTransform {
  this: NRC with NRCTransforms =>

  case class ShredValue(flat: Any, flatTp: Type, dict: Dict) {
    def quote: String =
      s"""|Flat: $flat
          |Dict: ${Printer.quote(dict)}""".stripMargin
  }

  case class ShredExpr(flat: Expr, dict: Dict) {
    def quote: String =
      s"""|Flat: ${Printer.quote(flat)}
          |Dict: ${Printer.quote(dict)}""".stripMargin
  }

  /**
    * Shredding transformation
    */
  object Shredder {

    def flatTp(tp: Type): Type = tp match {
      case _: PrimitiveType => tp
      case _: BagType => LabelType("id" -> IntType)
      case TupleType(as) =>
        TupleType(as.map { case (n, t) => n -> flatTp(t).asInstanceOf[TupleAttributeType] })
      case _ => sys.error("unknown flat type of " + tp)
    }

    def shredValue(v: Any, tp: Type): ShredValue = tp match {
      case _: PrimitiveType =>
        ShredValue(v, flatTp(tp), EmptyDict)

      case BagType(tp2) =>
        val l = v.asInstanceOf[List[_]]
        val sl = l.map(shredValue(_, tp2))
        val flatBag = sl.map(_.flat)
        val flatBagTp = BagType(flatTp(tp2).asInstanceOf[TupleType])
        val tupleDict = sl.map(_.dict).reduce(_.union(_)).asInstanceOf[TupleDict]
        val lbl = LabelId()
        val matDict = InputBagDict(Map(lbl -> flatBag), flatBagTp, tupleDict)
        ShredValue(lbl, flatTp(tp), matDict)

      case TupleType(as) =>
        val m = v.asInstanceOf[Map[String, Any]]
        val sm = m.map { case (n, a) => n -> shredValue(a, as(n)) }
        val flat = sm.map { case (n, s) => n -> s.flat }
        val dict = TupleDict(sm.map { case (n, s) => n -> s.dict.asInstanceOf[AttributeDict] })
        ShredValue(flat, flatTp(tp), dict)

      case _ => sys.error("shredValue with unknown type of " + tp)
    }

    def unshredValue(s: ShredValue): Any = unshredValue(s.flat, s.flatTp, s.dict)

    def unshredValue(flat: Any, flatTp: Type, dict: Dict): Any = flatTp match {
      case _: PrimitiveType => flat

      case _: LabelType =>
        val lbl = flat.asInstanceOf[LabelId]
        val matDict = dict.asInstanceOf[InputBagDict]
        matDict.f(lbl).map(t => unshredValue(t, matDict.flatBagTp.tp, matDict.tupleDict))

      case TupleType(as) =>
        val tuple = flat.asInstanceOf[Map[String, Any]]
        val tupleDict = dict.asInstanceOf[TupleDict]
        as.map { case (n, tp) => n -> unshredValue(tuple(n), tp, tupleDict.fields(n)) }

      case _ => sys.error("unshredValue with unknown type of " + flatTp)
    }

    def apply(e: Expr): ShredExpr = shred(e, Map.empty)

    def ivars(e: Expr): Map[String, VarRef] =
      // TODO: compute ivars
      Map.empty

    private def shred(e: Expr, ctx: Map[String, ShredExpr]): ShredExpr = e match {
      case Const(_, _) => ShredExpr(e, EmptyDict)

      case p: Project =>
        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(p.tuple, ctx)
        ShredExpr(Project(flat, p.field), dict.fields(p.field))

      case v: VarRef => ctx(v.name)

      case ForeachUnion(x, e1, e2) =>
        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val xdef = VarDef(x.name, dict1.flatBagTp.tp)
        val ShredExpr(l2: LabelExpr, dict2: BagDict) =
          shred(e2, ctx + (x.name -> ShredExpr(VarRef(xdef), dict1.tupleDict)))
//        val lbl = NewLabel(ivars(e))
        val lbl = LabelId()
        ShredExpr(lbl, OutputBagDict(lbl, ForeachUnion(xdef, dict1.flatBag(l1), dict2.flatBag(l2)), dict2.tupleDict))

      case Union(e1, e2) =>
        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val ShredExpr(l2: LabelExpr, dict2: BagDict) = shred(e2, ctx)
        val dict = dict1.union(dict2).asInstanceOf[BagDict]
        val lbl = LabelId()
        ShredExpr(lbl, OutputBagDict(lbl, Union(dict1.flatBag(l1), dict2.flatBag(l2)), dict.tupleDict))

      case Singleton(e1) =>
        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(e1, ctx)
        val lbl = NewLabel(ivars(e1))
        ShredExpr(lbl, OutputBagDict(lbl, Singleton(flat), dict))

      case Tuple(fs) =>
        val sfs = fs.map(f => f._1 -> shred(f._2, ctx))
        ShredExpr(
          Tuple(sfs.map(f => f._1 -> f._2.flat.asInstanceOf[TupleAttributeExpr])),
          TupleDict(sfs.map(f => f._1 -> f._2.dict.asInstanceOf[AttributeDict]))
        )

      case Let(x: VarDef, e1, e2) =>
        val se1 = shred(e1, ctx)
        val x1 = VarDef(x.name, se1.flat.tp)
        val se2 = shred(e2, ctx + (x.name -> ShredExpr(VarRef(x1), se1.dict)))
        ShredExpr(Let(x1, se1.flat, se2.flat), se2.dict)

      case Mult(e1, e2) =>
        val ShredExpr(flat1: TupleExpr, _: Dict) = shred(e1, ctx)    // dict1 is empty
        val ShredExpr(l2: LabelExpr, dict2: BagDict) = shred(e2, ctx)
        ShredExpr(Mult(flat1, dict2.flatBag(l2)), EmptyDict)

      case IfThenElse(c, e1, None) =>
        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val lbl = NewLabel(ivars(e1))
        ShredExpr(lbl, OutputBagDict(lbl, IfThenElse(c, dict1.flatBag(l1), None), dict1.tupleDict))

      case IfThenElse(c, e1, Some(e2)) =>
        sys.error("TODO")

      case Relation(_, ts, tp) =>
        val ShredValue(flat: LabelId, _: LabelType, dict: InputBagDict) = shredValue(ts, tp)
        ShredExpr(flat, dict)

      case _ => sys.error("not implemented")
    }
  }

}


object TestApp extends App {

  import NRCImplicits._

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = Relation("R", List(
          Map("a" -> 42, "b" -> "Milos"),
          Map("a" -> 69, "b" -> "Michael"),
          Map("a" -> 34, "b" -> "Jaclyn"),
          Map("a" -> 42, "b" -> "Thomas")
        ), BagType(itemTp))

      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)

      val q1 = ForeachUnion(xdef, relationR, Singleton(Tuple("w" -> Project(xref, "b"))))

      println("Q1: " + q1.quote)
      println("Q1 eval: " + q1.eval)
      println("Shredded Q1: " + q1.shred.quote)

      val ydef = VarDef("y", itemTp)
      val yref = TupleVarRef(ydef)
      val q2 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "grp" -> Project(xref, "a"),
          "bag" -> ForeachUnion(ydef, relationR,
            IfThenElse(
              Cond(OpEq, Project(xref, "a"), Project(yref, "a")),
              Singleton(Tuple("q" -> Project(yref, "b")))
            ))
        )))

      println("Q2: " + q2.quote)
      println("Q2 eval: " + q2.eval)
      println("Shredded Q2: " + q2.shred.quote)
    }
  }

  object Example2 {

    def run(): Unit = {

      val nested2ItemTp =
        TupleType(Map("n" -> IntType))

      val nestedItemTp = TupleType(Map(
        "m" -> StringType,
        "n" -> IntType,
        "k" -> BagType(nested2ItemTp)
      ))

      val itemTp = TupleType(Map(
        "h" -> IntType,
        "j" -> BagType(nestedItemTp)
      ))

      val relationR = Relation("R", List(
        Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Milos",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Michael",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
                Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Jaclyn",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 69,
          "j" -> List(
            Map(
              "m" -> "Thomas",
              "n" -> 987,
              "k" -> List(
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 987)
              )
            )
          )
        )
      ), BagType(itemTp))

      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)

      val wdef = VarDef("w", nestedItemTp)
      val wref = TupleVarRef(wdef)

      val q1 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "o5" -> Project(xref, "h"),
          "o6" ->
            ForeachUnion(wdef, Project(xref, "j").asInstanceOf[BagExpr],
              Singleton(Tuple(
                "o7" -> Project(wref, "m"),
                "o8" -> Mult(
                  Tuple("n" -> Project(wref, "n")),
                  Project(wref, "k").asInstanceOf[BagExpr]
                )
              ))
            )
        )))

      println("Q1: " + q1.quote)
      println("Q1 eval: " + q1.eval)
      println("Shredded Q1: " + q1.shred.quote)
    }
  }

  object ExampleShredValue {

    def run(): Unit = {
      val nested2ItemTp =
        TupleType(Map("n" -> IntType))

      val nestedItemTp = TupleType(Map(
        "m" -> StringType,
        "n" -> IntType,
        "k" -> BagType(nested2ItemTp)
      ))

      val itemTp = TupleType(Map(
        "h" -> IntType,
        "j" -> BagType(nestedItemTp)
      ))

      val relationR = List(
        Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Milos",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Michael",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
                Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Jaclyn",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Joe",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Alice",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
                Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Bob",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 69,
          "j" -> List(
            Map(
              "m" -> "Thomas",
              "n" -> 987,
              "k" -> List(
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 987)
              )
            )
          )
        )
      )

      val shredR = Shredder.shredValue(relationR, BagType(itemTp))

      println(Printer.quote(relationR, BagType(itemTp)))
      println(shredR.quote)

      val unshredR = Shredder.unshredValue(shredR)
      println(Printer.quote(unshredR, BagType(itemTp)))

      println("Same as original: " + relationR.equals(unshredR))
    }
  }

  Example1.run()
  Example2.run()

  ExampleShredValue.run()
}
