package shredding.nrc2

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

  case class VarDef(name: String, tp: Type)

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

  case class Project(tuple: TupleExpr, field: String) extends TupleAttributeExpr {
    val tp: TupleAttributeType = tuple.tp.attrs(field)
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

  /**
    * Shredding NRC extension
    */
  case class NewLabel(free: Map[String, VarRef]) extends LabelExpr {
    val tp: LabelType = LabelType(free.map(f => f._1 -> f._2.tp.asInstanceOf[LabelAttributeType]))

//    def resolve(dict: BagDict): BagExpr = dict.flat
  }

  case class Lookup(lbl: LabelExpr, dict: BagDict) extends BagExpr {
    def tp: BagType = dict.tp
  }


  /**
    * Runtime extensions
    */
  trait RuntimeExpr extends Expr

//  case class RValue(v: Any, tp: PrimitiveType) extends PrimitiveExpr with RuntimeExpr

  case class RBag(values: List[Any], tp: BagType) extends BagExpr with RuntimeExpr

//  case class RTuple(tp: TupleType, values: Map[String, Any]) extends TupleExpr with RuntimeExpr

//  case class RLabel(values: Map[String, Any], tp: LabelType) extends LabelExpr with RuntimeExpr

  object RLabelId {
    private var currId = 0

    implicit def orderingById: Ordering[RLabelId] = Ordering.by(e => e.id)
  }

  case class RLabelId(tp: BagType) extends BagExpr with RuntimeExpr {
    val id: Int = { RLabelId.currId += 1; RLabelId.currId }

    override def equals(that: Any): Boolean = that match {
      case that: RLabelId => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

    override def toString: String = "RLabelId(" + id + ")"
  }




  case class Relation(n: String, tp: BagType, b: List[Any]) extends BagExpr with RuntimeExpr






  //  case class PhysicalBag(tp: BagType, values: List[TupleExpr]) extends BagExpr
  //
  //  object PhysicalBag {
  //    def apply(itemTp: TupleType, values: TupleExpr*): PhysicalBag =
  //      PhysicalBag(BagType(itemTp), List(values: _*))
  //  }


  implicit def liftString(x: String): PrimitiveExpr = Const(x, StringType)

  implicit def liftInt(x: Int): PrimitiveExpr = Const(x, IntType)

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
        val tupleDict = sl.map(_.dict).reduce(_.union(_)).asInstanceOf[TupleDict]
        // Create a runtime label
        val lbl = RLabelId(BagType(flatTp(tp2).asInstanceOf[TupleType]))
        val matDict = MaterializedDict(Map(lbl -> flatBag), tupleDict, lbl.tp)
        ShredValue(lbl, flatTp(tp), matDict)

      case TupleType(as) =>
        val m = v.asInstanceOf[Map[String, Any]]
        val sm = m.map { case (n, a) => n -> shredValue(a, as(n)) }
        val flat = sm.map { case (n, s) => n -> s.flat }
        val dict = TupleDict(sm.map { case (n, s) => n -> s.dict.asInstanceOf[AttributeDict] })
        ShredValue(flat, flatTp(tp), dict)

      case _ => sys.error("shredValue with unknown type of " + tp)
    }

    def unshredValue(flat: Any, flatTp: Type, dict: Dict): Any = flatTp match {
      case _: PrimitiveType => flat

      case _: LabelType =>
        val lbl = flat.asInstanceOf[RLabelId]
        val matDict = dict.asInstanceOf[MaterializedDict]
        matDict.f(lbl).map(t => unshredValue(t, lbl.tp.tp, matDict.dict))

      case TupleType(as) =>
        val tuple = flat.asInstanceOf[Map[String, Any]]
        val tupleDict = dict.asInstanceOf[TupleDict]
        as.map { case (n, tp) => n -> unshredValue(tuple(n), tp, tupleDict.fields(n)) }

      case _ => sys.error("unshredValue with unknown type of " + flatTp)
    }

    def unshredValue(s: ShredValue): Any = unshredValue(s.flat, s.flatTp, s.dict)




    def apply(e: Expr): ShredExpr = shred(e, Map.empty)

    def ivars(e: Expr): Map[String, VarRef] =
      // TODO: compute ivars
      Map.empty

    private def shred(e: Expr, ctx: Map[String, ShredExpr]): ShredExpr = e match {
      case Const(_, _) => ShredExpr(e, EmptyDict)

      case Project(t, f) =>
        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(t, ctx)
        ShredExpr(Project(flat, f), dict.fields(f))

      case v: VarRef => ctx(v.name)

      case ForeachUnion(x, e1, e2) =>
        val ShredExpr(_: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val xdef = VarDef(x.name, dict1.flat.tp.tp)
        val ShredExpr(_: LabelExpr, dict2: BagDict) =
          shred(e2, ctx + (x.name -> ShredExpr(VarRef(xdef), dict1.dict)))
        val lbl = NewLabel(ivars(e))
        ShredExpr(lbl, BagDict(lbl, ForeachUnion(xdef, dict1.flat, dict2.flat), dict2.dict))

      case Union(e1, e2) =>
        val ShredExpr(_: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val ShredExpr(_: LabelExpr, dict2: BagDict) = shred(e2, ctx)
        val dict = dict1.union(dict2)
        ShredExpr(dict.lbl, dict)

      case Singleton(e1) =>
        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(e1, ctx)
        val lbl = NewLabel(ivars(e1))
        ShredExpr(lbl, BagDict(lbl, Singleton(flat), dict))

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
        val ShredExpr(_: LabelExpr, dict2: BagDict) = shred(e2, ctx)
        ShredExpr(Mult(flat1, dict2.flat), EmptyDict)

      case IfThenElse(c, e1, None) =>
        val ShredExpr(_: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val lbl = NewLabel(ivars(e1))
        ShredExpr(lbl, BagDict(lbl, IfThenElse(c, dict1.flat, None), dict1.dict))

      case IfThenElse(c, e1, Some(e2)) =>
        sys.error("TODO")

//      case PhysicalBag(tp, vs) =>
//        // TODO: shredding input relations
////        val svs = vs.map(v => shred(v, ctx))
////        val dict = svs.map(_.dict).reduce(_ union _)
////        ShredExpr(PhysicalBag(tp, svs.map(_.flat.asInstanceOf[TupleExpr])), dict)
//
//        ShredExpr(e, TupleDict(tp.tp.attrs.map(f => f._1 -> PrimitiveDict)))

      case r: Relation =>
        // TODO: shredding input relations
        val lbl = NewLabel(Map.empty)
        val dict = TupleDict(r.tp.tp.attrs.map(a => a._1 -> EmptyDict))
        ShredExpr(lbl, BagDict(lbl, r, dict))

      case _ => sys.error("not implemented")

    }

  }

}


object TestApp extends App {

  import NRCImplicits._

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = Relation("R", BagType(itemTp),
        List(
          Map("a" -> 42, "b" -> "Milos"),
          Map("a" -> 69, "b" -> "Michael"),
          Map("a" -> 34, "b" -> "Jaclyn"),
          Map("a" -> 42, "b" -> "Thomas")
        ))
//      val relationR = Relation("R", PhysicalBag(itemTp,
//        Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType)),
//        Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType)),
//        Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType)),
//        Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType))
//      ))

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

//  object Example2 extends NRCTransforms {
//
//    def run(): Unit = {
//
//      val nested2ItemTp =
//        TupleType(Map("n" -> IntType))
//
//      val nestedItemTp = TupleType(Map(
//        "m" -> StringType,
//        "n" -> IntType,
//        "k" -> BagType(nested2ItemTp)
//      ))
//
//      val itemTp = TupleType(Map(
//        "h" -> IntType,
//        "j" -> BagType(nestedItemTp)
//      ))
//
//      val relationR = Relation("R", PhysicalBag(itemTp,
//        Tuple(
//          "h" -> Const("42", IntType),
//          "j" -> PhysicalBag(nestedItemTp,
//            Tuple(
//              "m" -> Const("Milos", StringType),
//              "n" -> Const("123", IntType),
//              "k" -> PhysicalBag(nested2ItemTp,
//                Tuple("n" -> Const("123", IntType)),
//                Tuple("n" -> Const("456", IntType)),
//                Tuple("n" -> Const("789", IntType)),
//                Tuple("n" -> Const("123", IntType))
//              )
//            ),
//            Tuple(
//              "m" -> Const("Michael", StringType),
//              "n" -> Const("7", IntType),
//              "k" -> PhysicalBag(nested2ItemTp,
//                Tuple("n" -> Const("2", IntType)),
//                Tuple("n" -> Const("9", IntType)),
//                Tuple("n" -> Const("1", IntType))
//              )
//            ),
//            Tuple(
//              "m" -> Const("Jaclyn", StringType),
//              "n" -> Const("12", IntType),
//              "k" -> PhysicalBag(nested2ItemTp,
//                Tuple("n" -> Const("14", IntType)),
//                Tuple("n" -> Const("12", IntType))
//              )
//            )
//          )
//        ),
//        Tuple(
//          "h" -> Const("69", IntType),
//          "j" -> PhysicalBag(nestedItemTp,
//            Tuple(
//              "m" -> Const("Thomas", StringType),
//              "n" -> Const("987", IntType),
//              "k" -> PhysicalBag(nested2ItemTp,
//                Tuple("n" -> Const("987", IntType)),
//                Tuple("n" -> Const("654", IntType)),
//                Tuple("n" -> Const("987", IntType)),
//                Tuple("n" -> Const("654", IntType)),
//                Tuple("n" -> Const("987", IntType)),
//                Tuple("n" -> Const("987", IntType))
//              )
//            )
//          )
//        )
//      ))
//
//      val x = VarDef("x", itemTp)
//      val w = VarDef("w", nestedItemTp)
//
//      val q1 = ForeachUnion(x, relationR,
//        Singleton(Tuple(
//          "o5" -> Project(x, "h"),
//          "o6" ->
//            ForeachUnion(w, Project(x, "j").asInstanceOf[BagExpr],
//              Singleton(Tuple(
//                "o7" -> Project(w, "m"),
//                "o8" -> Mult(
//                  Tuple("n" -> Project(w, "n")),
//                  Project(w, "k").asInstanceOf[BagExpr]
//                )
//              ))
//            )
//        )))
//
//      println(Printer.quote(q1))
//      println(Evaluator.eval(q1))
//
////      println(Printer.quote(Shredder.shred(q1)))
//
//    }
//  }
//
  object Example3 {

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

//  Example1.run()
//  Example2.run()

  Example3.run()
}
