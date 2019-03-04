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

  trait LabelExpr extends Expr {
    def tp: Type = LabelType
  }

  case class VarDef(name: String, tp: Type)

  trait BaseVarRef extends Expr {
    def varDef: VarDef
  }

}

trait NRC extends BaseExpr with Dictionary {

  /**
    * NRC constructs
    */
  case class Const(v: String, tp: PrimitiveType) extends PrimitiveExpr

  case class VarRef(varDef: VarDef) extends BaseVarRef {
    def tp: Type = varDef.tp

    def name: String = varDef.name
  }

  case class Project(varDef: VarDef, field: String) extends BaseVarRef with TupleAttributeExpr {
    val tp: TupleAttributeType = varDef.tp match {
      case TupleType(attrs) => attrs(field)
      case _ => throw new IllegalArgumentException("failed Project()")
    }
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

  case class Relation(n: String, tp: BagType, b: List[Any]) extends BagExpr

  //  case class PhysicalBag(tp: BagType, values: List[TupleExpr]) extends BagExpr
  //
  //  object PhysicalBag {
  //    def apply(itemTp: TupleType, values: TupleExpr*): PhysicalBag =
  //      PhysicalBag(BagType(itemTp), List(values: _*))
  //  }

  /**
    * Shredding NRC extension
    */
  case class NewLabel(free: List[BaseVarRef]) extends LabelExpr

  case class Lookup(lbl: NewLabel, dict: BagDict) extends BagExpr {
    def tp: BagType = dict.flat.tp
  }

}

trait ShredNRCTransforms extends NRCTransforms {
  this: NRC =>

  /**
    * Shredding transformation
    */
  object Shredder {

    case class ShredExpr(flat: Expr, dict: Dict) {
      def quote: String =
        s"""|Flat: ${Printer.quote(flat)}
            |Dict: $dict
            |""".stripMargin

    }

    def flatTp(tp: Type): Type = tp match {
      case t: PrimitiveType => t
      case _: BagType => LabelType
      case TupleType(as) => TupleType(as.map(a => a._1 -> flatTp(a._2).asInstanceOf[TupleAttributeType]))
      case _ => throw new IllegalArgumentException("unknown flat type")
    }

    def apply(e: Expr): ShredExpr = shred(e, Map.empty)

    private def shred(e: Expr, ctx: Map[String, ShredExpr]): ShredExpr = e match {
      case Const(_, _) => ShredExpr(e, EmptyDict)
      case VarRef(v) => ctx(v.name)
      case Project(v, f) =>
        val ShredExpr(flat: Tuple, dict: TupleDict) = ctx(v.name)
        ShredExpr(flat.fields(f), dict.fields(f))
      case ForeachUnion(x, e1, e2) =>
        // TODO: compute ivars
        val ivars: List[BaseVarRef] = Nil

        val ShredExpr(_: NewLabel, dict1: BagDict) = shred(e1, ctx)
        val x1 = VarDef(x.name, dict1.flat.tp.tp)
        val s1 = ShredExpr(VarRef(x1), dict1.dict)
        val ShredExpr(_: NewLabel, dict2: BagDict) = shred(e2, ctx + (x.name -> s1))
        val lbl = NewLabel(ivars)
        ShredExpr(lbl, BagDict(lbl, ForeachUnion(x1, dict1.flat, dict2.flat), dict2.dict))

      case Union(e1, e2) =>
        val ShredExpr(_: NewLabel, dict1: BagDict) = shred(e1, ctx)
        val ShredExpr(_: NewLabel, dict2: BagDict) = shred(e2, ctx)
        val dict = dict1.union(dict2)
        ShredExpr(dict.lbl, dict)

      case Singleton(e1) =>
        // compute ivars
        val ivars: List[BaseVarRef] = Nil

        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(e1, ctx)
        val lbl = NewLabel(ivars)
        ShredExpr(lbl, BagDict(lbl, Singleton(flat), dict))

      case Tuple(fs) =>
        val sfs = fs.map(f => f._1 -> shred(f._2, ctx))
        ShredExpr(
          Tuple(sfs.map(f => f._1 -> f._2.flat.asInstanceOf[TupleAttributeExpr])),
          TupleDict(sfs.map(f => f._1 -> f._2.dict.asInstanceOf[AttributeDict]))
        )

      case Let(x: VarDef, e1, e2) =>
        val se1 = shred(e1, ctx)
        val sx = VarDef(x.name, flatTp(x.tp))
        val se2 = shred(e2, ctx + (x.name -> ShredExpr(VarRef(sx), se1.dict)))
        ShredExpr(Let(x, se1.flat, se2.flat), se2.dict)

      case Mult(e1, e2) =>
        val ShredExpr(flat1: TupleExpr, _: Dict) = shred(e1, ctx)    // dict1 is empty
      val ShredExpr(_: NewLabel, dict2: BagDict) = shred(e2, ctx)
        ShredExpr(Mult(flat1, dict2.flat), EmptyDict)

      case IfThenElse(c, e1, None) =>
        // TODO: compute ivars
        val ivars: List[BaseVarRef] = Nil

        val ShredExpr(_: NewLabel, dict1: BagDict) = shred(e1, ctx)
        val lbl = NewLabel(ivars)
        ShredExpr(lbl, BagDict(lbl, IfThenElse(c, dict1.flat, None), dict1.dict))

      case IfThenElse(c, e1, Some(e2)) =>
        sys.error("TODO")
      //      // compute ivars
      //      val ivars: List[VarRef] = Nil
      //      Label(ivars, IfThenElse(c, shred(e1).asInstanceOf[BagExpr], Some(shred(e2).asInstanceOf[BagExpr])))

      //        case PhysicalBag(tp, vs) =>
      //          // TODO: shredding input relations
      //  //        val svs = vs.map(v => shred(v, ctx))
      //  //        val dict = svs.map(_.dict).reduce(_ union _)
      //  //        ShredExpr(PhysicalBag(tp, svs.map(_.flat.asInstanceOf[TupleExpr])), dict)
      //
      //          ShredExpr(e, TupleDict(tp.tp.attrs.map(f => f._1 -> PrimitiveDict)))

      case r: Relation =>
        val lbl = NewLabel(Nil)
        ShredExpr(lbl, BagDict(lbl, r, null))

      //          // TODO: shredding input relations
      //          val ShredExpr(flat: PhysicalBag, dict: TupleDict) = shred(b, ctx)
      //
      //          val lbl = Label(Nil)
      //          ShredExpr(lbl, BagDict(lbl, Relation(n, flat.asInstanceOf[PhysicalBag]), dict))

      case _ => sys.error("not implemented")

    }
  }

}


object TestApp extends App {

  import NRCImplicits._

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val x = VarDef("x", itemTp)
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

      val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> Project(x, "b"))))

      println(q1.quote)
      println(q1.eval)

      val y = VarDef("y", itemTp)
      val q2 = ForeachUnion(x, relationR,
        Singleton(Tuple(
          "grp" -> Project(x, "a"),
          "bag" -> ForeachUnion(y, relationR,
            IfThenElse(
              Cond(OpEq, Project(x, "a"), Project(y, "a")),
              Singleton(Tuple("q" -> Project(y, "b")))
            ))
        )))

      println(q2.quote)
      println(q2.eval)

//      println(Shredder(q2).quote)

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
  Example1.run()
////  Example2.run()
}
