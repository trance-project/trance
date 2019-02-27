package shredding.nrc2

sealed trait VarDef { def n: String; def tp: Type }
case class PrimitiveVarDef(n: String, tp: PrimitiveType) extends VarDef
case class BagVarDef(n: String, tp: BagType) extends VarDef
case class TupleVarDef(n: String, tp: TupleType) extends VarDef

object VarDef {
  def apply(n: String, tp: Type): VarDef = tp match {
    case IntType => PrimitiveVarDef(n, IntType)
    case StringType => PrimitiveVarDef(n, StringType)
    case t: BagType => BagVarDef(n, t)
    case t: TupleType => TupleVarDef(n, t)
    case _ => throw new IllegalArgumentException("cannot create VarDef")
  }
}

sealed trait Expr { def tp: Type }
trait AttributeExpr extends Expr { def tp: AttributeType }
trait PrimitiveExpr extends AttributeExpr { def tp: PrimitiveType }
trait BagExpr extends AttributeExpr { def tp: BagType }
trait TupleExpr extends Expr { def tp: TupleType }

/**
  * NRC constructs
  */
case class Const(v: String, tp: PrimitiveType) extends PrimitiveExpr

trait VarRef extends Expr {
  def n: String
  def field: Option[String]
  def tp: Type
}
trait AttributeVarRef extends AttributeExpr with VarRef { def tp: AttributeType }
case class PrimitiveVarRef(n: String, field: Option[String], tp: PrimitiveType) extends PrimitiveExpr with AttributeVarRef
case class BagVarRef(n: String, field: Option[String], tp: BagType) extends BagExpr with AttributeVarRef
case class TupleVarRef(n: String, field: Option[String], tp: TupleType) extends TupleExpr with VarRef

object VarRef {
  def apply(d: VarDef): VarRef = d match {
    case v: PrimitiveVarDef => PrimitiveVarRef(v.n, None, v.tp)
    case v: BagVarDef => BagVarRef(v.n, None, v.tp)
    case v: TupleVarDef => TupleVarRef(v.n, None, v.tp)
    case _ => throw new IllegalArgumentException(s"cannot create VarRef(${d.n})")
  }

  def apply(d: VarDef, f: String): AttributeVarRef = d match {
    case v: TupleVarDef => v.tp.tps(f) match {
      case IntType => PrimitiveVarRef(v.n, Some(f), IntType)
      case StringType => PrimitiveVarRef(v.n, Some(f), StringType)
      case t: BagType => BagVarRef(v.n, Some(f), t)
    }
    case _ => throw new IllegalArgumentException(s"cannot create VarRef(${d.n}, $f)")
  }
}

case class ForeachUnion(x: TupleVarDef, e1: BagExpr, e2: BagExpr) extends BagExpr {
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

case class Tuple(fields: Map[String, AttributeExpr]) extends TupleExpr {
  val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
}

object Tuple {
  def apply(fs: (String, AttributeExpr)*): Tuple = Tuple(Map(fs: _*))
}

case class Let(x: VarDef, e1: Expr, e2: Expr) extends Expr {
  assert(x.tp == e1.tp)

  val tp: Type = e2.tp
}

case class Mult(e1: TupleExpr, e2: BagExpr) extends PrimitiveExpr {
  assert(e1.tp == e2.tp.tp)

  val tp: PrimitiveType = IntType
}

case class Cond(op: OpCmp, e1: AttributeExpr, e2: AttributeExpr)

case class IfThenElse(cond: Cond, e1: BagExpr, e2: Option[BagExpr] = None) extends BagExpr {
  assert(e2.isEmpty || e1.tp == e2.get.tp)

  val tp: BagType = e1.tp
}

case class PhysicalBag(tp: BagType, values: List[TupleExpr]) extends BagExpr

object PhysicalBag {
  def apply(itemTp: TupleType, values: TupleExpr*): PhysicalBag =
    PhysicalBag(BagType(itemTp), List(values: _*))
}

case class Relation(n: String, b: PhysicalBag) extends BagExpr {
  val tp: BagType = b.tp
}

case class Label(vars: List[VarRef], flat: BagExpr) extends BagExpr {
  val tp: BagType = flat.tp
}


/**
  * Pretty printer
  */
object Printer {

  import shredding.Utils.ind

  def quote(e: Expr): String = e match {
    case Const(v, StringType) => "\""+ v +"\""
    case Const(v, _) => v
    case v: VarRef => v.n + v.field.map("." + _).mkString
    case ForeachUnion(x, e1, e2) =>
      s"""|For ${x.n} in ${quote(e1)} Union
          |${ind(quote(e2))}""".stripMargin
    case Union(e1, e2) =>
      s"(${quote(e1)}) Union (${quote(e2)})"
    case Singleton(e1) =>
      "sng(" + quote(e1) + ")"
    case Tuple(fields) =>
      s"( ${fields.map(f => f._1 + " := " + quote(f._2)).mkString(", ")} )"
    case Let(x, e1, e2) =>
      s"""|Let ${x.n} = ${quote(e1)} In
          |${ind(quote(e2))}""".stripMargin
    case Mult(e1, e2) =>
      s"Mult(${quote(e1)}, ${quote(e2)})"
    case IfThenElse(Cond(op, l, r), e1, None) =>
      s"""|If (${quote(l)} $op ${quote(r)})
          |Then ${quote(e1)}""".stripMargin
    case IfThenElse(Cond(op, l, r), e1, Some(e2)) =>
      s"""|If (${quote(l)} $op ${quote(r)})
          |Then ${quote(e1)}
          |Else ${quote(e2)}""".stripMargin
    case PhysicalBag(_, vs) => "[ " + vs.mkString(", ") + " ]"
    case Relation(n, _) => n
    case Label(vs, fe) =>
      "Label(vars := " + vs.map(quote).mkString(", ") + ", flat := " + quote(fe) + ")"
    case _ => throw new IllegalArgumentException("unknown type")
  }
}

/**
  * Simple Scala evaluator
  */
object Evaluator {

  val ctx = collection.mutable.HashMap[String, Any]()

  def eval(e: Expr): Any = e match {
    case Const(v, tp) => tp match {
      case IntType => v.toInt
      case StringType => v
    }
    case v: VarRef => v.field match {
      case Some(f) => ctx(v.n).asInstanceOf[Map[String, _]](f)
      case None => ctx(v.n)
    }
    case ForeachUnion(x, e1, e2) =>
      val r1 = eval(e1).asInstanceOf[List[_]]
      val r = r1.flatMap { x1 => ctx(x.n) = x1; eval(e2).asInstanceOf[List[_]] }
      ctx.remove(x.n)
      r
    case Union(e1, e2) =>
      eval(e1).asInstanceOf[List[_]] ++ eval(e2).asInstanceOf[List[_]]
    case Singleton(e1) => List(eval(e1))
    case Tuple(fs) =>
      fs.map(x => x._1 -> eval(x._2))
    case Let(x, e1, e2) =>
      ctx(x.n) = eval(e1)
      val r = eval(e2)
      ctx.remove(x.n)
      r
    case Mult(e1, e2) =>
      val ev1 = eval(e1)
      eval(e2).asInstanceOf[List[_]].count(_ == ev1)
    case IfThenElse(Cond(op, l, r), e1, None) =>
      val el = eval(l)
      val er = eval(r)
      op match {
        case OpEq => if (el == er) eval(e1) else Nil
        case OpNe => if (el != er) eval(e1) else Nil
      }
    case IfThenElse(Cond(op, l, r), e1, Some(e2)) =>
      val el = eval(l)
      val er = eval(r)
      op match {
        case OpEq => if (el == er) eval(e1) else eval(e2)
        case OpNe => if (el != er) eval(e1) else eval(e2)
      }
    case PhysicalBag(_, v) => v.map(eval)
    case Relation(_, b) => eval(b)
    case _ => throw new IllegalArgumentException("unknown type")
  }
}

object Shredder {

//  trait Dict {
//    def isEmpty: Boolean
//    def flat: BagExpr
////    def dict: Dict
//  }
//
//  case object EmptyDict extends Dict {
//    def isEmpty = true
//    def flat: Nothing = throw new NoSuchElementException("flat of empty dictionary")
////    def dict: Nothing = throw new NoSuchElementException("dict of empty dictionary")
//  }
//
//  case class BagDict(flat: BagExpr) extends Dict {
//    def isEmpty = false
//  }
//  case class Dict(flat: BagExpr)


  def shred(e: Expr): Expr = e match {

    case Const(_, _) => e

    case _: VarRef => e

    case ForeachUnion(x, e1, e2) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      val l1 = shred(e1).asInstanceOf[Label]
      val l2 = shred(e2).asInstanceOf[Label]
      val sx = TupleVarDef(x.n, l1.flat.tp.tp)
      Label(ivars, ForeachUnion(sx, l1.flat, l2.flat))

    case Union(e1, e2) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      val l1 = shred(e1).asInstanceOf[Label]
      val l2 = shred(e2).asInstanceOf[Label]
      Label(ivars, Union(l1.flat, l2.flat))

    case Singleton(e1) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      Label(ivars, Singleton(shred(e1).asInstanceOf[TupleExpr]))

    case Tuple(fs) =>
      Tuple(fs.map(f => f._1 -> shred(f._2).asInstanceOf[AttributeExpr]))

    case Let(x, e1, e2) =>
      sys.error("TODO")

    case Mult(e1, e2) =>
      Mult(shred(e1).asInstanceOf[TupleExpr], shred(e2).asInstanceOf[Label])

    case IfThenElse(c, e1, None) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      Label(ivars, IfThenElse(c, shred(e1).asInstanceOf[BagExpr], None))

    case IfThenElse(c, e1, Some(e2)) =>
      // compute ivars
      val ivars: List[VarRef] = Nil
      Label(ivars, IfThenElse(c, shred(e1).asInstanceOf[BagExpr], Some(shred(e2).asInstanceOf[BagExpr])))

    case PhysicalBag(_, _) => Label(Nil, e.asInstanceOf[PhysicalBag])

    case Relation(_, _) => Label(Nil, e.asInstanceOf[Relation])

    case _ => sys.error("not implemented")

  }
}

object TestApp extends App {

  object Example1 {

    def run(): Unit = {

      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val x = TupleVarDef("x", itemTp)
      val relationR = Relation("R", PhysicalBag(itemTp,
        Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType)),
        Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType)),
        Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType)),
        Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType))
      ))

      val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> VarRef(x, "b"))))

      println(Printer.quote(q1))
      println(Evaluator.eval(q1))

      val y = TupleVarDef("y", itemTp)
      val q2 = ForeachUnion(x, relationR,
        Singleton(Tuple(
          "grp" -> VarRef(x, "a"),
          "bag" -> ForeachUnion(y, relationR,
            IfThenElse(
              Cond(OpEq, VarRef(x, "a"), VarRef(y, "a")),
              Singleton(Tuple("q" -> VarRef(y, "b")))
            ))
        )))

      println(Printer.quote(q2))
      println(Evaluator.eval(q2))

      println(Printer.quote(Shredder.shred(q2)))

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

      val relationR = Relation("R", PhysicalBag(itemTp,
        Tuple(
          "h" -> Const("42", IntType),
          "j" -> PhysicalBag(nestedItemTp,
            Tuple(
              "m" -> Const("Milos", StringType),
              "n" -> Const("123", IntType),
              "k" -> PhysicalBag(nested2ItemTp,
                Tuple("n" -> Const("123", IntType)),
                Tuple("n" -> Const("456", IntType)),
                Tuple("n" -> Const("789", IntType)),
                Tuple("n" -> Const("123", IntType))
              )
            ),
            Tuple(
              "m" -> Const("Michael", StringType),
              "n" -> Const("7", IntType),
              "k" -> PhysicalBag(nested2ItemTp,
                Tuple("n" -> Const("2", IntType)),
                Tuple("n" -> Const("9", IntType)),
                Tuple("n" -> Const("1", IntType))
              )
            ),
            Tuple(
              "m" -> Const("Jaclyn", StringType),
              "n" -> Const("12", IntType),
              "k" -> PhysicalBag(nested2ItemTp,
                Tuple("n" -> Const("14", IntType)),
                Tuple("n" -> Const("12", IntType))
              )
            )
          )
        ),
        Tuple(
          "h" -> Const("69", IntType),
          "j" -> PhysicalBag(nestedItemTp,
            Tuple(
              "m" -> Const("Thomas", StringType),
              "n" -> Const("987", IntType),
              "k" -> PhysicalBag(nested2ItemTp,
                Tuple("n" -> Const("987", IntType)),
                Tuple("n" -> Const("654", IntType)),
                Tuple("n" -> Const("987", IntType)),
                Tuple("n" -> Const("654", IntType)),
                Tuple("n" -> Const("987", IntType)),
                Tuple("n" -> Const("987", IntType))
              )
            )
          )
        )
      ))

      val x = TupleVarDef("x", itemTp)
      val w = TupleVarDef("w", nestedItemTp)

      val q1 = ForeachUnion(x, relationR,
        Singleton(Tuple(
          "o5" -> VarRef(x, "h"),
          "o6" ->
            ForeachUnion(w, VarRef(x, "j").asInstanceOf[BagExpr],
              Singleton(Tuple(
                "o7" -> VarRef(w, "m"),
                "o8" -> Mult(
                  Tuple("n" -> VarRef(w, "n")),
                  VarRef(w, "k").asInstanceOf[BagExpr]
                )
              ))
            )
        )))

      println(Printer.quote(q1))
      println(Evaluator.eval(q1))

//      println(Printer.quote(Shredder.shred(q1)))

    }
  }

  Example1.run()
  Example2.run()
}
