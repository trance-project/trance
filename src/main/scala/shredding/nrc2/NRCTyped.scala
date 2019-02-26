//package shredding.nrc2
//
//import shredding.Utils.ind
//import reflect.runtime.universe.{ Symbol => _, _ }
//
//trait NRCTyped {
//
//  type TTuple1[A] = Tuple1[A]
//  type TTuple2[A, B] = Tuple2[A, B]
//  type TTuple3[A, B, C] = Tuple3[A, B, C]
//  type TBag[A] = List[A]
//
//  /**
//    * Expr[A] is the type of expressions that evaluate to a value of type A
//    */
//  abstract class Expr[A]
//
//  /**
//    * A statically known constant of type T (e.g., String and Int)
//    */
//  case class Const[A](x: A) extends Expr[A]
//
//  /**
//    * A Sym is a symbolic reference used internally to refer to expressions.
//    */
//  object Sym { private var currId = 0 }
//  case class Sym[A](x: Symbol, id: Int = { Sym.currId += 1; Sym.currId }) extends Expr[A]
//
//  /**
//    * NRC constructs
//    */
//  case class ForeachUnion[A, B](x: Sym[A], e1: Expr[TBag[A]], e2: Expr[TBag[B]]) extends Expr[TBag[B]]
//
//  case class Union[A](e1: Expr[TBag[A]], e2: Expr[TBag[A]]) extends Expr[TBag[A]]
//
//  case class Singleton[A](e: Expr[A]) extends Expr[TBag[A]]
//
//  case class TupleStruct1[A](e1: Expr[A]) extends Expr[TTuple1[A]]
//
//  case class TupleStruct2[A, B](e1: Expr[A], e2: Expr[B]) extends Expr[TTuple2[A, B]]
//
//  case class TupleStruct3[A, B, C](e1: Expr[A], e2: Expr[B], e3: Expr[C]) extends Expr[TTuple3[A, B, C]]
//
//  case class Project[A, B](target: Expr[A], pos: Int) extends Expr[B]
//
//  case class Relation[A](r: Sym[A], b: TBag[A]) extends Expr[TBag[A]]
//
//  case class Mult[A](e1: Expr[A], e2: Expr[TBag[A]]) extends Expr[Int]
//
//
//  /**
//    * Extension methods for NRC expressions
//    */
//  implicit class TraversalOps[A: TypeTag](e: Expr[A]) {
//
//    def collect[B](f: PartialFunction[Expr[_], List[B]]): List[B] =
//      f.applyOrElse(e, (ex: Expr[_]) => ex match {
//        case ForeachUnion(_, e1, e2) => e1.collect(f) ++ e2.collect(f)
//        case Union(e1, e2) => e1.collect(f) ++ e2.collect(f)
//        case Singleton(e1) => e1.collect(f)
//        case TupleStruct1(e1) => e1.collect(f)
//        case TupleStruct2(e1, e2) => e1.collect(f) ++ e2.collect(f)
//        case TupleStruct3(e1, e2, e3) => e1.collect(f) ++ e2.collect(f) ++ e3.collect(f)
//        case Project(e1, _) => e1.collect(f)
//        case Mult(e1, e2) => e1.collect(f) ++ e2.collect(f)
//        case _ => List()
//      })
//
//    def containsRelation: Boolean =
//      e.collect { case Relation(_, _) => List(true) }.exists(x => x)
//
//    def isBagType: Boolean = typeOf[A] <:< typeOf[TBag[_]]
//
//    def isTupleType: Boolean =
//      typeOf[A] <:< typeOf[TTuple1[_]] ||
//        typeOf[A] <:< typeOf[TTuple2[_, _]] ||
//        typeOf[A] <:< typeOf[TTuple3[_, _, _]]
//
//    def inputVars: List[Sym[_]] = ivars()
//
//    private[TraversalOps] def ivars(scope: List[Sym[_]] = Nil): List[Sym[_]] = collect {
//      case ForeachUnion(x, e1, e2) => e1.ivars(scope) ++ e2.ivars(x :: scope)
//      case Union(e1, e2) => e1.ivars(scope) ++ e2.ivars(scope)
//      case Singleton(e1) => e1.ivars(scope)
//      case TupleStruct1(e1) => e1.ivars(scope)
//      case TupleStruct2(e1, e2) => e1.ivars(scope) ++ e2.ivars(scope)
//      case TupleStruct3(e1, e2, e3) => e1.ivars(scope) ++ e2.ivars(scope) ++ e3.ivars(scope)
//      case Project(e1, _) => e1.ivars(scope)
//      case Mult(e1, e2) => e1.ivars(scope) ++ e2.ivars(scope)
//      case _: Relation[_] => Nil
//      case s: Sym[A] => if (scope.map(_.x).contains(s.x)) Nil else List(s)
//      case _ => sys.error("Unhandled case in inputVars: " + e)
//    }
//  }
//}
//
//trait NRCTypedEmbedding extends NRCTyped {
//
//  /**
//    * Automatically lift basic types into their DSL representation
//    */
//  implicit def liftString(x: String): Expr[String] = Const(x)
//
//  implicit def liftInt(x: Int): Expr[Int] = Const(x)
//
//  implicit def liftDouble(x: Double): Expr[Double] = Const(x)
//
//  implicit def liftSymbol[A](x: Symbol): Sym[A] = Sym[A](x)
//
//  /**
//    * DSL constructors
//    */
//  implicit def bagOps[A](self: Expr[TBag[A]]) = new {
//    def ForeachUnion[B](f: Expr[A] => Expr[TBag[B]]): Expr[TBag[B]] = {
//      val x = Sym[A]('x)
//      new ForeachUnion(x, self, f(x))
//    }
//
//    def ForeachYield[B](f: Expr[A] => Expr[B]): Expr[TBag[B]] =
//      ForeachUnion((x: Expr[A]) => Singleton(f(x)))
//
//    def Union(rhs: Expr[TBag[A]]): Expr[TBag[A]] =
//      new Union(self, rhs)
//  }
//
//  implicit def flattenOp[A](self: Expr[TBag[TBag[A]]]) = new {
//    def Flatten: Expr[TBag[A]] = self.ForeachUnion(e => e)
//  }
//
//  implicit def projectOps1[A](self: Expr[TTuple1[A]]) = new {
//    def Project1: Expr[A] = Project(self, 1)
//  }
//
//  implicit def projectOps2[A, B](self: Expr[TTuple2[A, B]]) = new {
//    def Project1: Expr[A] = Project(self, 1)
//    def Project2: Expr[B] = Project(self, 2)
//  }
//
//  implicit def projectOps3[A, B, C](self: Expr[TTuple3[A, B, C]]) = new {
//    def Project1: Expr[A] = Project(self, 1)
//    def Project2: Expr[B] = Project(self, 2)
//    def Project3: Expr[C] = Project(self, 3)
//  }
//
//  implicit def multOp[A](self: Expr[A]) = new {
//    def Mult(b: Expr[TBag[A]]): Expr[Int] = new Mult(self, b)
//  }
//}
//
//trait NRCTypedTransform extends NRCTyped {
//
//  /**
//    * Pretty printer
//    */
//  object Printer {
//    def quote[A](e: Expr[A]): String = e match {
//      case ForeachUnion(x, e1, e2) =>
//        s"""|For ${quote(x)} in ${quote(e1)} Union
//            |${ind(quote(e2))}""".stripMargin
//      case Union(e1, e2) => s"(${quote(e1)}) Union (${quote(e2)})"
//      case Singleton(e1) => "sng(" + quote(e1) + ")"
//      case TupleStruct1(e1) => quote(e1)
//      case TupleStruct2(e1, e2) => s"( ${quote(e1)}, ${quote(e2)} )"
//      case TupleStruct3(e1, e2, e3) => s"( ${quote(e1)}, ${quote(e2)}, ${quote(e3)} )"
//      case Project(e1, pos) => quote(e1) + "._" + pos
//      case Mult(e1, e2) => s"Mult(${quote(e1)}, ${quote(e2)})"
//      case Relation(n, _) => quote(n)
//      case Const(s: String) => "\""+ s +"\""
//      case Const(c) => c.toString
//      case Sym(x, id) => x.name + id
//      case _ => "<unknown>"
//    }
//  }
//
//  /**
//    * Simple Scala evaluator
//    */
//  object Evaluator {
//    val ctx = collection.mutable.HashMap[Sym[_], Any]()
//
//    def eval[A](e: Expr[A]): A = e match {
//      case ForeachUnion(x, e1, e2) =>
//        val r = eval(e1).flatMap { x1 => ctx(x) = x1; eval(e2) }.asInstanceOf[A]
//        ctx.remove(x)
//        r
//      case Union(e1, e2) => eval(e1) ++ eval(e2)
//      case Singleton(e1) => List(eval(e1))
//      case TupleStruct1(e1) => Tuple1(eval(e1))
//      case TupleStruct2(e1, e2) => Tuple2(eval(e1), eval(e2))
//      case TupleStruct3(e1, e2, e3) => Tuple3(eval(e1), eval(e2), eval(e3))
//      case Project(e1, pos) =>
//        eval(e1).asInstanceOf[Product].productElement(pos-1).asInstanceOf[A]
//      case Mult(e1, e2) =>
//        val ev1 = eval(e1)
//        eval(e2).filter(_ == ev1).size
//      case Relation(_, c) => c
//      case Const(c) => c
//      case s @ Sym(_, _) => ctx(s).asInstanceOf[A]
//      case _ => sys.error("not implemented")
//    }
//  }
//
//  object Shredder {
//
//    case class Label[A](vars: List[Sym[_]])
//
//    case class ShredExpr[A](lbl: Option[Label[_]], flat: Expr[A], dict: List[ShredExpr[_]]) {
//      override def toString: String =
//        "(" + lbl.map("(" + _.vars.mkString(", ") + ") => ").mkString + flat + ", " + dict + "))"
//    }
//
//    def shred[A : TypeTag](e0: Expr[A]): ShredExpr[_] = e0 match {
//      case ForeachUnion(x, e1, e2) =>
//        val s1 = shred(e1)
//        val s2 = shred(e2)
//        val f1 = s1.flat.asInstanceOf[Expr[TBag[Any]]]
//        val f2 = s2.flat.asInstanceOf[Expr[TBag[Any]]]
//        ShredExpr(Some(Label(e0.inputVars)), ForeachUnion(x, f1, f2), s1.dict ++ s2.dict)
//
//      case Union(e1, e2) =>
//        val s1 = shred(e1)
//        val s2 = shred(e2)
//        val f1 = s1.flat.asInstanceOf[Expr[TBag[Any]]]
//        val f2 = s2.flat.asInstanceOf[Expr[TBag[Any]]]
//        ShredExpr(Some(Label(e0.inputVars)), Union(f1, f2), s1.dict ++ s2.dict)
//
//      case Singleton(e1) =>
//        val s1 = shred(e1)
//        ShredExpr(Some(Label(e0.inputVars)), Singleton(s1.flat), s1.dict)
//
//      case TupleStruct1(e1) =>
//        val s1 = shred(e1)
//        ShredExpr(None, TupleStruct1(s1.flat), s1.dict)
//
//      case TupleStruct2(e1, e2) =>
//        val s1 = shred(e1)
//        val s2 = shred(e2)
//        ShredExpr(None, TupleStruct2(s1.flat, s2.flat), s1.dict ++ s2.dict)
//
//      case TupleStruct3(e1, e2, e3) =>
//        val s1 = shred(e1)
//        val s2 = shred(e2)
//        val s3 = shred(e3)
//        ShredExpr(None, TupleStruct3(s1.flat, s2.flat, s3.flat), s1.dict ++ s2.dict ++ s3.dict)
//
//      case Project(e1, pos) =>
//        val s1 = shred(e1)
//        ShredExpr(None, Project(s1.flat, pos), s1.dict)
//
//      case r @ Relation(_, _) => shredRelation(r)
//
//      case Mult(e1, e2) =>
//        val s1 = shred(e1)
//        val s2 = shred(e2)
//        val f1 = s1.flat.asInstanceOf[Expr[Any]]
//        val f2 = s2.flat.asInstanceOf[Expr[TBag[Any]]]
//        assert(s1.dict.isEmpty)
//        ShredExpr(None, Mult(f1, f2), s2.dict)
//
//      case Sym(x, id) => ShredExpr(None, Sym(x, id), Nil)
//
//      case Const(c) => ShredExpr(None, Const(c), Nil)
//
//      case _ => sys.error("not supported")
//    }
//
//    def shredRelation[A : TypeTag](r: Relation[A]) = {
//      val lbl = Some(Label(Nil))
//
////      r.b match {
////        case _ if typeOf[A] <:< typeOf[TBag[TTuple1[_]]] =>
////          val x = r.b.asInstanceOf[TBag[TTuple1[_]]]
////
////
////      }
//
//      // TODO: Implement shredding of input relation
//      // TODO: fix dictionary
//      ShredExpr(lbl, r, Nil)
//    }
//  }
//
//
//
//  /**
//    * Shredding transformation: insert lambdas on every bag type
//    */
////  object Shredder2 {
////
////    def shred[A](e0: Expr[A]): Expr[A] = e0 match {
////      case e @ ForeachUnion(x, e1, e2) =>
////        val ivars = e.inputVars()
////        val ivars1 = e1.inputVars()
////        val ivars2 = e2.inputVars()
////        assert(ivars1.toSet.subsetOf(ivars.toSet) &&
////          ivars2.toSet.subsetOf(ivars.toSet))
////
////        assert(e1.isBagType && e2.isBagType)
////
////        val shred1 = shred(e1).asInstanceOf[Lambda[TBag[Any]]]
////        val shred2 = shred(e2).asInstanceOf[Lambda[A]]
////
////        Lambda(ivars, ForeachUnion(x, Apply(shred1, ivars1), Apply(shred2, ivars2)))
////
////      case e @ Union(e1, e2) =>
////        val ivars = e.inputVars()
////        val ivars1 = e1.inputVars()
////        val ivars2 = e2.inputVars()
////        assert(ivars1.toSet.subsetOf(ivars.toSet) &&
////          ivars2.toSet.subsetOf(ivars.toSet))
////
////        assert(e1.isBagType && e2.isBagType)
////
////        val shred1 = shred(e1).asInstanceOf[Lambda[A]]
////        val shred2 = shred(e2).asInstanceOf[Lambda[A]]
////
////        Lambda(ivars, Union(Apply(shred1, ivars1), Apply(shred2, ivars2)))
////
//////      case Singleton(e1) =>
//////        val ivars = e1.inputVars()
//////        Lambda(ivars,
//////          Singleton(shredSingleton(e1))
////      //      case TupleStruct1(e1) =>
////      //        TupleStruct1(shred(e1))
////      //      case TupleStruct2(e1, e2) =>
////      //        TupleStruct2(shred(e1), shred(e2))
////      //      case TupleStruct3(e1, e2, e3) =>
////      //        TupleStruct3(shred(e1), shred(e2), shred(e3))
////      //      case Project(e1, pos) =>
////      //        Project(shred(e1), pos)
////      //      case r @ Relation(_, _) => shredRelation(r)
////      //      case Const(_) | Sym(_, _) => e
////      case _ => sys.error("not supported")
////    }
////  }
//
//
//  //  /**
////    * Shredding transformation:
////    * the structure of a given expression remains unchanged,
////    * only input-dependent inner bags are replaced by labels
////    */
////  object Shredder {
////    def shred[A](e: Expr[A]): Expr[A] = e match {
////      case ForeachUnion(x, e1, e2) =>
////        ForeachUnion(x, shred(e1), shred(e2))
////      case Union(e1, e2) =>
////        Union(shred(e1), shred(e2))
////      case Singleton(e1) =>
////        Singleton(shredSingleton(e1))
////      case TupleStruct1(e1) =>
////        TupleStruct1(shred(e1))
////      case TupleStruct2(e1, e2) =>
////        TupleStruct2(shred(e1), shred(e2))
////      case TupleStruct3(e1, e2, e3) =>
////        TupleStruct3(shred(e1), shred(e2), shred(e3))
////      case Project(e1, pos) =>
////        Project(shred(e1), pos)
////      case r @ Relation(_, _) => shredRelation(r)
////      case Const(_) | Sym(_, _) => e
////      case _ => sys.error("not supported")
////    }
////
////    def shredSingleton[A](e: Expr[A]): Expr[A] = e match {
////      case TupleStruct1(e1) =>
////        TupleStruct1(
////          wrapNewLabel(shred(e1))
////        )
////      case TupleStruct2(e1, e2) =>
////        TupleStruct2(
////          wrapNewLabel(shred(e1)),
////          wrapNewLabel(shred(e2))
////        )
////      case TupleStruct3(e1, e2, e3) =>
////        TupleStruct3(
////          wrapNewLabel(shred(e1)),
////          wrapNewLabel(shred(e2)),
////          wrapNewLabel(shred(e3))
////        )
////      case _ => shred(e)
////    }
////
////    def wrapNewLabel[A: TypeTag](e: Expr[A]): Expr[A] =
////      if (isShreddable(e)) Label(Sym[A]('l), e) else e
////
////    def isShreddable[A: TypeTag](e: Expr[A]): Boolean =
////      e.containsRelation    // shred or not
////
////    def shredRelation[A](r: Relation[A]) = {
////      // TODO: shredding of input relations
////      r
////    }
////  }
//}
//
//object NRCTest extends App {
//
//  object Example extends NRCTypedEmbedding with NRCTypedTransform {
//
//    type Item = TTuple3[Int, String, Double]
//
//    def run(): Unit = {
//
//      val r = Relation[Item]('R,
//        List[Item]((1, "Oxford", 4.33), (2, "Edinburgh", 1.33))
//      )
//      val rr = Relation[TBag[Item]]('RR,
//        List(
//          List[Item]((14, "Dummy", -4.2)),
//          List[Item]((23, "Foo", 12), (45, "Bar", 2.4))
//        ))
//
//      val q = r.ForeachUnion(e => Singleton(e))
//
//      val q2 = q.Union(rr.Flatten)
//
//      val q3 = rr.ForeachUnion(e1 =>
//        e1.ForeachUnion(e2 =>
//          Singleton(
//            TupleStruct2(e2.Project1, e1))))
//
//      val q4 = r.ForeachYield(e => e)
//
//      println(Printer.quote(q))
//      println(Printer.quote(q2))
//      println(Printer.quote(q3))
//      println(Printer.quote(q4))
//
////      println(q3.vars)
////      println(Evaluator.eval(q))
////      println(Evaluator.eval(rr))
////      println(Evaluator.eval(q2))
////      println(rr)
////      println(Evaluator.eval(q3))
////
////      println(Printer.quote(Shredder.shred(q)))
////      println(Printer.quote(Shredder.shred(q3)))
//
//      val q5 = r.ForeachUnion(e1 =>
//        Singleton(
//          TupleStruct2(
//            e1.Project1,
//            r.ForeachUnion(e2 => Singleton(e2))
//          )
//        )
//      )
//
//      println(Printer.quote(q5))
////      println(Printer.quote(Shredder.shred(q5)))
//    }
//  }
//
//  object Example2 extends NRCTypedEmbedding with NRCTypedTransform {
//
//    type Item = TTuple1[TBag[TTuple2[Int, TBag[TTuple1[Int]]]]]
//
//    def run(): Unit = {
//
//      val r = Relation[Item]('R, Nil)
//
//      val q = r.ForeachUnion(x =>
//        x.Project1.ForeachUnion(y =>
//          Singleton(
//            TupleStruct2(
//              y.Project1,
//              y.Project2.ForeachUnion(z =>
//                Singleton(z.Project1))))))
//
//      println(Printer.quote(q))
//      println("Input vars: " + q.inputVars.mkString(", "))
//    }
//  }
//
//  object Example3 extends NRCTypedEmbedding with NRCTypedTransform {
//
//    type Item = TTuple2[Int, TBag[TTuple3[Int, Int, TBag[Int]]]]
//
//    def run(): Unit = {
//
//      val r = Relation[Item]('R,
//        List[Item](
//          (1, List((11, 12, List(123, 126)), (22, 23, List(223, 225)))),
//          (9, List((99, 92, List(911, 933)), (88, 883, List(883, 812, 883, 825))))
//        )
//      )
//
//      val q = r.ForeachUnion(x =>
//        Singleton(
//          TupleStruct2(
//            x.Project1,
//            x.Project2.ForeachUnion(w =>
//              Singleton(
//                TupleStruct2(
//                  w.Project1,
//                  w.Project2.Mult(w.Project3)
//                )
//              )
//            )
//          )
//        )
//      )
//
//      println(Printer.quote(q))
//      println("Input vars: " + q.inputVars.mkString(", "))
//
//      println(Evaluator.eval(q))
//
//    }
//  }
//
//  object Example4 extends NRCTypedEmbedding with NRCTypedTransform {
//
//    type Item = TTuple3[Int, String, Double]
//
//    def run(): Unit = {
//
//      val r = Relation[Item]('R, List[Item]((43, "AD", 3.1), (12, "FG", -12.2), (312, "XY", 2.3)))
//
//      val q1 = r.ForeachUnion(x =>
//        Singleton(TupleStruct2(x.Project1, x.Project3))
//      )
//
//      println(Printer.quote(q1))
//
//      val sq1 = Shredder.shred(q1)
//      println(sq1)
//    }
//
//  }
//
//
////  Example.run()
////  Example2.run()
////  Example3.run()
//  Example4.run()
//}