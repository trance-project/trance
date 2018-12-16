package shredding.nrc

import shredding.Utils.ind
import reflect.runtime.universe.{ Symbol => _, _ }

/**
  * Transformations for NRC expressions 
  * printing, evaluation, and shredding
  */

trait NRCTransforms extends NRCExprs with EmbedNRC {

  /**
    * Pretty printer
    */
  object Printer {
    def quote[A](e: Expr[A]): String = e match {
      case ForeachUnion(x, e1, e2) =>
        s"""|For ${quote(x)} in ${quote(e1)} Union
            |${ind(quote(e2))}""".stripMargin
      case ForeachMapunion(x, e1, e2) =>
        s"""|For ${quote(x)} in ${quote(e1)} Mapunion 
            |${ind(quote(e2))}""".stripMargin
      case Union(e1, e2) => s"(${quote(e1)}) Union (${quote(e2)})"
      case Mapunion(e1, e2) => s"(${quote(e1)}) Mapunion (${quote(e2)})"
      case Singleton(e1) => "sng(" + quote(e1) + ")"
      case MapStruct(k, v) => s"${quote(k)} -> ${quote(v)}"
      case TupleStruct1(e1) => quote(e1)
      case TupleStruct2(e1, e2) => s"( ${quote(e1)}, ${quote(e2)} )"
      case TupleStruct3(e1, e2, e3) => s"( ${quote(e1)}, ${quote(e2)}, ${quote(e3)} )"
      case Project(e1, pos) => quote(e1) + "._" + pos
      case Relation(n, _) => quote(n)
      case ShredRelation(n, _) => quote(n)
      case Const(s: String) => "\""+ s +"\""
      case Const(c) => c.toString
      case Sym(x, id) => x.name + id
      case Label(l, e1) => s"${quote(l)}" //where ${quote(l)} -> ${quote(e1)}"
      case Eq(e1, e2) => s"${quote(e1)} = ${quote(e2)}"
      case And(e1, e2) => s"${quote(e1)} and ${quote(e2)}"
      case IfThenElse(e1, e2, e3) => s"if ${quote(e1)} then ${quote(e2)} else ${quote(e3)}"
      case _ => "<unknown>"
    }

  }

  /**
    * Simple Scala evaluator
    */
  object Evaluator {
    val ctx = collection.mutable.HashMap[Sym[_], Any]()

    def eval[A](e: Expr[A]): A = e match {
      case ForeachUnion(x, e1, e2) =>
        val r = eval(e1).flatMap { x1 => ctx(x) = x1; eval(e2) }.asInstanceOf[A]
        ctx.remove(x)
        r
      case Union(e1, e2) => eval(e1) ++ eval(e2)
      case Singleton(e1) => List(eval(e1))
      case TupleStruct1(e1) => Tuple1(eval(e1))
      case TupleStruct2(e1, e2) => Tuple2(eval(e1), eval(e2))
      case TupleStruct3(e1, e2, e3) => Tuple3(eval(e1), eval(e2), eval(e3))
      case Project(e1, pos) =>
        eval(e1).asInstanceOf[Product].productElement(pos-1).asInstanceOf[A]
      case Eq(e1, e2) => eval(e1) == eval(e2)
      case And(e1, e2) => eval(e1) && eval(e2)
      case IfThenElse(e1, e2, e3) => 
        if (eval(e1)) eval(e2) else eval(e3)
      case Relation(_, c) => c
      case Const(c) => c
      case s @ Sym(_, _) => ctx(s).asInstanceOf[A]
      case _ => sys.error("not implemented")
    }
  }

  /**
    * Shredding transformation:
    * the structure of a given expression remains unchanged,
    * only input-dependent inner bags are replaced by labels
    */
  object Shredder {
    
    val ctx = collection.mutable.HashMap[Sym[_], Expr[_]]()

    def reset = ctx.clear
     
    // shredding of output queries
    def shred[A](e: Expr[A]): Expr[A] = e match {
      case ForeachUnion(x, e1, e2) =>
        ForeachUnion(x, shred(e1), shred(e2))
      case Union(e1, e2) =>
        Union(shred(e1), shred(e2))
      case Singleton(e1) =>
        Singleton(shredSingleton(e1))
      case TupleStruct1(e1) =>
        TupleStruct1(shred(e1))
      case TupleStruct2(e1, e2) =>
        TupleStruct2(shred(e1), shred(e2))
      case TupleStruct3(e1, e2, e3) =>
        TupleStruct3(shred(e1), shred(e2), shred(e3))
      case Project(e1, pos) =>
        Project(shred(e1), pos)
      case Eq(e1, e2) => Eq(shred(e1), shred(e2))
      case And(e1, e2) => And(shred(e1), shred(e2))
      case IfThenElse(e1, e2, e3) => 
        IfThenElse(shred(e1), shred(e2), shred(e3))
      case r @ Relation(_, _) => shredRelation(r)
      case Const(_) | Sym(_, _) => e
      case _ => sys.error("not supported")
    }

    def shredQueryBag[A](e: Expr[TBag[A]]): Expr[TMap[Label[A], TBag[A]]] = e match {
      case ForeachUnion(x, e1, e2) => ForeachMapunion(x, e1, shredQueryBag(e2))
      case Union(e1, e2) => Mapunion(shredQueryBag(e1), shredQueryBag(e2))
      case Singleton(e1) => {
        val s = shredSingleton(e1)
        MapStruct(Label(Sym('s), s), Singleton(s))
      }
      case IfThenElse(e1,e2,e3) => IfThenElse(e1, shredQueryBag(e2), shredQueryBag(e3))
      case _ => sys.error("not supported")
    }


    // TransformQuerySingleton
    def shredSingleton[A](e: Expr[A]): Expr[A] = e match {
      case TupleStruct1(e1) =>
        TupleStruct1(
          wrapNewLabel(e1)
        )
      case TupleStruct2(e1, e2) =>
        TupleStruct2(
          wrapNewLabel(e1),
          wrapNewLabel(e2)
        )
      case TupleStruct3(e1, e2, e3) =>
        TupleStruct3(
          wrapNewLabel(e1),
          wrapNewLabel(e2),
          wrapNewLabel(e3)
        )
      // bags should only be made up of tuples
      case _ => sys.error("not supported")
    }

    // TransformQueryTupleElement
    def wrapNewLabel[A: TypeTag](e: Expr[A]): Expr[A] = { 
      if (isShreddable(e)) {
        val s = Sym[A]('l)
        ctx(s) = shredQueryBag(e.asInstanceOf[Expr[TBag[A]]])
        Label(s, e) 
      }else{
         e
      }
    }

    // isBagType on the output of shred returns false negative due to Any type
    def isShreddable[A: TypeTag](e: Expr[A]): Boolean = {
      e match {
        case ForeachUnion(x, e1, e2) => isShreddable(e2)
        case Singleton(e1) => true
        case IfThenElse(e1,e2,e3) => true
        case Union(e1,e2) => true
        case Relation(r,b) => true
        case _ => false 
      }
    }

    // shredding of input relations
    def shredRelation[A](r: Relation[A]) = {
      r
    }
  }
}
