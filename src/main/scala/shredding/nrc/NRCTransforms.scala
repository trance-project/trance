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
      case ForeachDomain(x, e1, e2) =>
        s"""|For ${quote(x)} in ${quote(e1)} Union
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
      case Label(l, e1) => s"${quote(l)} where ${quote(l)} -> ${e1.map{e => quote(e)}.mkString(",")}"
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

    /**
      * Evaluate all the queries in the query set
      * Note that input relations still are not shredded 
      * so this is really only evaluating the top query
      */
    def evalQueries(qs: collection.mutable.ListMap[Sym[_], Expr[_]]) = {
      qs.foreach(q => {
        println(q._1+" "+Printer.quote(q._2))
        println(eval(q._2))
      })
    }

    /**
      * Evaluation of a query. Since label handling still needs some work, 
      * this is only working correctly on the top-level query. 
      * Bug in IfThenElse
      */
    def eval[A](e: Expr[A]): A = {
      e match {
        case ForeachUnion(x, e1, e2) =>
          val r = eval(e1).flatMap { x1 => ctx(x) = x1; eval(e2) }.asInstanceOf[A]
          //ctx.remove(x)
          r
        case ForeachMapunion(x, e1, e2) =>
          val r = eval(e1).flatMap { x1 => ctx(x) = x1; eval(e2) }.asInstanceOf[A]
          //ctx.remove(x)
          r
        case Mapunion(e1, e2) => eval(e1) ++ eval(e2)
        case MapStruct(e1, e2) => e1 match {
          case Label(s,v) => { // maintain label
            //ctx(s) = v.map{v2 => eval(v2)}
            collection.mutable.Map(e1 -> eval(e2))     
          } 
        }
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
        case Relation(r, c) => ctx(r) = c; c
        case Const(c) => c
        case l @ Label(s,v) => // create a new label based on the actual values of this label
          val nl = Sym[Any]('s)
          ctx(nl) = v.map{v2 => eval(v2)}; ctx(s) = ctx(nl); ShredLabel(nl, ctx(nl)).asInstanceOf[A]
        case l @ ShredLabel(s,v) => s.asInstanceOf[A]
        case s @ Sym(_, _) => ctx.getOrElse(s, s).asInstanceOf[A]
        case _ => sys.error("not implemented")
      }
    }

  }

  /**
    * Shredding transformation:
    * implementation of TransformQueryBag and TransformQueryBagAux
    * there is still no shredding of input relations
    */
  object Shredder {
    
    // set of queries with FIFO access 
    // either want to pass a simliar object through to each of the functions 
    // or change shredder to a class so that this is not shared for all Shredder calls
    val queries = collection.mutable.ListMap[Sym[_], Expr[_]]()

    def reset = queries.clear
    
    /**
      * generateShredQueries wraps shredQueryBag in order to properly update
      * the query collection (queries). Shredding expression (E) produces query
      * (Q) through a recursive shredding process of subexpressions (E_n).
      */
    def generateShredQueries[A](e: Expr[TBag[A]]): collection.mutable.ListMap[Sym[_], Expr[_]] = {
      queries(Sym('q)) = shredQueryBag(e)
      queries
    }
    
    /**
      * shredQueryBag (ie. TransformQueryBag from shredalg)
      * input a expression of bag type and tracked free variables for 
      * each of the levels and produce the shredded representation of the 
      * query. Singleton marks the final "same level expression" of the input expression.
      * A key-value pair is produced based on the tracked freevars (key), 
      * and shredSingleton (TransformQuerySingleton(Aux)) (value)
      */ 
    def shredQueryBag[A,B](e: Expr[TBag[A]], fs: List[Expr[_]] = List()): Expr[TMap[Label[A], TBag[A]]] = {
      val freevars = fs ++ e.asInstanceOf[Expr[Any]].freevars
      e match {
        // e1 is a bag so shredQueryBag(e1) should actually happen 
        // but no shredding of input relations yet so holding off on that
        // this also would not support For x in (For y in R Union sng(y._1)) Union sng(x._1)
        case ForeachUnion(x, e1, e2) => 
          ForeachMapunion(x, shredQueryElement(e1), shredQueryBag(e2, freevars))
        case Union(e1, e2) => 
          Mapunion(shredQueryBag(e1, freevars), shredQueryBag(e2, freevars))
        case Singleton(e1) => 
          val frees = fs.filterNot(e.asInstanceOf[Expr[Any]].freevars.toSet)
          MapStruct(Label(Sym('s), frees.distinct), Singleton(shredSingleton(e1)))
        case IfThenElse(e1,e2,e3) => 
          IfThenElse(shredQueryElement(e1), shredQueryBag(e2, freevars), shredQueryBag(e3, freevars))
        case _ => sys.error("not supported")
      }
    }

    /**
      * shredQueryElement (ie. TransformQueryTuple, TransformQueryElement, etc)
      * Note that a tuple with a bag will be handled by wrapNewLabel in shredSingleton
      */
    def shredQueryElement[A](e: Expr[A]): Expr[A] = e match {
      case TupleStruct1(e1) =>
        TupleStruct1(shredQueryElement(e1))
      case TupleStruct2(e1, e2) =>
        TupleStruct2(shredQueryElement(e1), shredQueryElement(e2))
      case TupleStruct3(e1, e2, e3) =>
        TupleStruct3(shredQueryElement(e1), shredQueryElement(e2), shredQueryElement(e3))
      case Project(e1, pos) => 
        Project(shredQueryElement(e1), pos)
      case Eq(e1, e2) => Eq(shredQueryElement(e1), shredQueryElement(e2))
      case And(e1, e2) => And(shredQueryElement(e1), shredQueryElement(e2))
      case r @ Relation(_, _) => shredRelation(r)
      case Const(_) | Sym(_, _) => e
      case _ => sys.error("not supported")
    }

    /** 
      * Shredding of a singleton is the final same-level expression
      * in the current expression. Expressions inside of a singleton
      * are only of tuple type. wrapNewLabel is called on each element
      * to either produce a label or return a const, sym, etc.
      */
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

    /**
      * wrapNewLabel generates a label for a Bag type inside a tuple 
      * subexpressions that are not same level are shredded
      * and added to the query set
      */
    def wrapNewLabel[A: TypeTag](e: Expr[A]): Expr[A] = { 
      if (isShreddable(e)) {
        val s = Sym[A]('l)
        // for we in domain of previous query
        val sq = shredQueryBag(e.asInstanceOf[Expr[TBag[A]]])
        queries(s) = sq
        Label(s, e.freevars.distinct)         
      }else{
         shredQueryElement(e)
      }
    }

    /**
      * not using isBagType here since A is interpretted as Any
      * and e.isBagType returns false for Any 
      */
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
      // TODO
      r 
    }
  }
}
