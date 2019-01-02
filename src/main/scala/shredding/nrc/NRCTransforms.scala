package shredding.nrc

import shredding.Utils._
import reflect.runtime.universe.{ Symbol => _, _ }

import scala.collection.mutable.{HashMap,Map,SortedMap}

/**
  * Transformations for NRC Expressions
  */

trait NRCTransforms extends EmbedNRC {

  /**
    * Pretty printer
    */
  object Printer {
    def quote[A](e: Expr[A]): String = e match {
      case ForeachUnion(x, e1 @ Sym(_,_), e2 @ ForeachUnion(y, e3, e4)) =>
        val sub = s"""For ${quote(y)} in extractFromLabel_${quote(e3)}(${quote(x)}) Union 
                      |${ind(quote(e4))}""".stripMargin
        s"""|For ${quote(x)} in ${quote(e1)} Union
            |${ind(sub)}""".stripMargin
      case ForeachUnion(x, e1, e2) =>
        s"""|For ${quote(x)} in ${quote(e1)} Union
            |${ind(quote(e2))}""".stripMargin
      case Union(e1, e2) => s"(${quote(e1)}) Union (${quote(e2)})"
      case Singleton(e1) => "sng(" + quote(e1) + ")"
      case EmptySet() => "sng()"
      case TupleStruct1(e1) => quote(e1)
      case TupleStruct2(e1, e2) => s"( ${quote(e1)}, ${quote(e2)} )"
      case TupleStruct3(e1, e2, e3) => s"( ${quote(e1)}, ${quote(e2)}, ${quote(e3)} )"
      case Project(e1, pos) => quote(e1) + "._" + pos
      case Relation(n, s) => quote(n)//s"${quote(n)} ${s}"
      case Const(s: String) => "\""+ s +"\""
      case Const(c) => c.toString
      case Sym(x, id) => x.name + id
      case Label(l, e1) => s"${quote(l)} where (${quote(l)} -> ${e1.mkString(",")})"
      case Eq(e1, e2) => s"${quote(e1)} = ${quote(e2)}"
      case And(e1, e2) => s"${quote(e1)} and ${quote(e2)}"
      case IfThenElse(e1, e2, e3) => s"if ${quote(e1)} then ${quote(e2)} else ${quote(e3)}"
      case _ => "<unknown>"
    }


    /**
      * Print helper functions for writing out shredded queries, and 
      * evaluation of shredded queries.
      */ 

    def printQueries(qs: scala.collection.mutable.SortedMap[Sym[_],Expr[_]]) = {
      qs.foreach(q => {
        println(q._1+": "+quote(q._2))
      })
    }

    def printOutputs(qs: collection.mutable.HashMap[Sym[_],Any]) = {
      qs.foreach(q => {
        println(q._1+":")
        q._2.asInstanceOf[List[Any]].foreach(println)
      })
    }

  }

  /**
    * Simple Scala evaluator
    */
  object Evaluator {

    val ctx = HashMap[Sym[_], Any]()
    val reset = ctx.clear

    /**
      * Initial implementation of extractFromLabel_e1(label) 
      * given an expression and a label, extract the value associated
      * with the the expression in the label. Note that for now a label
      * holds a list of key-value pairs 
      */
    def extractFromLabel(e1: Expr[_], label: Label[_]) = { 
      label.e.toMap.getOrElse(e1,None).asInstanceOf[List[Any]]
    }

    def extractFromLabelAll(l: Label[_]) = {
      l.e.map{ case (v2, empty) =>  
        val v3 = eval(v2.asInstanceOf[Expr[_]])
          v3 match {
             case l2 @ Label(s1,v1) => (v2, v1)
             case _ => (v2, v3)
           }}
    }

    /**
      * Evaluate all the queries in the set of shredded queries (output of Shredder.generateShredQueries)
      */
    def evalQueries(qs: SortedMap[Sym[_],Expr[_]]): HashMap[Sym[_],Any] = {
      val shredded_outputs = HashMap[Sym[_], Any]()
      qs.foreach({ i => 
        shredded_outputs += (i._1 -> eval(i._2))
        reset       
      })
      shredded_outputs
    }

    /**
      * Evaluation of a query. In the current shredding process, labels hold the values of the nested 
      * elements, so no extraction from a shredded dict is necessary. This will change when shredRelation
      * does the full shredding process (see Shredder.shredRelation).
      *
      * There are currently two cases for ForeachUnion:
      *    i) iterating elements of the flat shred relation (top level records)
      *    ii) iterating labels from the domain of a parent expression, and extracting
      *        a free-variable from the label. 
      */
    def eval[A](e: Expr[A]): A = {
      e match {
        // for we in domain union
        //   for y in extractFromLabel_fv(we)
        case ForeachUnion(x, e1 @ Sym(_,_), e2 @ ForeachUnion(y, e3, e4)) =>
          val domain = eval(e1).map{ s => eval(s.asInstanceOf[Sym[A]]) }
          val r = flatten(domain).flatMap{ we =>
            ctx(x) = we
            extractFromLabel(e3, we.asInstanceOf[Label[_]]).map{ y1 =>
              ctx(y) = y1
              eval(e4)
            }
          }.asInstanceOf[A]
          r
        case ForeachUnion(x, e1, e2) => 
          val r = eval(e1).flatMap{ x1 => 
            ctx(x) = x1;
            eval(e2) 
          }.asInstanceOf[A]
          r
        case Union(e1, e2) => eval(e1) ++ eval(e2)
        case Singleton(e1) => List(eval(e1))
        case EmptySet() => List()
        case TupleStruct1(e1) => Tuple1(eval(e1))
        case TupleStruct2(e1, e2) => Tuple2(eval(e1), eval(e2))
        case TupleStruct3(e1, e2, e3) => Tuple3(eval(e1), eval(e2), eval(e3))
        case Project(e1, pos) =>
          eval(e1).asInstanceOf[Product].productElement(pos-1).asInstanceOf[A]
        case Eq(e1, e2) => eval(e1) == eval(e2)
        case And(e1, e2) => eval(e1) && eval(e2)
        case IfThenElse(e1, e2, e3) => 
          if (eval(e1)) eval(e2) else eval(e3)
        case Relation(r, c) => 
          ctx(r) = c; c
        case Const(c) => c
        case l @ Label(s,v) => {// create a new label based on evaluated values
          val nl = Label(Sym('s), extractFromLabelAll(l))
          ctx(nl.l) = nl.e
          // update domain
          try { 
            ctx(s) = ctx(s).asInstanceOf[List[(Any,Any)]] ++ List(nl) 
          } catch { 
            case e:Exception => ctx(s) = List(nl)
          }
          nl.asInstanceOf[A]
        }
        case s @ Sym(_, _) => // return symbol if no value has been associated
          ctx.getOrElse(s,s).asInstanceOf[A]
        case _ => sys.error("not implemented")
      }
    }

  }

  /**
    * Shredding transformation:
    * implementation of TransformQueryBag and TransformQueryBagAux
    */
  object Shredder {

    // the query stack holds the next (non-same level) subexpression to be 
    // evaluated by the shredding procedure
    var exprs = scala.collection.mutable.Stack[Expr[_]]()
    implicit val ord = Sym.orderingById
    
    /**
      * generateShredQueries class shredQueryBag on subexpressions in the stack until there
      * are no more subexpressions to shred.
      */
    def generateShredQueries[A](e: Expr[A]): SortedMap[Sym[_],Expr[_]] = {
      
      val shredset = SortedMap[Sym[_], Expr[_]]()
      
      exprs.push(e)      
      
      var qsym = Sym[Any]('Q)
      var dsym = Sym[Any]('D)
      
      while (!exprs.isEmpty) {
        var shredq = shredQueryBag(exprs.pop, dsym)
        shredset += (qsym -> shredq)
        
        // produce domain query if necessary
        dsym = Sym[Any]('D)
        var domain = shredq.asInstanceOf[Expr[Any]].domain
        if (!domain.isEmpty){
          // relaxing singleton tuple requirement here for cleaner evaluation
          var d = Relation(dsym, domain).ForeachUnion(l => Singleton(l))
          shredset += (dsym -> d)
          qsym = Sym[Any]('Q)
        }
      }
      shredset
    }
    
    /**
      * shredQueryBag (ie. TransformQueryBag from shredalg)
      * input expression of bag type, a set of labels from previous query (domain),
      * and a set of free variables (tracked from previous same-level expression)
      * Singleton marks the final "same level expression" of the input expression.
      * A key-value pair is produced based on the tracked freevars (key), 
      * and shredSingleton (TransformQuerySingleton(Aux)) (value)
      * Currently, key-value pairs are of type Tuple2[Label, Expr], can move
      * to a map later.
      */ 
    def shredQueryBag[A](e: Expr[A], domain: Sym[_], fs: List[Expr[_]] = List()): Expr[A] = {
      
      //val fvars = fs ++ e.asInstanceOf[Expr[Any]].freevars
      e match {
        // for x in shred(relation) union e2
        case ForeachUnion(x, e1 @ Relation(_,_), e2) =>
          val r = shredQueryBag(e1, domain)
          ForeachUnion(x, r, shredQueryBag(e2, domain, fs ++ r.freevars))
        // for we in domain
        //    for y in extract(e1, we) shred(e2)
        case ForeachUnion(x, e1, e2) =>
          val we = Sym[Any]('we)
          ForeachUnion(we, domain.asInstanceOf[Expr[TBag[Any]]],
            // we passed to freevars should be an extract from all  
            ForeachUnion(x, e1, shredQueryBag(e2, domain, List(domain, we))))
        case Union(e1, e2) => 
          Union(shredQueryBag(e1, domain, fs), shredQueryBag(e2, domain, fs))
        case r @ Relation(_, _) => shredRelation(r)
        case Singleton(e1) =>
          Singleton(TupleStruct2(Label(Sym('s), fs.map{v => (v, None)}), 
            Singleton(shredSingleton(e1))))
        case EmptySet() => EmptySet()
        case IfThenElse(e1,e2,e3) => 
          IfThenElse(shredQueryElement(e1), shredQueryBag(e2, domain, fs), shredQueryBag(e3, domain, fs))
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
        exprs.push(e)
        Label(Sym[A]('q), e.freevars.distinct.map{v => (v, None)})         
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
        case ForeachUnion(x, e1, e2) => true
        case Singleton(e1) => true
        case EmptySet() => true
        case IfThenElse(e1,e2,e3) => isShreddable(e2) && isShreddable(e3)
        case Union(e1,e2) => true
        case Relation(r,b) => true
        case _ => false 
      }
    }

    def createLabel[A](v: A, rdict: Map[Label[_], Any]) = v match {
      case head :: tail =>
        val l = Label(Sym('l), v.asInstanceOf[List[(Any, Any)]])
        rdict += (l -> v.asInstanceOf[List[(Any,Any)]])
        l
      case _ => v
    }

    /**
      * For now, this shreds the top relation, and stores the 
      * nested element in the label, which is then accessed during evaluation
      */
    def shredRelation[A](r: Relation[A]) = {
      val rdict = Map[Label[_], Any]()
      val rflat = r.b.map{ r2 => r2 match {
          case Tuple3(e1,e2,e3) => Tuple3(createLabel(e1, rdict), createLabel(e2, rdict), createLabel(e3, rdict))
          case Tuple2(e1,e2) => Tuple2(createLabel(e1, rdict), createLabel(e2, rdict))
          case Tuple1(e1) => Tuple1(createLabel(e1, rdict))
        }}
      println(rdict)
      println(rflat)
      Relation('Rf, rflat)
    }
  }

}
