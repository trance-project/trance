package shredding.nrc

import shredding.Utils._
import reflect.runtime.universe.{ Symbol => _, _ }

import scala.collection.mutable.{HashMap,Map,SortedMap}

/**
  * Transformations for NRC Expressions
  */

trait NRCTransforms extends EmbedNRC with CompCalc {

  /**
    * Pretty printer
    */
  object Printer {
    def quote[A](e: Expr[A]): String = e match {
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
      case Mult(x, y) => s"Mult(${quote(x)}, ${quote(y)})"
      case IfThenElse(e1, e2, e3) => s"if ${quote(e1)} then ${quote(e2)} else ${quote(e3)}"
      case _ => "<unknown>"
    }

    def cquote[A](e: Calc[A]): String = e match {
      case Comprehension(x, c@_*) => s"{ ${cquote(x)} | ${c.map(cquote(_)).mkString(",")} }"
      case Generator(x, v) => s"${cquote(x)} <- ${cquote(v)}"
      case Pred(op) => s"${cquote(op)}"
      case OpEq(e1, e2) => s"${cquote(e1)} = ${cquote(e2)}"
      case OpLeq(e1, e2) => s"${cquote(e1)} <= ${cquote(e2)}"
      case OpLt(e1, e2) => s"${cquote(e1)} < ${cquote(e2)}"
      case r @ Record(e@_*) => s"(${r.e.map(cquote(_)).mkString(",")})"
      case Sng(e1) => s"{${cquote(e1)}}"
      case RProject(e1, pos) => s"${cquote(e1)}._${pos}"
      case Constant(c) => c.toString()
      case Bind(x, v) => s"${cquote(x)} := ${cquote(v)}"
      case Symb(x,id) => x.name + id
      case InputR(s,b) => cquote(s)
      case Zero() => "{}"
      case IfStmt(e1, e2, e3) => s"if ${cquote(e1)} then ${cquote(e2)} else ${cquote(e3)}"
      case Null() => "null"
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
              (we, eval(e4))
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
        case Mult(x,y) => eval(y).asInstanceOf[List[Any]].filter{ _ == eval(x) }.size.asInstanceOf[A]
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
      
      var toplevel = true
      var qsym = Sym[Any]('Q)
      var dsym = Sym[Any]('D)
    
      while (!exprs.isEmpty) {
        var fs = List[Expr[_]]()
        if (toplevel){ toplevel = false }else{ fs = List(dsym) }
        var shredq = shred(exprs.pop, fs)
        shredset += (qsym -> shredq)

        // produce domain query if necessary
        dsym = Sym[Any]('D)
        var domain = shredq.asInstanceOf[Expr[Any]].domain
        if (!domain.isEmpty){
          // relaxing singleton tuple requirement here for cleaner evaluation
          var d = Relation(qsym, domain).ForeachUnion(l => Singleton(l))
          shredset += (dsym -> d)
          qsym = Sym[Any]('Q)
        }
      }
      shredset
    }

    def checkDomainNested[A](e: Expr[A]): Boolean = {
      try{
        if (e.asInstanceOf[Sym[Any]].x == 'D) true else false
      }catch{
        case e:Exception => false
      }
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
    def shred[A](e: Expr[A], fs: List[Expr[_]] = List()): Expr[A] = {
      e match {
        case ForeachUnion(x, e1, e2) => fs match {
          case y if fs.isEmpty => 
            val se = shred(e1)
            ForeachUnion(x, se, shred(e2, se.freevars))
          case y if checkDomainNested(fs.head) =>
              val we = Sym[Any]('we)
              ForeachUnion(we, fs.head.asInstanceOf[Expr[TBag[Any]]], 
                ForeachUnion(x, e1, shred(e2, fs.tail :+ e1)))
          case y if !checkDomainNested(fs.head) =>
            ForeachUnion(x, e1, shred(e2, e1.freevars))
        }
        case Union(e1, e2) => Union(shred(e1, fs), shred(e2, fs))
        case r @ Relation(_, _) => shredRelation(r)
        case Singleton(e1) => Singleton(wrapNewLabel(e1))
        case EmptySet() => EmptySet()
        case IfThenElse(e1,e2,e3) => IfThenElse(shred(e1, fs), shred(e2, fs), shred(e3, fs))
        case TupleStruct1(e1) => TupleStruct1(wrapNewLabel(e1))
        case TupleStruct2(e1, e2) => TupleStruct2(wrapNewLabel(e1), wrapNewLabel(e2))
        case TupleStruct3(e1, e2, e3) => 
          TupleStruct3(wrapNewLabel(e1), wrapNewLabel(e2), wrapNewLabel(e3))
        case Project(e1, pos) => Project(shred(e1), pos)
        case Eq(e1, e2) => Eq(shred(e1), shred(e2))
        case And(e1, e2) => And(shred(e1), shred(e2))
        case Mult(e1, e2) => Mult(shred(e1), e2)
        case Const(_) | Sym(_, _) => e
        case _ => sys.error("not supported")
      }
    }

    /**
      * wrapNewLabel generates a label for a Bag type inside a tuple 
      * subexpressions that are not same level are shredded
      * and added to the query set
      */
    def wrapNewLabel[A: TypeTag](e: Expr[A]): Expr[A] = isShreddable(e) match { 
      case true => {
        exprs.push(e)
        Label(Sym[A]('q), e.freevars.distinct.map{v => (v, None)})         
      }
      case _ => shred(e)
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

  object Calculus {
    
    def translate[A](e: Expr[A]): Calc[A] = e match {
      // U { e | x <- S, q }
      case ForeachUnion(x, e1, e2) => e1 match {
          case ForeachUnion(y, e3, e4) => // N8
            Comprehension(translate(e2), 
              Seq(Generator(translate(y), translate(e3)), Bind(translate(x), translate(e4))):_*)
          case EmptySet() => Zero() //N5
          case _ => Comprehension(translate(e2), Seq(Generator(translate(x), translate(e1))):_*)
      }
      case Union(e1, e2) => Merge(translate(e1), translate(e2))
      // U { e | pred, q }
      case IfThenElse(e1, e2, e3 @ EmptySet()) => 
        Comprehension(translate(e2), Seq(Pred(translate(e1).asInstanceOf[Calc[Boolean]])):_*)
      case EmptySet() => Zero()
      // pred
      case Eq(e1, e2) => OpEq(translate(e1), translate(e2))
      case Leq(e1, e2) => OpLeq(translate(e1), translate(e2))
      case Lt(e1, e2) => OpLt(translate(e1), translate(e2))
      // U { e | }
      case TupleStruct1(e1) => Record(Seq(translate(e1)):_*)
      case TupleStruct2(e1, e2) => Record(Seq(translate(e1), translate(e2)):_*)
      case TupleStruct3(e1, e2, e3) => Record(Seq(translate(e1), translate(e2), translate(e3)):_*)
      case Singleton(e1) => Sng(translate(e1))//Comprehension(translate(e1), Seq(Zero()):_*)
      case Project(e1, pos) => RProject(translate(e1), pos)
      case Const(e) => Constant(e)
      case Sym(s, id) => Symb(s,id)
      case Relation(s, b) => InputR(translate(s), b) 
      case _ => sys.error("not implemented") 
    }

    def normalize[A](e: Calc[A], env: List[Calc[_]] = List()): Calc[A] = e match {
      case Generator(x, e2 @ Zero()) => Zero() //N5
      case Generator(x, e1 @ Sng(e2)) => Bind(normalize(x), normalize(e2)).asInstanceOf[Calc[A]] //N6
      case Comprehension(e1, q@_*) => //base case
        val qs = q.map(normalize(_))
        qs match {
          case y if qs.contains(Zero()) => Zero()
          case _ => Comprehension(normalize(e1), qs:_*)
        }
      case RProject(e1 @ Record(_), pos) => e1.productElement(pos).asInstanceOf[Calc[A]] //N3
      case Bind(x,y) => Bind(normalize(x), normalize(y))
      case Sng(Record(e1@_*)) => e1.size match {
        case 1 => normalize(e1.head.asInstanceOf[Calc[A]])
        case _ => Record(e1.map(normalize(_)):_*)
      }
      case Sng(RProject(e1, pos)) => RProject(normalize(e1), pos)
      case Sng(Symb(x,id)) => Symb(x,id)
      case _ => e
    }


  }

}
