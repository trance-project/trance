package shredding.calc

import shredding.core._

trait CalcTranslator extends Algebra{
  this: Calc =>

  object Unnester{
    // helper functions that should probably be moved 
    // to the Calc file
    def eq(p: TupleAttributeCalc, v: VarDef): Boolean = p match {
      case PrimitiveVar(vd) => vd.name == v.name
      case _ => false // check other types
    }

    // p[v]
    def pred1(v: VarDef, c: CompCalc): Boolean = c match {
      case AndCondition(e1, e2) => pred1(v, e1) && pred1(v, e2)
      case OrCondition(e1, e2) => pred1(v, e1) && pred1(v, e2)
      case NotCondition(e1) => pred1(v, e1) 
      case Conditional(op, e1, e2:Constant) => eq(e1, v)
      case Conditional(op, e1:Constant, e2) => eq(e2, v)
      case Conditional(op, e1, e2) => (e1 == e2) && (eq(e1, v) || eq(e2, v))
      case _ => false
    }

    // p[(w,v)]
    def pred2(v: VarDef, w: List[VarDef], c: CompCalc): Boolean = c match {
      case NotCondition(e1) => pred2(v, w, e1)
      case OrCondition(e1, e2) => pred2(v, w, e1) && pred2(v, w, e2)
      case AndCondition(e1, e2) => pred2(v, w, e1) && pred2(v, w, e2)
      case Conditional(op, e1, e2) => 
        (eq(e1,v) && w.map(eq(e2,_)).contains(true)) || (eq(e2,v) && w.map(eq(e1,_)).contains(true))
      case _ => false
    }

    /**
      * The unnest algorithm from Fegaras and Maier 2000, which translates 
      * comprehension calculus expressions into a series of alebraic operators. 
      * In general, Term1 = (E1, Term2) will pass output tuples from Term2 into an 
      * input stream of E1.
      * See Algebra.scala for more information on the operators
      * 
      * Currently, not implementing C11
      * @param e1: Calc, comprehension calculus expression on the left-hand side that represents the 
      *           calc expression that needs to be unnested
      * @param u: List[VarDef], list of variables that need to be converted to zeros during unnesting
      *           if they are nulls. These are the group-by variables.
      * @param w: List[VarDef], list of variables that are in the environment so far
      * @param e2: AlgOp, this is E in the F&M algorithm. It starts out as an empty input stream [{}]
      *            and is recursively replaced with Algebraic Term type that stores an Algebraic operator
      *            on the right, which sends tuples via an input stream to the alebraic operator on the left.
      * @returns The final AlgOp Term with the unnested, comprehension-calculus expressions as operator
      */
    def unnest(e1: CompCalc, u: List[VarDef] = List(), w: List[VarDef] = List(), e2: AlgOp = Init()): AlgOp = e1 match {
      case b @ BagComp(e, qs) => qs match {
        // { e | v <- X, tail }
        case head @ Generator(v: VarDef, x) :: tail => 
          // SELECTION: rule C4
          if (u.isEmpty && w.isEmpty){
            val nqs = tail.filter{ case y => pred1(v,y) }
            unnest(BagComp(e, tail.filterNot(nqs.toSet)), u, v +: w, 
                  Select(x, v, nqs.asInstanceOf[List[PrimitiveCalc]]))
          // JOIN and NEST - if outer depends on u value (C6, C7, C9, or C10)
          }else{
            val p1 = tail.filter{ case y => pred1(v, y) }.asInstanceOf[List[PrimitiveCalc]]
            val p2 = tail.filter{ case y => pred2(v, w, y) }.asInstanceOf[List[PrimitiveCalc]]
            val term = (x,u) match {
              case (BagVar(vd), Nil) => Unnest(v +: w, x, p1 ++ p2) // UNNEST
              case (BagVar(vd), _) => OuterUnnest(v +: w, x, p1 ++ p2) // OUTER-UNNEST
              case (_, Nil) => Term(Join(v +: w, p2), Select(x, v, p1)) // JOIN
              case (_, _) => Term(OuterJoin(v +: w, p2), Select(x, v, p1)) // OUTER-JOIN
            }
            unnest(BagComp(e, tail.filterNot((p1++p2).toSet)), u, v +: w, Term(term, e2))
          }
        // { e | p } (ie. has no generators): rules C5, C8, and C12
        case y if !b.hasGenerator => e match { // { f ( e' ) | p }
            case t: Tup => // f ( e' )
              // find outermost term e' = { e2 | r }
              val eprime = t.fields.filter(field => field._2.isOutermost)
              eprime match {
                // outermost term found in f ( { e2 | r } )
                case z if !eprime.isEmpty =>
                  // C12 [{ f(v) | p }] ( [{ e2 | r }] E )
                  val nv = VarDef("v", eprime.head._2.asInstanceOf[BagComp].tp)
                  unnest(BagComp(e.substitute(eprime.head._2.asInstanceOf[TupleAttributeCalc], nv).asInstanceOf[TupleCalc], qs), 
                        u, nv +: w, unnest(eprime.head._2, w, w, e2))
                case _ => if (u.isEmpty) {
                            // any new variable is coming from the input stream
                           // REDUCE: rule C5, u is empty so nothing to group by
                            Term(Reduce(e, w, qs.asInstanceOf[List[PrimitiveCalc]]), e2)
                          }else{
                           // NEST: rule C8, u is not empty so there is grouping
                           Term(Nest(e, w, u, qs.asInstanceOf[List[PrimitiveCalc]], w.filterNot(u.toSet)), e2)
                         }
              }
              // creat another function for this because it is messy
              case t:TupleVar => if (u.isEmpty) {
                           // any new variable is coming from the input stream
                            // REDUCE: rule C5, u is empty so nothing to group by
                            Term(Reduce(e, w, qs.asInstanceOf[List[PrimitiveCalc]]), e2)
                         }else{
                            // NEST: rule C8, u is not empty so there is grouping
                            Term(Nest(e, w, u, qs.asInstanceOf[List[PrimitiveCalc]], w.filterNot(u.toSet)), e2)
                          }
            case _ => sys.error("not supported")
          }
  
        case _ => sys.error("not supported")
      }
      case _ => sys.error("not supported")
    }

  }

}
