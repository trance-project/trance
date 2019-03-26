package shredding.calc

import shredding.core._

trait CalcTranslator extends Algebra{
  this: CalcImplicits =>
  

  object Unnester{
    
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
            val nqs = tail.filter{_.pred1(v)}
            unnest(BagComp(e, tail.filterNot(nqs.toSet)), u, v +: w, 
                  Select(x, v, nqs.asInstanceOf[List[PrimitiveCalc]]))
          // JOIN and NEST - if outer depends on u value (C6, C7, C9, or C10)
          }else{
            val p1 = tail.filter{_.pred1(v)}.asInstanceOf[List[PrimitiveCalc]]
            val p2 = tail.filter{_.pred2(v, w)}.asInstanceOf[List[PrimitiveCalc]]
            val term = (x,u) match {
              // if x is a path
              case (ProjToBag(vd, field), Nil) => Term(Unnest(v +: w, x, p1 ++ p2), e2) // UNNEST
              case (ProjToBag(vd, field), _) => Term(OuterUnnest(v +: w, x, p1 ++ p2), e2) // OUTER-UNNEST
              // if x is a variable
              case (_, Nil) => Term(Join(v +: w, p2), Term(Select(x, v, p1), e2)) // JOIN
              case (_, _) => Term(OuterJoin(v +: w, p2), Term(Select(x, v, p1), e2)) // OUTER-JOIN
            }
            unnest(BagComp(e, tail.filterNot((p1++p2).toSet)), u, v +: w, term)
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
                  val nv = VarDef("v", eprime.head._2.asInstanceOf[BagComp].tp, VarCnt.inc)
                  unnest(BagComp(e.substitute(eprime.head._2, nv).asInstanceOf[TupleCalc], qs), 
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
