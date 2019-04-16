package shredding.calc

import shredding.core._
import shredding.nrc.Shredding
import shredding.Utils.Symbol

trait CalcTranslator extends Algebra {
  this: CalcImplicits with Shredding with NRCTranslator =>
  

  object Unnester extends Serializable{

    var normalize = true

    /**
      * Turn a list of predicates into an and condition
      */
    def andPreds(e: List[CompCalc]): PrimitiveCalc = e match {
      case Nil => Constant(true, BoolType)
      case tail :: Nil => tail.asInstanceOf[PrimitiveCalc] 
      case head :: tail => AndCondition(head, andPreds(tail))
    }

    /**
      * Captures C12: { f({ e | r }) | p } to support 
      * unnesting when there are no generators left 
      * in the body
      */
    def unnestHead(e: CompCalc) = e match {
      case t:Tup => t.fields.filter(field => field._2.isOutermost)
      case _ => Nil
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
      //case b @ BagComp(e, qs) => qs match {
      case b:Comprehension => b.qs match {
        // { e | v <- X, tail }
        case head @ Generator(v: VarDef, x) :: tail => 
          // SELECTION: rule C4
          if (u.isEmpty && w.isEmpty){
            val nqs = tail.filter{_.pred1(v)}
            unnest(Comprehension(b.e, tail.filterNot(nqs.toSet)), u, w :+ v, Select(x, v, andPreds(nqs)))
          // JOIN and NEST - if outer depends on u value (C6, C7, C9, or C10)
          }else{
            val p1 = tail.filter{_.pred1(v)}
            val p2 = tail.filter{_.pred2(v, w)}
            val nb = Comprehension(b.e, tail.filterNot((p1++p2).toSet))
            (x,u) match {
              //case (b2 @ BagComp(be2, _), _) => 
              case (b2:Comprehension, _) =>
                // this should be identified as a bag variable
                unnest(Comprehension(b.e, tail), u, w :+ v, unnest(b2, w, w, e2))
              case (b2 @ CLookup(lbl, dict:OutputBagDict), _) =>
                unnest(Comprehension(b.e, tail), u, w :+ v, selectBag(b2, andPreds(Nil), v, w, e2))
              // if x is a path
              case (ProjToBag(vd, field), Nil) => unnest(nb, u, w :+v, Term(Unnest(w, x, andPreds(p1 ++ p2)), e2)) // UNNEST
              case (ProjToBag(vd, field), _) => unnest(nb, u, w :+v, Term(OuterUnnest(w, x, andPreds(p1 ++ p2)), e2)) // OUTER-UNNEST
              // if x is a variable
              case (_, Nil) => 
                unnest(nb, u, w :+v, Term(Join(w :+ v, andPreds(p2)), selectBag(x, andPreds(p1), v, w, e2))) // JOIN
              case (_, _) => 
                val joinPreds = x match {
                  case CLookup(lbl, dict) => 
                    if (andPreds(p2) == Constant(true, BoolType)){
                      Conditional(OpEq, lbl, TupleVar(v))
                    }else{
                      andPreds(Conditional(OpEq, lbl, lbl) +: p2)
                    }
                  case _ => andPreds(p2)
                }
                unnest(nb, u, w :+v, Term(OuterJoin(w :+ v, joinPreds), selectBag(x, andPreds(p1), v, w, e2))) // OUTER-JOIN
            }
          }
        // { e | p } (ie. has no generators): rules C5, C8, and C12
        case y if !b.hasGenerator => 
          val eprime = unnestHead(b.e)
          eprime match {
            case z if eprime.nonEmpty =>
              val nv = VarDef(Symbol.fresh("v"), eprime.head._2.tp)
              unnest(Comprehension(b.e, b.qs).substitute(eprime.head._2, nv), 
                u, w :+ nv, unnest(eprime.head._2, w, w, e2))
            case _ => if (u.isEmpty) { 
                Term(Reduce(b.e, w, andPreds(b.qs)), e2) 
              }else{
                // the last variable in the set is now the blocking variable
                Term(Nest(b.e, w, u, andPreds(b.qs), w.filterNot(u.toSet)), e2)
              }
          }
        case _ => sys.error("not supported")
      }
      case b:BagCalc => selectBag(b, Constant(true, BoolType), VarDef(Symbol.fresh("v"), b.tp.tp), w, e2)
      case CNamed(n, b) => NamedTerm(n, unnest(if (normalize) { b.normalize } else { b }))
      case CSequence(cs) => PlanSet(cs.map(unnest(_)))
      case _ => sys.error("not supported "+e1)
   }

   def selectBag(e:BagCalc, p:PrimitiveCalc, v: VarDef, w: List[VarDef], e2: AlgOp): AlgOp = e match {
     case b @ CLookup(lbl, dict @ OutputBagDict(lbl2, flat, tdict)) =>
      println("label "+lbl)
      println("w "+w)
      val ctxs = Map(w.map(v2 => v2.tp.asInstanceOf[TupleType].attrs.get("lbl") match {
        case Some(a) => a.asInstanceOf[LabelType].attrs.map(v3 => v3._1 -> VarRef(VarDef(v3._1, v3._2)))
        case None => (v2.name -> VarRef(v2))
      }).filter{ _ != Map() }.asInstanceOf[List[(String, Expr)]]:_*)
      println(ctxs)
      val unshredded = ExprShredder.unshred(flat, dict, ctxs)//Map(lbl.asInstanceOf[CLabel].vars.map(v2 => v2.varDef.name -> VarRef(v2.varDef)).toList:_*))
      val calcunshred = Translator.translate(unshredded)
      println(calc.quote(calcunshred.asInstanceOf[calc.CompCalc]))
      //Select(b, v, p) 
      unnest(if (normalize) { calcunshred.normalize } else { calcunshred }, w, w, e2)
     case _ => e2 match {
       case Init() => Select(e, v, p)
       case _ => Term(Select(e, v, p), e2)
     }
   }

  }

}
