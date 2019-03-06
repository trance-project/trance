package shredding.nrc2

object Unnester{

  // should make this implicit somewhere
  def eq(p: AttributeCalc, x: VarDef): Boolean = p match {
    case PrimitiveVar(n, f, t) => n == x.n
    case _ => false // check other types
  }

  // p[v]
  def pred1(x: VarDef, c: Calc): Boolean = c match {
    case Pred(Conditional(op, e1, e2)) => eq(e1, x) || eq(e2, x) 
    case _ => false
  }

  // p[(w,v)]
  def pred2(x: VarDef, y: VarDef, c: Calc): Boolean = c match {
    case Pred(Conditional(op, e1, e2)) => (eq(e1,x) && eq(e2,y)) || (eq(e1,y) && eq(e2,x))
    case _ => false
  }

  def hasGenerator(e1: List[Calc]): Boolean = e1 match {
    case Nil => false
    case Generator(_,_) :: tail => true
    case head :: tail => hasGenerator(tail)
  }

  def substitute(e1: Calc, e2: Calc, v: VarDef): Calc = e1 match {
    case y if e1 == e2 => Var(v)
    case t:Tup => Tup(t.fields.map(f => f._1 -> substitute(f._2, e2, v).asInstanceOf[AttributeCalc]))
    case _ => e1 
  }

  def isOutermost(e1: Calc): Boolean = e1 match {
    case BagComp(_,_) => true
    case _ => false
  } 
  
  def unnest(e1: Calc, u: List[VarDef] = List(), w: List[VarDef] = List(), e2: AlgOp = Init()): AlgOp = e1 match {
    case BagComp(e, qs) => qs match {
      // if e = { e1 | p } (ie. has no generators) 
      case y if !hasGenerator(qs) => e match { // { f (...) | qs }
          case t: Tup => // f
            val eprime = t.fields.filter(field => isOutermost(field._2))
            eprime match {
              case z if eprime.isEmpty => if (u.isEmpty) { 
                          Term(Reduce(e, w, qs.asInstanceOf[List[Pred]]), e2)
                        }else{
                          Term(Nest(e, w, u, qs.asInstanceOf[List[Pred]], w.filterNot(u.toSet)), e2)
                        }
              case _ => // f( { e1 | qs1 } )
                val nv = VarDef("v", eprime.head._2.asInstanceOf[BagComp].tp)
                unnest(BagComp(substitute(e, eprime.head._2.asInstanceOf[AttributeCalc], nv).asInstanceOf[TupleCalc], qs), u, nv +: w, 
                       unnest(eprime.head._2, w, w, e2))
            }
          case _ => sys.error("not supported")
      }
      case Nil => Term(Reduce(e1, w, List[Pred]()), e2)
      case head @ Generator(v: VarDef, x) :: tail => 
        if (u.isEmpty && w.isEmpty){ // selection
          val nqs = tail.filter{ case y => pred1(v,y) }
          unnest(BagComp(e, tail.filterNot(nqs.toSet)), u, v +: w, 
                  Term(Select(x, v, nqs.asInstanceOf[List[Pred]]), e2))
        }else{
          val nqs = tail.filter{ case y => pred2(v, w.head, y) }
          val term = (x,u) match {
            case (BagVar(n, f, t), Nil) => Unnest(w.head, x, nqs.asInstanceOf[List[Pred]]) // unnest
            case (BagVar(n, f, t), _) => OuterUnnest(w.head, x, nqs.asInstanceOf[List[Pred]]) // outer-unnest
            case (_, Nil) => Term(Join(w.head, v, nqs.asInstanceOf[List[Pred]]), 
                                   Select(x, v, tail.filter{ case y => pred1(v,y) }.asInstanceOf[List[Pred]])) // join
            case (_, _) => Term(OuterJoin(w.head, v, nqs.asInstanceOf[List[Pred]]), 
                                   Select(x, v, tail.filter{ case y => pred1(v,y) }.asInstanceOf[List[Pred]])) // outer-join
          }
          unnest(BagComp(e, tail.filterNot(nqs.toSet)), u, v +: w, Term(term, e2))
        }
     }
  }

}
