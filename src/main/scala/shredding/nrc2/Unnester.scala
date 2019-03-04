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
  
  def unnest(e1: Calc, u: List[VarDef] = List(), w: List[VarDef] = List(), e2: AlgOp = Init()): AlgOp = e1 match {
    case BagComp(e, qs) => qs match {
      // if e = { e1 | p } (ie. has no generators) 
      case y if !hasGenerator(qs) => e match {
          case _ => if (u.isEmpty) { 
                      Term2(Reduce(e1, w.head, qs.asInstanceOf[List[Pred]]), e2)
                    }else{
                      // replace reduce with group by
                      Term2(Reduce(e1, w.head, qs.asInstanceOf[List[Pred]]), e2)
                    }
        }
      case Nil => Term2(Reduce(e1, w.head, List[Pred]()), e2)
      case head @ Generator(v: VarDef, x) :: tail => 
        if (u.isEmpty && w.isEmpty){ // selection
          val nqs = tail.filter{ case y => pred1(v,y) }
          unnest(BagComp(e, tail.filterNot(nqs.toSet)), u, v +: w, 
                  Term2(Select(x, v, nqs.asInstanceOf[List[Pred]]), e2))
        }else{
          val nqs = tail.filter{ case y => pred2(v, w.head, y) }
          val term = (x,u) match {
            case (BagVar(n, f, t), Nil) => Unnest(x, nqs.asInstanceOf[List[Pred]]) // unnest
            case (BagVar(n, f, t), _) => OuterUnnest(x, nqs.asInstanceOf[List[Pred]]) // outer-unnest
            case (_, Nil) => Join(x, nqs.asInstanceOf[List[Pred]]) // join
            case (_, _) => OuterJoin(x, nqs.asInstanceOf[List[Pred]]) // outer-join
          }
          unnest(BagComp(e, tail.filterNot(nqs.toSet)), u, v +: w, Term2(term, e2))
        }
     }
  }

}
