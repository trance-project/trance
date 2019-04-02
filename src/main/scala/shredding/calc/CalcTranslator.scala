package shredding.calc

import shredding.core._
import reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Translate Algebra term trees into Spark and execute
  */

trait AlgTranslator {
  this: Algebra with Calc =>
  
  class SparkEvaluator(@transient val sc: SparkContext) extends Serializable{
    
    import collection.mutable.{HashMap => HMap}
    
    val ctx: HMap[String, RDD[_]] = HMap[String, RDD[_]]()
    def reset = ctx.clear 
    
    /**
      * Gets the index of a projection label
      * // TODO make implicit
      */
    def getIndex(e: CompCalc): Int = e match { 
      case ProjToLabel(t, f) => t.tp.attrs.keys.toList.indexOf(f)
      case ProjToBag(t, f) => t.tp.attrs.keys.toList.indexOf(f)
      case ProjToPrimitive(t,f) => t.tp.attrs.keys.toList.indexOf(f)
    }

    /**
      * Matches a tuple pattern 
      */
    def matchPattern(e: CompCalc, value: Any) = e match {
      case Tup(fs) => fs.map{ case (k,v) => v match {
        case l @ CLabelRef(_) => l
        case _ => value.asInstanceOf[Product].productElement(getIndex(v))
      }}
      case Constant(a, t) => a
      case TupleVar(vd) => value // return the full value?
      case _ => value.asInstanceOf[Product].productElement(getIndex(e))
    }

    /**
      * maps terms in a condition to constant type to avoid comparision
      * issues with Any type
      */
    def toConstant(e: CompCalc, value: Any): CompCalc = e match {
      case c:Constant => e
      case _ => 
        Constant(value.asInstanceOf[Product].productElement(getIndex(e)), 
          e.tp.asInstanceOf[PrimitiveType])
    }

    /**
      * Casts to constant for equals and not equals, otherwise int 
      * only tested with one variable for now
      *
      */
    def filterCondition(e: CompCalc): Any => Boolean = e match {
      case Conditional(op, e1, e2) => op match {
        case OpGt => (a: Any) => 
          matchPattern(e1, a).asInstanceOf[Int] > matchPattern(e2, a).asInstanceOf[Int]
        case OpGe => (a: Any) => 
          matchPattern(e1, a).asInstanceOf[Int] >= matchPattern(e2, a).asInstanceOf[Int]
        case OpNe => (a: Any) => 
          toConstant(e1, a) != toConstant(e2, a)
        case OpEq => (a: Any) => 
          toConstant(e1, a) == toConstant(e2, a)
      }
      case NotCondition(e1) => (a: Any) => !filterCondition(e)(a)
      case AndCondition(e1, e2) => (a: Any) => filterCondition(e1)(a) && filterCondition(e2)(a)
      case OrCondition(e1, e2) => (a: Any) => filterCondition(e1)(a) || filterCondition(e2)(a)
    }

    /**
      * Passes the filter conditions, if necessary
      */
    def filterRDD(r: RDD[_], p: PrimitiveCalc) = p match {
      case Constant(true, _) => r
      case _ => r.filter(filterCondition(p)(_))
    }
 
    /**
      * Tuples a list of vardefs into (K,V) structure
      */ 
    def tuple2(f: List[_]): (_, VarDef) = f match {
      case head :: tail :: Nil => (head, tail.asInstanceOf[VarDef])
      case head :: tail => tuple2((head, tail.head) +: tail.tail)
    }

    def groups(vars: (_,_), grps: List[_]): Tuple2[_,_] => List[_] = vars match {
      case (b @ (_,_), c) =>
        val f = groups(b, grps)
        if (grps.contains(c)) { (a: (_,_)) => f(a) :+ a._2 } else { f }
      case (b, c) => (grps.contains(b), grps.contains(c)) match {
        case (true, true) => (a:(_,_)) =>
          List(a._1.asInstanceOf[Product].productElement(0), a._1.asInstanceOf[Product].productElement(1))
        case (true, false) => (a:(_,_)) =>
          List(a._1.asInstanceOf[Product].productElement(0))
        case (false, true) => (a:(_,_)) => List(a._2.asInstanceOf[Product].productElement(1))
        case _ => (a:(_,_)) => Nil
      }
    }
   
    /**
      * Linearization will produce 3 types of queries:
      *
      * 1) EmptyCtx1 = { (lbl := Label(...)) } // top level query (just a single label) 
      *   - optimizations can be made to not turn this into an rdd
      *
      * 2) For label in domain union sng( k = Label(...), v = flat query )
      *
      *   - given the structure of this query:
      *       M_flat4 := For l4 in EmptyCtx3 Union
      *                   sng(( k := l4.lbl, v := For x0 in R Union ...
      *     translates to comp calc:
      *       { ( k := l4.lbl, v := { ... |  x0 <- R  } ) |  l4 <- EmptyCtx3 }
      *     which will always produce the plan:
      *        Reduce[ U / lambda(v10,l4).( k := l4.lbl, v := v10 ), lambda(v10,l4).true]
      *          Nest[ U / lambda(x0,l4).( ... ) / lambda(x0,l4).l4, lambda(x0,l4).true / lambda(x0,l4).x0]
      *            OuterJoin[lambda(x0,l4).true]
      *              Select[lambda(x0).true](R)
      *              Select[lambda(l4).true](EmptyCtx3)
      *     thus, nest will output data in the pattern (k,v) and pattern matching does not need to happen
      *     in reduce
      *
      * 3) For kv in M_flat union For xF in kv.v union sng(lbl = xF.bagAttr) 
      *     - this is also a predictable pattern:
      *       { v |  kv <- M_flat ,  v <- { ( lbl := xF.bagAttr ) |  xF <- kv.v  } }
      *       Reduce[ U / lambda(xF,kv).( lbl := xF.bagAttr ), lambda(xF,kv).true]
      *          Unnest[lambda(xF,kv).kv.v, lambda(xF,kv).true]
      *            Select[lambda(kv).true](M_flat)
      *     which requires pattern matching to extract the labels identified at xF.bagAttr
      *     
      */ 
    def evaluate(e: AlgOp): RDD[_] = e match {
      case Term(Reduce(e1, v, pred), e2) =>
        val output = e2 match {
          case Term(Unnest(_,_,_), _) => 
            evaluate(e2).asInstanceOf[RDD[(_, Iterable[_])]].flatMap{
              case (k,v) => v.map{ case (k1, v1) => matchPattern(e1, v1) }
            }
          case _ => evaluate(e2)
        }
        filterRDD(output, pred)
      case Term(Nest(e1, vars, grps, preds, zeros), e2) => 
        val groupFun = groups(tuple2(vars), grps)
        // this should map data to proper key, value structure 
        evaluate(e2).map{ case (k,v) => (k, matchPattern(e1,v)) }.groupBy(groupFun)//.filter(!_._2.isEmpty)
      case Term(OuterJoin(v, p), Term(e1, e2)) => p match {
        case Constant(true, _) => 
        evaluate(e2).cartesian(evaluate(e1))
        case _ => 
          evaluate(e2).map{ case (k,v) => (k, v) }
          .leftOuterJoin(evaluate(e1).map{ case (k,v) => (k, v) })
      }
      case Term(Join(v, p), Term(e1, e2)) => p match {
        case Constant(true, _) => evaluate(e2).cartesian(evaluate(e1))
        case _ => evaluate(e2).map{ case (k,v) => (k, v) }
                   .join(evaluate(e1).map{ case (k,v) => (k, v) })
      }
      // { (v, w) | v <- X, w <- v.path }
      case Term(Unnest(vars, proj, pred), vterm) =>
        val i = getIndex(proj)
        // think about handling this de-duplication of data
        val output = evaluate(vterm).map{
          case v => (v, v.asInstanceOf[Product].productElement(i))
        }
        filterRDD(output, pred)
      // v <- X
      case Select(x, v, pred) => filterRDD(evaluateBag(x), pred)
      case NamedTerm(n, t) => ctx.getOrElseUpdate(n, evaluate(t))
      case _ => sys.error("unsupported evaluation for "+e)
    }

    def evaluateBag(e: BagCalc): RDD[_] = {
      e match {
      // temporary solution to embedded maps
      case BagVar(vd) => ctx(vd.name)
      case NamedCBag(n, b) => ctx.getOrElseUpdate(n, evaluateBag(b))
      case Sng(t @ Tup(_)) => 
        sc.parallelize(Seq(t.fields.map{ case (k,v) => v }))
      case InputR(n, d:List[Map[String,Any]], t) => 
        val data = d.map(m => m.map{ case (k,v) => v })
        ctx.getOrElseUpdate(n, sc.parallelize(data))
      case _ => sys.error("unsupported evaluation for "+e)
    }}

  } 

  object SparkEvaluator{
    def apply(sc: SparkContext) = new SparkEvaluator(sc)
  }
}
