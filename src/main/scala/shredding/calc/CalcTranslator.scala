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
      * Matches the variable to the actual value based on the structure defined in
      * the tuple pattern represented by vars (use Any since could be a base type)
      */
    def extractVar(v: VarDef, vars: Any, value: Any): Any = vars match {
      case (a:VarDef, b:VarDef) => 
        if (a == v) { value.asInstanceOf[Product].productElement(0) } 
        else if (b == v) { value.asInstanceOf[Product].productElement(1) }
      case (a, b:VarDef) => if (b == v) { value.asInstanceOf[Product].productElement(1) } 
        else { extractVar(v, a, value.asInstanceOf[Product].productElement(0)) }
      case (a: VarDef, b) => 
        if (a == v) { value.asInstanceOf[Product].productElement(0) } 
        else { extractVar(v, b, value.asInstanceOf[Product].productElement(1)) }
      case _ => value
    }

    
    /**
      * Matches the pattern based on the structure of the the input vars
      */
    def matchPattern(vars: Any, e:CompCalc, value: Any): Any = e match {
      case Tup(fs) => fs.map{ case (k,v) => v match {
        case l @ CLabelRef(_) => l
        case BagVar(vd) => extractVar(vd, vars, value)
        case ProjToPrimitive(TupleVar(vd), f) => 
          extractVar(vd, vars, value).asInstanceOf[Product].productElement(getIndex(v))
        case ProjToBag(TupleVar(vd), f) => 
          extractVar(vd, vars, value).asInstanceOf[Product].productElement(getIndex(v))
        case ProjToLabel(TupleVar(vd), f) => 
          extractVar(vd, vars, value).asInstanceOf[Product].productElement(getIndex(v)) 
        }
      }
      case Constant(a, t) => a
      case v:Var => value // ?
      case _ => value.asInstanceOf[Product].productElement(getIndex(e))
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

    /**
      * Produces an anonymous function to map across an RDD[(K,V)] to key appropriately
      */
    def keyBy(vars: (_,VarDef), grps: List[VarDef]): Tuple2[_,_] => Tuple2[_,_] = vars match {
      case (b @ (_,_), c) =>
        val f = keyBy(b.asInstanceOf[Tuple2[_, VarDef]], grps)
        if (grps.contains(c)){
          (a:(_,_)) => val fa = f(a._1.asInstanceOf[Tuple2[_,_]]); ((fa._1, a._2), fa._2)
        }else{
          (a:(_,_)) => val fa = f(a._1.asInstanceOf[Tuple2[_,_]]); (fa._1, (a._2, fa._2))
        }
      case (b,c) => (grps.contains(b), grps.contains(c)) match {
        case (true, true) => (a:(_,_)) => a // need to look into this more
        case (true, false) => (a:(_,_)) => (a._1, a._2)
        case (false, true) => (a:(_,_)) => (a._2, a._1)
        case (false, false) => (a:(_,_)) => (None, None)
      }
    }

    /**
      * Initial implementation of Spark mappings and evaluation
      * // todo: reduce and unnest should read from pattern matching
      *
      */
    def evaluate(e: AlgOp): RDD[_] = e match {
      case Term(Reduce(e1, v, pred), e2) =>
        val structure = tuple2(v)
        val output = e2 match {
          case Term(Unnest(_,_,_), _) => 
            evaluate(e2).asInstanceOf[RDD[(_, Iterable[_])]].flatMap{ 
              case (k,v) => v.map{ matchPattern(e1, _) }
            } 
          case _ => evaluate(e2)
        }
        filterRDD(output, pred)
      case Term(Nest(e1, vars, grps, preds, zeros), e2) => 
        val mapFun = keyBy(tuple2(vars), grps)
        val newstruct = mapFun(tuple2(vars))
        evaluate(e2).map{ case (k,v) => mapFun((k,v)) }.groupByKey().map{
          case (k,v) => (k, v.map(matchPattern(newstruct._2, e1, _)))}.filter(_._2.nonEmpty)
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
      // duplication of data
      case Term(Unnest(vars, proj, pred), vterm) =>
        val i = getIndex(proj)
        val output = evaluate(vterm).map{
          case v => (v, v.asInstanceOf[Product].productElement(i))
        }
        filterRDD(output, pred)
      // v <- X
      case Select(x, v, pred) => filterRDD(evaluateBag(x), pred)
      case NamedTerm(n, t) => ctx.getOrElseUpdate(n, evaluate(t))
      case _ => sys.error("unsupported evaluation for "+e)
    }

    /**
      * Strips keys out of the tuple-maps, values are 
      * referenced by position based on the type later
      */
    def evaluateBag(e: BagCalc): RDD[_] = {
      e match {
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
