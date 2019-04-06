package shredding.calc

import shredding.core._
import reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Translate Algebra term trees into Spark and execute
  */

trait AlgTranslator {
  this: Algebra with ShreddedCalc =>
  
  class SparkEvaluator(@transient val sc: SparkContext) extends Serializable{
    
    import collection.mutable.{HashMap => HMap}
    
    val ctx: HMap[String, RDD[_]] = HMap[String, RDD[_]]()
    def reset = ctx.clear 
   
    def execute(p: PlanSet) = {
      reset
      p.plans.foreach(e =>{
        println("\nUnnested: "+calc.quote(e.asInstanceOf[calc.AlgOp]))
        println("\nEvaluated: ")
        evaluate(e).take(100).foreach(println(_))
      })
    }
    
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
        else if (b == v) { 
          value.asInstanceOf[Product].productElement(1)
        }
      case (a, b:VarDef) => if (b == v) { value.asInstanceOf[Product].productElement(1) } 
        else { extractVar(v, a, value.asInstanceOf[Product].productElement(0)) }
      case (a: VarDef, b) => 
        if (a == v) { value.asInstanceOf[Product].productElement(0) } 
        else { extractVar(v, b, value.asInstanceOf[Product].productElement(1)) }
      case _ => value
    }

    def flatten(xs: Any, i:Int): Any = xs match {
      case s:List[Set[_]] => s(i)
      case l:Iterable[_] => l.map(flatten(_, i))
      case l => l.asInstanceOf[Product].productElement(i)
    }

    /**
      * Matches the pattern based on the structure of the the input vars
      */
    def matchPattern(vars: Any, e:CompCalc, value: Any): Any = {
      e match {
      case Tup(fs) => fs.map{ case (k,v) => v match {
        case CLabel(vs,_) => vs.map( vd => extractVar(vd.varDef, vars, value))
        case _ => matchPattern(vars, v, value)
      }}
      case p:Proj => p.tuple match {
        case TupleVar(vd) => flatten(extractVar(vd, vars, value), getIndex(e))
        /** match {
            case ev:Iterable[_] => 
              println("extracted this var "+ev); 
              ev.map(_.asInstanceOf[Product].productElement(getIndex(e)))
            case ev => 
              println("extracted a base type "+ev); 
              ev.asInstanceOf[Product].productElement(getIndex(e))
          }
        }**/
      }
      case Sng(e1) => matchPattern(vars, e1, value)
      case Constant(a, t) => a
      case v:Var => extractVar(v.varDef, vars, value)
      case _ => value.asInstanceOf[Product].productElement(getIndex(e))
    }}

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
    def filterCondition(vars: Any, e: CompCalc): Any => Boolean = e match {
      case Conditional(op, e1, e2) => op match {
        case OpGt => (a: Any) => 
          matchPattern(vars, e1, a).asInstanceOf[Int] > matchPattern(vars, e2, a).asInstanceOf[Int]
        case OpGe => (a: Any) => 
          matchPattern(vars, e1, a).asInstanceOf[Int] >= matchPattern(vars, e2, a).asInstanceOf[Int]
        case OpNe => (a: Any) => 
          toConstant(e1, a) != toConstant(e2, a)
        case OpEq => (a: Any) => 
          toConstant(e1, a) == toConstant(e2, a)
      }
      case NotCondition(e1) => (a: Any) => !filterCondition(vars, e)(a)
      case AndCondition(e1, e2) => (a: Any) => filterCondition(vars, e1)(a) && filterCondition(vars, e2)(a)
      case OrCondition(e1, e2) => (a: Any) => filterCondition(vars, e1)(a) || filterCondition(vars, e2)(a)
    }

    /**
      * Passes the filter conditions, if necessary
      */
    def filterRDD(r: RDD[_], p: PrimitiveCalc, vars: Any) = p match {
      case Constant(true, _) => r
      case _ => r.filter(filterCondition(vars, p)(_))
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
    def keyBy(vars: (_,_), grps: List[VarDef]): Tuple2[_,_] => Tuple2[_,_] = vars match {
      case (b @ (_,_), c) =>
        val f = keyBy(b.asInstanceOf[Tuple2[_, _]], grps)
        if (grps.contains(c)){
          (a:(_,_)) => 
            val fa = f(a._1.asInstanceOf[Tuple2[_,_]]); 
            // assumes that (a,None) doesn't happen in this case
            // or else that is grouping by every element..
            ((fa._1, a._2), fa._2)
        }else{
          (a:(_,_)) => 
            val fa = f(a._1.asInstanceOf[Tuple2[_,_]]); 
            fa match {
              case (_,None) => (fa._1, a._2)
              case (_,_) => (fa._1, (fa._2, a._2))
            }
        }
      case (b,c) => (grps.contains(b), grps.contains(c)) match {
        case (true, true) => (a:(_,_)) => (a, None) // need to look into this more
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
        val structure = v match {
          case tail :: Nil => tail
          case head :: tail => tuple2(v)
        }
        val output = evaluate(e2).map(matchPattern(structure, e1, _))
        filterRDD(output, pred, structure)
      case Term(Nest(e1, vars, grps, preds, zeros), e2) =>
        val vars2 = tuple2(vars)
        val mapFun = keyBy(vars2, grps)
        val newstruct = mapFun(vars2)
        evaluate(e2).map{
          case (k,v) => mapFun((k,v))
        }.map{
          case (k,v) => (k, matchPattern(newstruct, e1, (k,v)))
        }.groupByKey() 
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
      // should w be removed if u is null?
      case Term(Unnest(vars, proj, pred), vterm) =>
        val i = getIndex(proj)
        val output = evaluate(vterm).flatMap{
          case v => v.asInstanceOf[Product].productElement(i).asInstanceOf[Iterable[_]].map{
            v2 => ((v),v2)
          }
        }
        filterRDD(output, pred, tuple2(vars))
      // same as unnest 
      case Term(OuterUnnest(vars, proj, pred), vterm) =>
        val i = getIndex(proj)
        val output = evaluate(vterm).flatMap{
          case v => v.asInstanceOf[Product].productElement(i).asInstanceOf[Iterable[_]] match {
            case Nil => List(((v), None))
            case v1 => v1.map{ v2 => ((v), v2) }
          }
        }
        filterRDD(output, pred, tuple2(vars))
      // v <- X
      case Select(x, v, pred) => filterRDD(evaluateBag(x), pred, v)
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
      case Sng(t @ Tup(_)) => 
        sc.parallelize(Seq(t.fields.map{ case (k,v) => v }))
      case InputR(n, d, t) => 
        val data = d.asInstanceOf[List[Map[String,Any]]].map(m => m.map{ case (k,v) => v })
        ctx.getOrElseUpdate(n, sc.parallelize(data))
      case _ => sys.error("unsupported evaluation for "+e)
    }}

  } 

  object SparkEvaluator{
    def apply(sc: SparkContext) = new SparkEvaluator(sc)
  }
}
