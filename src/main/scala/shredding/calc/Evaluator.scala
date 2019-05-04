package shredding.calc

import shredding.core._
import shredding.Utils.Symbol
import reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Translate Algebra term trees into Spark and execute
  */

trait AlgEvaluator extends AlgebraImplicits {
  
  /**case class SLabel(vals: Map[String, Any]){
    
    def extract(v:String): Any = vals.get(v) match {
      case Some(a) => a
      case None => None
    }

    def extract(i: Int): Any = extract(vals.keys.toList(i).toString)

  }

  object SLabel{
    def apply(vals: (String, Any)*): SLabel = SLabel(Map(vals: _*))
  }**/

  class SparkEvaluator(@transient val sc: SparkContext, ctx: Context[RDD[_]]) extends Serializable{
    
    def execute(p: PlanSet) = {
      p.plans.foreach(e =>{
        println("\nUnnested: "+e.quote)
        println("\nEvaluated: ")
        evaluate(e).take(100).foreach(println(_))
      })
    }
 
    def getIndex(t: Type, f: String): Int = t match {
      case TupleType(attrs) => attrs.keys.toList.indexOf(f)
      case LabelType(attrs) => attrs.keys.toList.indexOf(f)
    }
   
    /**
      * Gets the index of a projection label
      * // TODO make implicit
      */
    def getIndex(e: CompCalc): Int = e match { 
      //case ProjToLabel(t, f) => getIndex(t.tp, f)
      case ProjToBag(t, f) => getIndex(t.tp, f)
      case ProjToPrimitive(t,f) => getIndex(t.tp, f)
    }


    /**
      * Matches the variable to the actual value based on the structure defined in
      * the tuple pattern represented by vars (use Any since could be a base type)
      */
    def extractVar(v: VarDef, vars: Any, value: Any): Any = vars match {
      case (a:VarDef, b:VarDef) => 
        if (a.name == v.name){
          accessElement(value, 0)
        }else if (b.name == v.name){
          accessElement(value, 1)
        }else{
          accessLabel(value, v.name) // variable not in context
        }
      case (a, b:VarDef) => 
        if (b.name == v.name) { 
          accessElement(value, 1) 
        } else { 
          extractVar(v, a, value.asInstanceOf[Product].productElement(0)) 
        }
      case (a: VarDef, b) => 
        if (a.name == v.name) { 
          accessElement(value, 0) 
        } else { 
          extractVar(v, b, value.asInstanceOf[Product].productElement(1)) 
        }
      case a:VarDef if a.name == v.name => value //single var
      case _ => accessLabel(value, v.name) // single var not in context
    }

    /**
      * Access an element from a list or prouct type (ie. a tuple), 
      * if Iterable is recognized, this is a compact buffer produced from 
      * a groupBy() operation, so the values should be mapped over
      */
    def accessElement(xs: Any, i: Int): Any = xs match {
      //case l:Tuple2[_,_] if l._1.isInstanceOf[shredding.nrc.Label]=> 
        // coming from input bag
      //  accessElement(l._2, i)
      case l:List[_] => l(i)
      case l:Iterable[_] => l.map(accessElement(_, i))
      case l:Product => l.productElement(i)
      case l => l
    }

    /**
      * Called when a variable is not in context to extract the relevant variable from 
      * a label
      */
    def accessLabel(xs: Any, vname: String): Any = xs match {
      //case l:List[_] if (l.head.isInstanceOf[SLabel] && l.size == 1) => accessLabel(l.head, vname)
      //case lbl:SLabel => lbl.extract(vname)
      case l => l
    }

    /**
      * Matches the pattern based on the structure of the the input vars
      */
    def matchPattern(vars: Any, e:CompCalc, value: Any): Any = e match {
      case Tup(fs) => fs.map{ case (k,v) => v match {
        //case CLabel(id, vs) => 
        //  SLabel(vs.map( vd => (vd._1, matchPattern(vars, vd._2, value))).toList:_*)
        case _ => matchPattern(vars, v, value)
      }}
      case p:Proj => accessElement(matchPattern(vars, p.tuple, value), getIndex(e))
      case Sng(e1) => matchPattern(vars, e1, value)
      case Constant(a, t) => a
      case v:Var => extractVar(v.varDef, vars, value)
      case _ => sys.error("unsupported expression in match pattern "+e)
    }

    /**
      * maps terms in a condition to constant type to avoid comparision
      * issues with Any type
      */
    def toConstant(e: CompCalc, value: Any): CompCalc = e match {
      case c:Constant => e
      case _ => Constant(value.asInstanceOf[Product].productElement(getIndex(e)), 
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
    def tuple(f: List[_]): Any = f match {
      case tail :: Nil => tail
      case head :: tail :: Nil => (head, tail.asInstanceOf[VarDef])
      case head :: tail => tuple((head, tail.head) +: tail.tail)
    }

    /**
      * Produces an anonymous function to map across an RDD[(K,V)] to key appropriately
      */
    def keyBy(vars: Any, grps: List[VarDef]): Tuple2[_,_] => Tuple2[_,_] = vars match {
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
    
    def handleInputBag(vars: Any, e: CompCalc, a: Any) = a match {
      //case t:Tuple2[_,_] if t._1.isInstanceOf[shredding.nrc.Label] => t._1 // coming from input bag dict
      case _ => matchPattern(vars, e, a) 
    } 

    def joinKey(vars: Any, e:PrimitiveCalc): (Any => Tuple2[_,_], Any => Tuple2[_,_]) = e match {
      case Conditional(OpEq, e1, e2) => 
        val tuplev = vars.asInstanceOf[Tuple2[_,_]]
        ((a: Any) => (handleInputBag(tuplev._1, e1, a), (a)), (a: Any) => (handleInputBag(tuplev._2, e2, a), (a)))
      case _ => sys.error("unsupported join condition")
    }

    /**
      * Initial implementation of Spark mappings and evaluation
      * // todo: reduce and unnest should read from pattern matching
      *
      */
    def evaluate(e: AlgOp): RDD[_] = e match {
      case Term(Reduce(e1, v, pred), e2) => 
        val structure = tuple(v)
        val output = evaluate(e2).map(v1 => matchPattern(structure, e1, v1)) 
        filterRDD(output, pred, structure)
      case Term(Nest(e1, vars, grps, preds, zeros), e2) =>
        // reformat variable context
        val vars2 = tuple(vars)
        val mapFun = keyBy(vars2, grps)
        val newstruct = mapFun(vars2.asInstanceOf[Tuple2[_,_]])
        val output = evaluate(e2).map{
          case (k,v) => mapFun((k,v))
        }
        val out = e1.tp match {
          case t:PrimitiveType => output.map{
            case (k,v) => (k, 1)
          }.reduceByKey(_+_)
          case t => output.map{
            case (k,v) => (k, matchPattern(newstruct, e1, (k,v)))
          }.groupByKey() 
        }
        out
      case Term(OuterJoin(v, p), Term(e1, e2)) => 
      val output = p match {
        case Constant(true, _) => 
          evaluate(e2).cartesian(evaluate(e1))
        case _ =>
          val (m1, m2) = joinKey(tuple(v), p)
          val out1 = evaluate(e2).map(m1).map{ case (k,v) => (k,v) }
          val out2 = evaluate(e1).map(m2).map{ case (k,v) => (k,v) }
          val out = out1.join(out2).map{ case (k,v) => v }
          out
      }
      output
      case Term(Join(v, p), Term(e1, e2)) => p match {
        case Constant(true, _) => evaluate(e2).cartesian(evaluate(e1))
        case _ =>
          val (m1, m2) = joinKey(tuple(v), p)
          val out1 = evaluate(e2).map(m1).map{ case (k,v) => (k,v) }
          val out2 = evaluate(e1).map(m2).map{ case (k,v) => (k,v) }
          val out = out1.join(out2).map{ case (k,v) => v }
          out
      }
      // { (v, w) | v <- X, w <- v.path }
      case Term(Unnest(vars, proj, pred), vterm) =>
        val i = getIndex(proj)
        val output = evaluate(vterm).flatMap{
            v => accessElement(v, i).asInstanceOf[Iterable[_]].map{ 
                  v1 => ((v.asInstanceOf[List[_]].take(i) ++ v.asInstanceOf[List[_]].drop(i+1)), v1) }
          }
        filterRDD(output, pred, tuple(vars))
      // same as unnest 
      case Term(OuterUnnest(vars, proj, pred), vterm) =>
        val i = getIndex(proj)
        val output = evaluate(vterm).flatMap{
            v => accessElement(v, i).asInstanceOf[Iterable[_]] match {
              case Nil => List(((v.asInstanceOf[List[_]].take(i) ++ v.asInstanceOf[List[_]].drop(i+1)),None))
              case v2 => v2.map{ v1 => ((v.asInstanceOf[List[_]].take(i) ++ v.asInstanceOf[List[_]].drop(i+1)), v1) }
            }
          }
        filterRDD(output, pred, tuple(vars))
      // v <- X
      case Select(x @ BagVar(vd), v, pred) => 
        val output = filterRDD(ctx(vd.name), pred, v)
        output
      case NamedTerm(n, t) => ctx(n)
      case _ => sys.error("unsupported evaluation for "+e)
    }

  }
  
  object SparkEvaluator{
    def apply(sc: SparkContext, ctx: Context[RDD[_]]) = new SparkEvaluator(sc, ctx)
  }

}
