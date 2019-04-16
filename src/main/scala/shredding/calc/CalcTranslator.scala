package shredding.calc

import shredding.core._
import shredding.Utils.Symbol
import reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Translate Algebra term trees into Spark and execute
  */

trait AlgTranslator {
  this: Algebra with ShreddedCalc =>
  
  case class SLabel(vals: Map[String, Any]){
    def extract(v:String): Any = vals.get(v) match {
      case Some(a) => a
      case None => None
    }
  }

  object SLabel{
    def apply(vals: (String, Any)*): SLabel = SLabel(Map(vals: _*))
  }

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
 
    def getIndex(t: Type, f: String): Int = t match {
      case TupleType(attrs) => attrs.keys.toList.indexOf(f)
      case LabelType(attrs) => attrs.keys.toList.indexOf(f)
    }
   
    /**
      * Gets the index of a projection label
      * // TODO make implicit
      */
    def getIndex(e: CompCalc): Int = e match { 
      case ProjToLabel(t, f) => getIndex(t.tp, f)
      case ProjToBag(t, f) => getIndex(t.tp, f)
      case ProjToPrimitive(t,f) => getIndex(t.tp, f)
    }


    /**
      * Matches the variable to the actual value based on the structure defined in
      * the tuple pattern represented by vars (use Any since could be a base type)
      */
    def extractVar(v: VarDef, vars: Any, value: Any): Any = vars match {
      case (a:VarDef, b:VarDef) => 
        if (a.name == v.name) { accessElement(value, 0) }
        else if (b.name == v.name){
          accessElement(value, 1)
        }else{
          println("in here with this value "+value)
          println(v)
          println(vars)
          value.asInstanceOf[SLabel].extract(v.name)
        }
      case (a, b:VarDef) => 
        if (b.name == v.name) { 
          accessElement(value, 1) 
        } else { 
          extractVar(v, a, value.asInstanceOf[Product].productElement(0)) 
        }
      case (a: VarDef, b) => 
        if (a.name == v.name) { 
          accessElement(value,0) 
        } else { 
          extractVar(v, b, value.asInstanceOf[Product].productElement(1)) 
        }
      case _ => value // single var 
    }


    /**
      * Access an element from a list or prouct type (ie. a tuple), 
      * if Iterable is recognized, this is a compact buffer produced from 
      * a groupBy() operation, so the values should be mapped over
      */
    def accessElement(xs: Any, i: Int): Any = xs match {
      case l:List[_] => l(i)
      case l:Iterable[_] => l.map(accessElement(_, i))
      case l:Product => l.productElement(i)
      case l => l
    }

    /**
      * Matches the pattern based on the structure of the the input vars
      */
    def matchPattern(vars: Any, e:CompCalc, value: Any): Any = {
      e match {
      case Tup(fs) => fs.map{ case (k,v) => v match {
        case CLabel(vs,_) => 
          SLabel(vs.toList.map( vd => (vd.varDef.name, matchPattern(vars, vd, value))):_*)
        case _ => matchPattern(vars, v, value)
      }}
      case p:Proj => 
         accessElement(matchPattern(vars, p.tuple, value), getIndex(e))
      case Sng(e1) => matchPattern(vars, e1, value)
      case Constant(a, t) => a
      case v:Var => 
        extractVar(v.varDef, vars, value)
      case _ => accessElement(value, getIndex(e))
    }}

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
    def tuple2(f: List[_]): Any = f match {
      case tail :: Nil => tail
      case head :: tail :: Nil => (head, tail.asInstanceOf[VarDef])
      case head :: tail => tuple2((head, tail.head) +: tail.tail)
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

    def getVar(e: CompCalc): VarDef = e match {
      case p:Proj => getVar(p.tuple)
      case TupleVar(vd) => vd 
    }
    
    /**
      * Returns value within a tuple
      * if a label is found, first extract the value from the tuple
      */
    def getValue(a: List[_], i: Int, v: VarDef) = getIndex(v.tp, "lbl") match {
      case -1 => a(i)
      case l => a(l)
    } 

    def getPred(e: PrimitiveCalc, v:VarDef, a: List[_]) = e match {
      case Conditional(OpEq, e1, e2) =>
        if (v == getVar(e1)) {
          // return the value associated to the project in e1
          getValue(a, getIndex(e1), v)
        }else if (v == getVar(e2)){
            getValue(a, getIndex(e2), v)
        }else{
          getValue(a, -1, v) match {
            case f:SLabel => f.extract(getVar(e1).name) match {
            case None => 
              getValue(f.extract(getVar(e2).name).asInstanceOf[List[_]], getIndex(e2), getVar(e2))
            case l => 
            getValue(l.asInstanceOf[List[_]], getIndex(e1), getVar(e1)) 
          }
          case _ => None 
        }
    }}

    /**
      * Produces an anonymous function to map across an RDD[List[_]] to key appropriately
      * for a join
      */
    def joinKey(vars: Any, pred: PrimitiveCalc): Any => Tuple2[_,_] = vars match {
      case v:VarDef => 
        (a: Any) =>  a match {
          case t:Tuple2[_,_] => t // from input dictionary, need to handle this from the condition
          case _ => (List(getPred(pred, v, a.asInstanceOf[List[_]])), (a))
        }
      case (b:VarDef, c:VarDef) => 
        (a: Any) => 
          val a1 = getPred(pred, c, a.asInstanceOf[Tuple2[_,_]]._2.asInstanceOf[List[_]])
          val a2 = getPred(pred, b, a.asInstanceOf[Tuple2[_,_]]._2.asInstanceOf[List[_]])
          (List(a1, a2).filter{_ != None}, (a))
      //case (b @ (_,_), c:VarDef) => 
      //  (a: Any) => (getPred(pred, v, a.asInstanceOf[List[_]]), (a))
      //case (b,c) => 
    }

    /**
      * Initial implementation of Spark mappings and evaluation
      * // todo: reduce and unnest should read from pattern matching
      *
      */
    def evaluate(e: AlgOp): RDD[_] = e match {
      case Term(Reduce(e1, v, pred), e2) => 
        val structure = tuple2(v)
        val output = evaluate(e2).map(v1 => matchPattern(structure, e1, v1)) 
        filterRDD(output, pred, structure)
      case Term(Nest(e1, vars, grps, preds, zeros), e2) =>
        // reformat variable context
        val vars2 = tuple2(vars)
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
          val out1 = evaluate(e2)
          val out2 = evaluate(e1)
          val out = out1.cartesian(out2)
          out
        case _ =>
          val vars2 = tuple2(v).asInstanceOf[Tuple2[_,_]]
          val mapFun_e2 = joinKey(vars2._1, p)
          val mapFun_e1 = joinKey(vars2._2, p)
          // not doing outer join
          val out1 = evaluate(e2).map(mapFun_e2(_))
          val out2 = evaluate(e1).map(mapFun_e1(_))
          val out = out1.join(out2).map{ case (k,v) => v }
          out
      }
      output
      case Term(Join(v, p), Term(e1, e2)) => p match {
        case Constant(true, _) => evaluate(e2).cartesian(evaluate(e1))
        case _ =>
          val vars2 = tuple2(v).asInstanceOf[Tuple2[_,_]]
          val mapFun_e2 = joinKey(vars2._1, p)
          val mapFun_e1 = joinKey(vars2._2, p)
          val out1 = evaluate(e2).map(mapFun_e2(_))
          val out2 = evaluate(e1).map(mapFun_e1(_))
          val out = out1.join(out2).map{ case (k,v) => v }
          out
      }
      // { (v, w) | v <- X, w <- v.path }
      case Term(Unnest(vars, proj, pred), vterm) =>
        val i = getIndex(proj)
        val tuples = tuple2(vars)
        val output = evaluate(vterm).flatMap{
            v => accessElement(v, i).asInstanceOf[Iterable[_]].map{ 
                  v1 => ((v.asInstanceOf[List[_]].take(i) ++ v.asInstanceOf[List[_]].drop(i+1)), v1) }
          }
        filterRDD(output, pred, tuple2(vars))
      // same as unnest 
      case Term(OuterUnnest(vars, proj, pred), vterm) =>
        val i = getIndex(proj)
        val output = evaluate(vterm).flatMap{
            v => accessElement(v, i).asInstanceOf[Iterable[_]] match {
              case Nil => List(((v.asInstanceOf[List[_]].take(i) ++ v.asInstanceOf[List[_]].drop(i+1)),None))
              case v2 => v2.map{ v1 => ((v.asInstanceOf[List[_]].take(i) ++ v.asInstanceOf[List[_]].drop(i+1)), v1) }
            }
          }
        filterRDD(output, pred, tuple2(vars))
      // v <- X
      case Select(x, v, pred) => 
        val output = filterRDD(evaluateBag(x), pred, v)
        output
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
      case InputR(n, d, t) => // flat input?
        val data = d.asInstanceOf[List[Map[String,Any]]].map(m => m.values.toList)
        ctx.getOrElseUpdate(n, sc.parallelize(data))
      case CLookup(lbl, dict) => dict match {
        // lbl is also projected here, it's the label in the first position
        // will need to use that when the data isn't coming in shredded
        case InputBagDict(d, ftp, tdict) =>
          val data = d.toList.flatMap{
            case (l, v) => v.asInstanceOf[List[Map[String,Any]]].map{
              v2 => (List(l), v2.values.toList)}
          }
          ctx.getOrElseUpdate(lbl.toString, sc.parallelize(data)) 
        case _ => sys.error("unsupported evaluation for "+e)
      }
      case _ => sys.error("unsupported evaluation for "+e)
    }}

  }
  
  object SparkEvaluator{
    def apply(sc: SparkContext) = new SparkEvaluator(sc)
  }

}
