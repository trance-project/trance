package framework.optimize

import framework.common._
import framework.loader.csv._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{Map => MMap}
import framework.plans._

/** Optimizer used for plans from BatchUnnester **/
class Optimizer(schema: Schema = Schema()) extends Extensions {

  val extensions = new Extensions{}
  import extensions._


  var joinConds = List.empty[CExpr]

  // push projections
  def applyPush(e: CExpr): CExpr = {
    val o1 = pushUnnest(e)
    val o2 = pushCondition(o1)
    val o3 = removeUnnecProj(push(o2))
    o3
  }

  // push projections and aggregation
  def applyAll(e: CExpr): CExpr = {
    val o1 = pushUnnest(e)
  	val o2 = pushCondition(o1)
  	val o3 = removeUnnecProj(push(o2))
    val o4 = pushAgg(o3)
    o4
  }

  /** Push projections in plans made of batch operations
    * @param e input plan from BatchUnnester
    * @param fs set of attributes, default empty set
    * @todo capture attributes from filter
    */
  def push(e: CExpr, fs: Set[String] = Set()): CExpr = e match {

    case Projection(in, v, filter, fields, l) => 
      val tfields = fs ++ collect(filter)
      val pin = push(in, tfields ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      val nfilter = replace(filter, nv)
      Projection(pin, nv, nfilter, tfields.toList, l)

    case s @ Select(in, v, p, l) =>
      val ptp = v.tp.attrs.filter(f => fs(f._1))
      val pin = push(in, ptp.keySet)
      val nv = Variable.freshFromBag(pin.tp)
      val nrec = Record(ptp.map(f => (f._1, Project(nv, f._1))))
      p match {
        case Constant(true) => 
          removeUnnecProj(Projection(pin, nv, nrec, ptp.keySet.toList, l))
        case _ => 
          Projection(Select(pin, nv, p, l), nv, nrec, ptp.keySet.toList, l)
      }
      
    case Unnest(in, v, path, v2, filter, fields, l) =>
      val pin = push(in, fields.toSet ++ fs + path)
      val nv = Variable.fromBag(v.name, pin.tp)
      val nfields = (fields.toSet ++ fs) & (nv.tp.attrs.keySet ++ v2.tp.attrs.keySet)
      Unnest(pin, nv, path, v2, filter, nfields.toList, l)

    case OuterUnnest(in, v, path, v2, filter, fields, l) =>
      val pin = push(in, fields.toSet ++ fs + path)
      val nv = Variable.fromBag(v.name, pin.tp)
      val nfields = (fields.toSet ++ fs) & (nv.tp.attrs.keySet ++ v2.tp.attrs.keySet)
      OuterUnnest(pin, nv, path, v2, filter, nfields.toList, l)

    case Join(left, v, right, v2, cond, fields, l) =>
      joinConds = joinConds :+ cond
      val jcols = collect(cond)
      val nfields = fs ++ jcols
      val lpin = push(left, nfields) 
      val rpin = push(right, nfields)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      Join(lpin, lv, rpin, rv, cond, nfields.toList, l)

    case OuterJoin(left, v, right, v2, cond, fields, l) =>
      val jcols = collect(cond)
      val nfields = fs ++ jcols
      val lpin = push(left, nfields)
      val rpin = push(right, nfields)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      OuterJoin(lpin, lv, rpin, rv, cond, nfields.toList, l)

    case Nest(in, v, key, value, filter, nulls, ctag, l) => 
      // adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey0 = (key.toSet & fs) ++ indices 
      val nkey = if (nkey0.isEmpty) key.toSet else nkey0

      val pfs = nkey ++ collect(value) ++ fs
      val pin = push(in, pfs)
      val nv = Variable.fromBag(v.name, pin.tp)

      // push projections before performing nest
      val nrecFields = nkey.map(k => k -> Project(nv, k)).toMap ++ collect(replace(value, nv)).map(v1 => v1 -> Project(nv, v1))
      val nrec = Record(nrecFields)
      // this creates a nasty double projection issue
      val npin = Projection(pin, nv, nrec, nrecFields.keySet.toList, l)
      val nv2 = Variable.freshFromBag(npin.tp)

      Nest(npin, nv2, nkey.toList, replace(value, nv2), filter, collect(value).toList, ctag, l)

    case Reduce(e1 @ Projection(in, v, filter, fields, l), v2, key, value, l2) =>
      // adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey0 = (key.toSet & fs) ++ indices 
      val nkey = if (nkey0.isEmpty) key.toSet else nkey0

      // collect variables
      val vs = nkey ++ value.toSet ++ fs ++ indices

      // filter out unnecessary values in record
      val nfilter = filter match {
    		case Record(ffs) => Record(ffs.filter(f => vs(f._1)))
        case If(cond, Sng(Record(f1)), Some(Sng(Record(f2)))) => 
    			 If(cond, Sng(Record(f1.filter(f => vs(f._1)))), 
    			 	Some(Sng(Record(f2.filter(f => vs(f._1))))))
    		case _ => sys.error(s"implementation missing for $filter")
	    } 

      // create a new projection
	    val nfs = collect(nfilter)
      val pin = push(in, nfs)
      val nv = Variable.fromBag(v.name, pin.tp)
      val pin2 = Projection(pin, nv, nfilter, nfs.toList, l2)

      // creat a new reduce
      val nv2 = Variable.fromBag(v2.name, pin2.tp)
      val scheck = nfilter.tp.attrs.keySet
      Reduce(pin2, nv2, (nkey & scheck).toList, value, l2)

    case Reduce(in, v, key, value, l) =>
      //adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey = (key.toSet & fs) ++ indices

      val pin = push(in, nkey ++ value.toSet ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      Reduce(pin, nv, nkey.toList, value, l)

    case CGet(e1) => CGet(push(e1, fs))

    case AddIndex(e1, name) => 
      if (fs(name)) AddIndex(push(e1, fs), name)
      else push(e1, fs)

    case FlatDict(e1) => FlatDict(push(e1, fs))
    case GroupDict(e1) => GroupDict(push(e1, fs))
    case CNamed(n, e1) => CNamed(n, push(e1))

    case LinearCSet(fs) => LinearCSet(fs.map(f => push(f)))

    case i @ InputRef(name, tp) => 
      val fields = fs & tp.attrs.keySet
      if (fields.nonEmpty && !hasComplexLabel(tp)) {
        val v = Variable.freshFromBag(tp)
        val nrec = Record(tp.attrs.flatMap( f => 
          if (fields(f._1)) List((f._1, Project(v, f._1))) else Nil).toMap)
        Projection(i, v, nrec, nrec.fields.keySet.toList, 0)
      } else i

    case RemoveNulls(CDeDup(Projection(in, v, f1:Record, f2, l), l2)) => 
      val ids = v.tp.attrs.keySet.filter(f => f.contains("_index"))
      val atts = fs ++ collect(f1) ++ ids
      val nrec = if (fs.nonEmpty) Record(f1.fields.filter(f => atts.contains(f._1))) else f1
      val pin = push(in, atts)
      val nv = Variable.fromBag(v.name, pin.tp)
      RemoveNulls(CDeDup(Projection(pin, nv, nrec, fs.toList, l), l2))

    case CDeDup(e1, l) => CDeDup(push(e1, fs), l)

    case RemoveNulls(in) => RemoveNulls(push(in, fs))

    case _ => e
  }

  def hasComplexLabel(tp: Type): Boolean = {
    val check = tp.attrs.filter(f => f._2 match 
      {case LabelType(fs) if fs.size > 1 => true; case _ => false }).size
    check > 0
  }

  def removeUnnecProj(e: CExpr): CExpr = fapply(e, {
    case Projection(r:Reduce, v, p:Record, f, _) => 
      val attrs = (r.keys ++ r.values).toSet
      val outs = p.fields.keySet
      if (attrs == outs) r else e
    case Projection(in, _, r @ Record(fs), _, _) 
      if (fs.keySet == collect(r) && !hasComplexLabel(r.tp)) => in

  })

  /** Returns true if an expression is a base expression 
    * (ie. the input relations or simple operations 
    * on top of them).
    * @param e CExpr input expression
    * @return true if it is a base expression, false otherwise
    **/
  private def isBase(e: CExpr): Boolean = e match {
    case FlatDict(e1) => isBase(e1)
    case AddIndex(e1, _) => isBase(e1)
    case _:InputRef => true
    case _ => false
  }

  /** Checks if a primary key is being used for an aggregate key
    * This is done only for the "base" expressions, ie. the 
    * input relations and simple operations on top of them
    * (see isBase above).
    * @param e expression that should represent the base input
    * @param keys set of aggregation keys from pushAgg
    * @return true if key set contains a primary key, else false
    **/
  private def baseKeyCheck(e: CExpr, keys: Set[String]): Boolean = e match {
    case InputRef(name, tp) => schema.findTable(name) match {
        case Some(tbl) => tbl.primaryKey match {
          case Some(pk) => pk.attributes.find(k => keys(k.name)) match {
            case Some(b) => true
            case _ => false
          }
          case _ => false
        }
        case _ => false
      }
    case AddIndex(e1, _) => baseKeyCheck(e1, keys)
    case FlatDict(e1) => baseKeyCheck(e1, keys)
    case _ => false
  }

  // avoid doing local aggregation on a bag of single element tuples
  private def singleElementBag(e: Type): Boolean = e match {
    case BagCType(tup) => singleElementBag(tup)
    case RecordCType(ms) => (ms.size == 1 && ms.head._1 == "element")
    case _ => false
  }

  /** Push aggregates to local operations, while persisting orignal aggregation.
    * @param e plan or subplan
    * @param keys set of key values relevant to current location in plan
    * @param values set of values relevant to current location in plan
    * @return plan with local aggregations where relevant 
    */
  def pushAgg(e: CExpr, keys: Set[String] = Set.empty, values: Set[String] = Set.empty): CExpr = fapply(e, {
    
    // base case
    case Reduce(e1, v, keys, value, l) =>
      Reduce(pushAgg(e1, keys.toSet, value.toSet), v, keys, value, l)

    case Select(in, v1, p, l) if keys.nonEmpty && values.nonEmpty && isBase(in) =>
      val attrs = v1.tp.attrs.keySet
      if (!baseKeyCheck(in, attrs)){
        val nkeys = attrs & keys
        val nvalues = attrs & values
        CReduceBy(e, v1, nkeys.toList, nvalues.toList)
      }else e

    case Projection(in, v, f1, fs, l) if keys.nonEmpty && values.nonEmpty => 
      // capture column renaming
      val nameMap: Map[String, String] = f1 match {
        case Record(ms) => ms.flatMap(f => f._2 match {
          case Project(e2, f2) => List((f._1 -> f2))
          case _ => Nil
        }).toMap
      }

      val nkeys = keys.map(k => nameMap.getOrElse(k, k))
      val nvalues = collect(f1).map(k => nameMap.getOrElse(k, k)) -- nkeys

      Projection(pushAgg(in, nkeys, nvalues), v, f1, fs, l)

    case un:UnnestOp => 

      val attrs = un.fields.toSet 
      val rkeys = attrs & keys
      val rvalues = attrs & values

      // todo make sure to see a base check here?
      if (rkeys.nonEmpty && rvalues.nonEmpty && !singleElementBag(un.v2.tp)){
        val nv = Variable.freshFromBag(un.tp)
        CReduceBy(un, nv, rkeys.toList, rvalues.toList)
      }else e

    case ej:JoinOp =>

      val condkeys = collect(ej.cond)

      val lattrs = ej.v.tp.attrs.keySet
      val lkeys = keys.filter(f => lattrs(f)) ++ condkeys.filter(c => lattrs(c))

      val rattrs = ej.v2.tp.attrs.keySet
      val rkeys = keys.filter(f => rattrs(f)) ++ condkeys.filter(c => rattrs(c))

      val lpush = pushAgg(ej.left, lkeys, values.filter(f => lattrs(f)))
      val rpush = pushAgg(ej.right, rkeys, values.filter(f => rattrs(f)))

      if (ej.jtype == "inner") Join(lpush, ej.v, rpush, ej.v2, ej.cond, ej.fields, ej.level)
      else OuterJoin(lpush, ej.v, rpush, ej.v2, ej.cond, ej.fields, ej.level)

    case v:Variable if keys.nonEmpty && values.nonEmpty =>
      // do we do a baseCheck here?
      CReduceBy(e, v, keys.toList, values.toList)

  })

  /** The below functions are partially integrated from the experiment_fix branch **/

  private def validateMatch(t1: Type, f1: String, t2: Type, f2: String): Boolean = 
    (t1.attrs.get(f1).isDefined && t2.attrs.get(f2).isDefined) ||
      (t1.attrs.get(f2).isDefined && t2.attrs.get(f1).isDefined)

  def pushUnnest(e: CExpr): CExpr = fapply(e, {

    case OuterUnnest(
      AddIndex(OuterJoin(e1, x2, e2, x3, Constant(true), fs1, l), index),
        x7, field, x4, Equals(Project(x4_expr, f1), Project(x5, f2)), fs2, l2)
          if validateMatch(x2.tp, f1, x4.tp, f2) => {

        //        if(x4.toString.equals(x4_expr.toString)){
        //      if(x2.tp.attrs.get(f1).isDefined && x4.tp.attrs.get(f2).isDefined){
        val unnest: Unnest = Unnest(
          AddIndex(e2, index), x3, field, x4, Constant(true), Nil, l)
        val cond = Equals(Project(x4_expr, f1), Project(x5, f2))
        OuterJoin(pushUnnest(e1), x2, unnest, x7, cond, fs2, l2)
    }

   }

  )


  def pushCondition(e: CExpr): CExpr = fapply(e, {
    case Projection(OuterJoin(e1, v1, e2, v2, Constant(true), fs1, l), v3,
      jc @ If(cond @ Equals(Project(_, f1), Project(_, f2)), s1, s2), fs2, l2) =>
      Projection(OuterJoin(e1, v1, e2, v2, cond, fs1, l), v3, 
        If(Equals(Project(v3, f2), Null),s1, s2), fs2, l2)
  })

}

object Optimizer {
  def apply(schema: Schema = Schema()): Optimizer = new Optimizer(schema)
}
