package framework.plans

import framework.common._

/** Optimizer used for plans from BatchUnnester **/
object BatchOptimizer extends Extensions {

  val extensions = new Extensions{}
  import extensions._

  def applyAll(e: CExpr): CExpr = {
//    val pushedProjections = push(e)
//e
    pushUnnest(e)
//    pushUnnest(pushedProjections)
  }

  /** TODO Yao's awesome optimizer **/
  def pushUnnest(e: CExpr): CExpr = fapply(e,  {

    case DFOuterUnnest(
    AddIndex(DFOuterJoin(e1, x2, e2, x3, Constant(true), fs1), index),
        x7, field, x4, Equals(Project(x4_expr, f1), Project(x5, f2)), fs2) =>{


        print(x4.toString + "..." + x4_expr.toString)
        if(x4.toString.equals(x4_expr.toString)){ // although they have different types, they should be the same after
          val unnest: DFOuterUnnest = DFOuterUnnest(
            AddIndex(e2, index), x3, field, x4, Constant(true), Nil)
          val cond = Equals(Project(x4_expr, f1), Project(x5, f2))
          DFOuterJoin(unnest, x7, e1, x2, cond, fs2)
        }else{
          e
        }
      }

  })


  /** Push projections in plans made of batch operations
    * @param e input plan from BatchUnnester
    * @param fs set of attributes, default empty set
    * @todo capture attributes from filter
    */
  def push(e: CExpr, fs: Set[String] = Set()): CExpr = e match {
    
    case DFProject(in, v, filter, fields) => 
      val tfields = fields.toSet ++ collect(filter)
      val pin = push(in, tfields ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFProject(pin, nv, filter, tfields.toList)

    case DFUnnest(in, v, path, v2, filter, fields) =>
      val pin = push(in, fields.toSet ++ fs + path)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFUnnest(pin, nv, path, v2, filter, (fields.toSet ++ fs).toList)

    case DFOuterUnnest(in, v, path, v2, filter, fields) =>
      val pin = push(in, fields.toSet ++ fs + path)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFOuterUnnest(pin, nv, path, v2, filter, (fields.toSet ++ fs).toList)

    case DFJoin(left, v, right, v2, cond, fields) =>
      val jcols = collect(cond)
      val lpin = push(left, fields.toSet ++ fs ++ jcols) 
      val rpin = push(right, fields.toSet ++ fs ++ jcols)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      DFJoin(lpin, lv, rpin, rv, cond, fields)

    case DFOuterJoin(left, v, right, v2, cond, fields) =>
      val jcols = collect(cond)
      val lpin = push(left, fields.toSet ++ fs ++ jcols)
      val rpin = push(right, fields.toSet ++ fs ++ jcols)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      DFOuterJoin(lpin, lv, rpin, rv, cond, fields)

    case DFNest(in, v, key, value, filter, nulls, ctag) => 
      // adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey0 = (key.toSet & fs) ++ indices 
      val nkey = if (nkey0.isEmpty) key.toSet else nkey0

      val pin = push(in, nkey ++ value.inputColumns ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFNest(pin, nv, nkey.toList, value, filter, nulls, ctag)

    case DFReduceBy(e1 @ DFProject(in, v, filter:Record, fields), v2, key, value) =>
      // adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey0 = (key.toSet & fs) ++ indices 
      val nkey = if (nkey0.isEmpty) key.toSet else nkey0

      val nfs = nkey ++ value.toSet ++ fs ++ collect(filter)
      val pin = push(in, nfs)
      val nv = Variable.fromBag(v.name, pin.tp)

      val pin2 = DFProject(pin, nv, Record(filter.fields.filter(f => nfs(f._1))), nfs.toList)
      val nv2 = Variable.fromBag(v2.name, pin2.tp)
      DFReduceBy(pin2, nv2, nkey.toList, value)

    case DFReduceBy(in, v, key, value) =>
      //adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey = (key.toSet & fs) ++ indices

      val pin = push(in, nkey ++ value.toSet ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFReduceBy(pin, nv, nkey.toList, value)

    case Select(in, v, p, v2:Variable) =>
      val ptp = v.tp.attrs.filter(f => fs(f._1))
      val nv = Variable(v2.name, RecordCType(ptp))
      Select(in, v, p, nv)

    case CGet(e1) => CGet(push(e1, fs))
    case AddIndex(e1, name) => AddIndex(push(e1, fs), name)
    case FlatDict(e1) => FlatDict(push(e1, fs))
    case GroupDict(e1) => GroupDict(push(e1, fs))
    case CNamed(n, e1) => CNamed(n, push(e1))
    case LinearCSet(fs) => LinearCSet(fs.map(f => push(f)))
    case _ => e
  }

}