package framework.plans

import framework.common._

/** Optimizer used for plans from BatchUnnester **/
object BatchOptimizer{

  /** Push projections in plans made of batch operations
    * @param e input plan from BatchUnnester
    * @param fs set of attributes, default empty set
    * @todo capture attributes from filter
    */
  def push(e: CExpr, fs: Set[String] = Set()): CExpr = e match {
    
    case DFProject(in, v, filter, fields) => 
      val pin = push(in, fields.toSet ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFProject(pin, nv, filter, (fields.toSet & fs).toList)

    case DFUnnest(in, v, path, v2, filter, fields) =>
      val pin = push(in, fields.toSet ++ fs + path)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFUnnest(pin, nv, path, v2, filter, (fields.toSet ++ fs).toList)

    case DFOuterUnnest(in, v, path, v2, filter, fields) =>
      val pin = push(in, fields.toSet ++ fs + path)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFOuterUnnest(pin, nv, path, v2, filter, (fields.toSet ++ fs).toList)

    case DFJoin(left, v, p1, right, v2, p2, fields) =>
      val lpin = push(left, fields.toSet ++ fs + p1 
      val rpin = push(right, fields.toSet ++ fs + p2)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      DFJoin(lpin, lv, p1, rpin, rv, p2, fields)

    case DFOuterJoin(left, v, p1, right, v2, p2, fields) =>
      val lpin = push(left, fields.toSet ++ fs + p1 
      val rpin = push(right, fields.toSet ++ fs + p2)
      val lv = Variable.fromBag(v.name, lpin.tp)
      val rv = Variable.fromBag(v2.name, rpin.tp)
      DFOuterJoin(lpin, lv, p1, rpin, rv, p2, fields)

    case DFNest(in, v, key, value, filter, nulls) => 
      // adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey0 = (key.toSet & fs) ++ indices 
      val nkey = if (nkey0.isEmpty) key.toSet else nkey0

      val pin = push(in, nkey ++ value.inputColumns ++ fs)
      val nv = Variable.fromBag(v.name, pin.tp)
      DFNest(pin, nv, nkey.toList, value, filter, nulls)

    case DFReduceBy(e1 @ DFProject(in, v, filter:Record, fields), v2, key, value) =>
      // adjust key
      val indices = key.filter(k => k.contains("index")).toSet
      val nkey0 = (key.toSet & fs) ++ indices 
      val nkey = if (nkey0.isEmpty) key.toSet else nkey0

      val nfs = nkey ++ value.toSet ++ fs
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

    case AddIndex(e1, name) => AddIndex(push(e1, fs), name)
    case FlatDict(e1) => FlatDict(push(e1, fs))
    case GroupDict(e1) => GroupDict(push(e1, fs))
    case CNamed(n, e1) => CNamed(n, push(e1))
    case LinearCSet(fs) => LinearCSet(fs.map(f => push(f)))
    case _ => e
  }

}