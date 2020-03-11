package shredding.runtime

import shredding.core._

/**
  * Runtime representations of labels and dictionaries in Scala
  */
trait ScalaRuntime {

  sealed trait RLabel

  object RInLabel {
    private var currId = 0

    def getNextId: Int = {
      currId += 1
      currId
    }
  }

  final case class RInLabel(id: Int = RInLabel.getNextId) extends RLabel {
    override def toString: String = s"RInLabel($id)"
  }

  final case class ROutLabel(vars: Map[VarDef, Any]) extends RLabel {
    override def toString: String =
      s"RNewLabel(${vars.map(v => v._1 + " := " + v._2).mkString(", ")})"
  }

  sealed trait RDict {
    def tp: DictType
  }

  trait RTupleDictAttribute extends RDict

  case object REmptyDict extends RTupleDictAttribute {
    def tp: DictType = EmptyDictType
  }

  trait RBagDict extends RTupleDictAttribute {
    def tp: BagDictType = BagDictType(flatBagTp, dict.tp)

    def apply(l: RLabel): List[Any]

    def flatBagTp: BagType

    def dict: RTupleDict
  }

  final case class RInBagDict(f: Map[RLabel, List[Any]], flatBagTp: BagType, dict: RTupleDict) extends RBagDict {
    def apply(l: RLabel): List[Any] = f(l)
  }

  class DictFn(init: Context, val f: Context => List[Any]) {
    // Store context when DictFn was created. Need only
    // input values to be stored but we keep entire context.
    val ctx: Context = Context(init.ctx.toList: _*)
  }

  final case class ROutBagDict(dictFn: DictFn, flatBagTp: BagType, dict: RTupleDict) extends RBagDict {
    def apply(l: RLabel): List[Any] = l match {
      case ROutLabel(vs) =>
        val ctx = dictFn.ctx
        vs.foreach(v => ctx.add(v._1, v._2))
        dictFn.f(ctx)
      case _: RInLabel => sys.error("Cannot evaluate ROutBagDict with RInLabel")
    }
  }

  final case class RTupleDict(fields: Map[String, RTupleDictAttribute]) extends RDict {
    val tp: TupleDictType = TupleDictType(fields.map(f => f._1 -> f._2.tp.asInstanceOf[TupleDictAttributeType]))
  }

  implicit class RDictOps(dict1: RDict) {
    def union(dict2: RDict): RDict = (dict1, dict2) match {
      case (REmptyDict, REmptyDict) =>
        REmptyDict
      case (RInBagDict(f1, tp1, d1), RInBagDict(f2, tp2, d2)) =>
        assert(tp1 == tp2)
        RInBagDict(f1 ++ f2, tp1, d1.union(d2).asInstanceOf[RTupleDict])
      case (RTupleDict(fields1), RTupleDict(fields2)) =>
        assert(fields1.keySet == fields2.keySet)
        RTupleDict(fields1.map { case (k1, d1) =>
          k1 -> d1.union(fields2(k1)).asInstanceOf[RTupleDictAttribute]
        })
      case _ => sys.error("Illegal runtime dictionary union " + dict1 + " and " + dict2)
    }
  }

}
