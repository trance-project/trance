package framework.runtime

import framework.common._
import framework.nrc.BaseShredding

/**
  * Shredding of Scala input values
  */
trait ScalaShredding extends BaseShredding with ScalaRuntime {

  final case class ShredValue(flat: Any, flatTp: Type, dict: RDict)

  def shred(v: Any, tp: Type): ShredValue = tp match {
    case _: PrimitiveType =>
      ShredValue(v, tp, REmptyDict)

    case BagType(t) =>
      val b = v.asInstanceOf[List[_]]
      val sb = b.map(shred(_, t))
      // Form flat bag
      val flatBag = sb.map(_.flat)
      val flatBagTp = BagType(flatTp(t).asInstanceOf[TupleType])
      // Form tuple dictionary
      val tupleDict = sb.map(_.dict).reduce(_ union _).asInstanceOf[RTupleDict]
      // Flat value
      val lbl = RInLabel()
      val lblTp = LabelType()
      // Dict value
      val lblDict = RInBagDict(Map[RLabel, List[Any]](lbl -> flatBag), flatBagTp, tupleDict)

      ShredValue(lbl, lblTp, lblDict)

    case TupleType(as) =>
      val t = v.asInstanceOf[Map[String, Any]]
      val st = t.map { case (n, a) => n -> shred(a, as(n)) }
      val flat = st.map { case (n, s) => n -> s.flat }
      val dict = RTupleDict(st.map { case (n, s) =>
        n -> s.dict.asInstanceOf[RTupleDictAttribute]
      })

      ShredValue(flat, flatTp(tp), dict)

    case _ => sys.error("Shredding value with unknown type " + tp)
  }

  def unshred(v: ShredValue): Any = unshred(v.flat, v.flatTp, v.dict)

  def unshred(flat: Any, flatTp: Type, dict: RDict): Any = flatTp match {
    case _: PrimitiveType => flat

    case _: BagType =>
      val l = flat.asInstanceOf[List[Any]]
      val flatBagTp = flatTp.asInstanceOf[BagType]
      val tupleDict = dict.asInstanceOf[RTupleDict]
      l.map(t => unshred(t, flatBagTp.tp, tupleDict))

    case TupleType(as) =>
      val tuple = flat.asInstanceOf[Map[String, Any]]
      val tupleDict = dict.asInstanceOf[RTupleDict]
      as.map { case (n, tp) => n -> unshred(tuple(n), tp, tupleDict.fields(n)) }

    case _: LabelType =>
      val lbl = flat.asInstanceOf[RInLabel]
      val matDict = dict.asInstanceOf[RBagDict]
      matDict(lbl).map(t => unshred(t, matDict.flatBagTp.tp, matDict.dict))

    case _ => sys.error("Unshredding value with unknown type " + flatTp)
  }

}
