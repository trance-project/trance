package shredding.nrc2

import shredding.Utils.ind
import shredding.core._ 

trait Dictionary extends BaseDict{
  this: NRC with NRCTransforms =>

  case class OutputBagDict(lbl: LabelExpr, flat: BagExpr, tupleDict: TupleDict) extends BagDict {
    def union(that: Dict): OutputBagDict = that match {
      case OutputBagDict(lbl2, flat2, tupleDict2) if flat.tp == flat2.tp =>
        (lbl, lbl2) match {
          //          case (NewLabel(free1), NewLabel(free2)) =>
          //            val free = free1 ++ free2
          //            OutputBagDict(NewLabel(free), Union(flat, flat2), tupleDict.union(dict2))

          case (LabelRef(ld1), LabelRef(ld2)) =>
            val lbl = LabelRef(LabelDef())
            OutputBagDict(lbl, Union(flat, flat2), tupleDict.union(tupleDict2))

          case _ => sys.error("Illegal dictionary union - unknown label type")
        }
      case _ => sys.error("Illegal dictionary union")
    }
    override def toString: String = s"""
      |( ${lbl.asInstanceOf[LabelRef].labelDef.toString} --> ${Printer.quote(flat)},
      |${ind(tupleDict.toString)}
      |)""".stripMargin

    def flatBagTp: BagType = flat.tp
  }

}
