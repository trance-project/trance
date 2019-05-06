package shredding.spark

import shredding.core._

trait SparkRuntime {

  sealed trait SLabel

  object SInLabel {
    private var currId = 0
    def getNextId: Int = {
      currId += 1
      currId
    }
  }

  final case class SInLabel(id: Int = SInLabel.getNextId) extends SLabel {
    override def toString: String = s"SInLabel($id)"
  }

  final case class SOutLabel(vals: Map[String, Any]){

    def extract(v:String): Any = vals.get(v) match {
      case Some(a) => a
      case None => None
    }

    def extract(i: Int): Any = extract(vals.keys.toList(i).toString)

  }

  object SOutLabel{
    def apply(vals: (String, Any)*): SOutLabel = SOutLabel(Map(vals: _*))
  }

}
