package shredding.nrc


/**
  * Comparison operators
  */
sealed trait OpCmp
case object OpEq extends OpCmp { override def toString = "=" }
case object OpNe extends OpCmp { override def toString = "!=" }
//case object OpGt extends OpCmp { override def toString = ">" }
//case object OpGe extends OpCmp { override def toString = ">=" }