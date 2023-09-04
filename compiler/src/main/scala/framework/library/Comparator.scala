package framework.library

sealed trait Comparator
case object Equals extends Comparator

case object NotEqual extends Comparator

case object GreaterThan extends Comparator

case object GreaterThanEqual extends Comparator

case object LessThan extends Comparator

case object LessThanEqual extends Comparator

