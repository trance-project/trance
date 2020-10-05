package framework.loader.csv

import framework.common._
import scala.language.implicitConversions
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

/**
  * Code from: https://github.com/epfldata/dblab/tree/develop/components/src/main/scala/ch/epfl/data/dblab/schema
  */

sealed trait Constraint
case class PrimaryKey(attributes: List[Attribute]) extends Constraint
case class ForeignKey(ownTable: String, referencedTable: String, attributes: List[(String, String)], var selectivity: Double = 1) extends Constraint {
  def foreignTable(implicit s: Schema): Option[Table] =
    s.tables.find(t => t.name == referencedTable)
  def thisTable(implicit s: Schema): Option[Table] =
    s.tables.find(t => t.name == ownTable)
  def matchingAttributes(implicit s: Schema): List[(Attribute, Attribute)] =
    attributes.map {
      case (localAttr, foreignAttr) =>
        thisTable.get.attributes.find(a => a.name == localAttr).get -> foreignTable.get.attributes.find(a => a.name == foreignAttr).get
    }
}
case class NotNull(attribute: Attribute) extends Constraint
case class Unique(attribute: Attribute) extends Constraint
case class AutoIncrement(attribute: Attribute) extends Constraint
/**
 * Specifies that the rows of a given table are continues (which means it is
 * also a preimary key) with respect to the given attribute. The offset specifies
 * the offset between the index of a row and the value of the attribute.
 */
case class Continuous(attribute: Attribute, offset: Int) extends Constraint
object Compressed extends Constraint

case class Schema(tables: ArrayBuffer[Table]) {
  def this(tables: List[Table]) = this(ArrayBuffer[Table]() ++ tables)
  def findTable(name: String): Option[Table] = {
    val ncheck = name match {
      case y if name.contains("IBag") => y.split("_")(1)
      case _ => name
    }
    tables.find(t => t.name.toLowerCase() == ncheck.toLowerCase())
  }
  //def findTableByType(tpe: Type): Option[Table] = tables.find(t => t.name) //+ "Tuple" == tpe.name)
  def findAttribute(attrName: String): Option[Attribute] = tables.map(t => t.attributes).flatten.find(attr => attr.name == attrName)
}

object Schema{
  def apply(): Schema = Schema(ArrayBuffer.empty[Table])
}

case class Table(name: String, attributes: List[Attribute], constraints: ArrayBuffer[Constraint], resourceLocator: String, var rowCount: Long = -1) {
  def primaryKey: Option[PrimaryKey] = constraints.collectFirst { case pk: PrimaryKey => pk }
  def foreignKeys: List[ForeignKey] = constraints.collect { case fk: ForeignKey => fk }.toList
  def notNulls: List[NotNull] = constraints.collect { case nn: NotNull => nn }.toList
  def uniques: List[Unique] = constraints.collect { case unq: Unique => unq }.toList
  def autoIncrement: Option[AutoIncrement] = constraints.collectFirst { case ainc: AutoIncrement => ainc }
  def continuous: Option[Continuous] = constraints.collectFirst { case cont: Continuous => cont }
  def findAttribute(attrName: String): Option[Attribute] = attributes.find(attr => attr.name == attrName)
}

case class Attribute(_name: String, dataType: Type, constraints: List[Constraint] = List(), var distinctValuesCount: Int = 0, var nullValuesCount: Long = 0) {
  def name = _name.toLowerCase()
  def hasConstraint(con: Constraint) = constraints.contains(con)
}

object Attribute {
  implicit def tuple2ToAttribute(nameAndType: (String, Type)): Attribute = Attribute(nameAndType._1, nameAndType._2)
  implicit def tuple2oftuple2ToAttribute(nameAndTypeAndConstraints: ((String, Type), List[Constraint])): Attribute =
    Attribute(nameAndTypeAndConstraints._1._1, nameAndTypeAndConstraints._1._2, nameAndTypeAndConstraints._2)
}

