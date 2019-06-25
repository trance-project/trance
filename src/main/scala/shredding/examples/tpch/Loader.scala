package shredding.examples.tpch

import scala.reflect._
import scala.reflect.runtime.universe._
import shredding.loader.csv._

object TPCHLoader {

  val datapath = Config.datapath
  def tpchSchema: Schema = TPCHSchema.getSchema()
  def getScalingFactor: Double = datapath.slice(datapath.lastIndexOfSlice("sf") + 2, datapath.length - 1).toDouble
  def getTable(tableName: String): Table = tpchSchema.tables.find(t => t.name == tableName).get

  import Loader.loadTable

  def loadRegion[T](implicit c: ClassTag[T]) = loadTable[T](getTable("REGION"))

  def loadPartsupp[T: TypeTag](implicit c: ClassTag[T]) = loadTable[T](getTable("PARTSUPP"))

  def loadPart[T: TypeTag](implicit c: ClassTag[T]) = loadTable[T](getTable("PART"))

  def loadNation[T: TypeTag](implicit c: ClassTag[T]) = loadTable[T](getTable("NATION"))

  def loadSupplier[T: TypeTag](implicit c: ClassTag[T]) = loadTable[T](getTable("SUPPLIER"))

  def loadLineitem[T: TypeTag](implicit c: ClassTag[T]) = loadTable[T](getTable("LINEITEM"))

  def loadOrders[T: TypeTag](implicit c: ClassTag[T]) = loadTable[T](getTable("ORDERS"))

  def loadCustomer[T: TypeTag](implicit c: ClassTag[T]) = loadTable[T](getTable("CUSTOMER"))
}

