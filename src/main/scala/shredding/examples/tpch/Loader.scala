package shredding.examples.tpch

import shredding.loader.csv._

object TPCHLoader {

  val datapath = Config.datapath
  def tpchSchema: Schema = TPCHSchema.getSchema()
  def getScalingFactor: Double = datapath.slice(datapath.lastIndexOfSlice("sf") + 2, datapath.length - 1).toDouble
  def getTable(tableName: String): Table = tpchSchema.tables.find(t => t.name == tableName).get

  import Loader.loadTable

  def loadRegion() = loadTable[Region](getTable("REGION"))

  def loadPartsupp() = loadTable[PartSupp](getTable("PARTSUPP"))

  def loadPart() = loadTable[Part](getTable("PART"))

  def loadNation() = loadTable[Nation](getTable("NATION"))

  def loadSupplier() = loadTable[Supplier](getTable("SUPPLIER"))

  def loadLineitem() = loadTable[Lineitem](getTable("LINEITEM"))

  def loadOrders() = loadTable[Orders](getTable("ORDERS"))

  def loadCustomer() = loadTable[Customer](getTable("CUSTOMER"))
}

