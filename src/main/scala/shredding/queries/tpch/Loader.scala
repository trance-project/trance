package shredding.queries.tpch

import shredding.loader.csv._

object TPCHLoader {

  def tpchSchema: Schema = TPCHSchema.getSchema(Config.datapath, getScalingFactor)
  def getScalingFactor: Double = Config.datapath.slice(Config.datapath.lastIndexOfSlice("sf") + 2, Config.datapath.length - 1).toDouble
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

