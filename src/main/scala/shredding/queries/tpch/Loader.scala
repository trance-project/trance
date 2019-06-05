package shredding.queries.tpch

import shredding.loader.csv._
import shredding.core._
import scala.collection.mutable.ArrayBuffer
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

object TPCHSchema {
  def getSchema(folderLocation: String, scalingFactor: Double): Schema = {
    val lineItemTable = {
      val L_ORDERKEY: Attribute = "L_ORDERKEY" -> IntType
      val L_LINENUMBER: Attribute = "L_LINENUMBER" -> IntType
      val L_SHIPMODE = Attribute("L_SHIPMODE", StringType, List(Compressed))
      val L_SHIPINSTRUCT = Attribute("L_SHIPINSTRUCT", StringType, List(Compressed))

      new Table("LINEITEM", List(
        L_ORDERKEY,
        "L_PARTKEY" -> IntType,
        "L_SUPPKEY" -> IntType,
        L_LINENUMBER,
        "L_QUANTITY" -> DoubleType,
        "L_EXTENDEDPRICE" -> DoubleType,
        "L_DISCOUNT" -> DoubleType,
        "L_TAX" -> DoubleType,
        "L_RETURNFLAG" -> StringType,
        "L_LINESTATUS" -> StringType,
        "L_SHIPDATE" -> StringType,
        "L_COMMITDATE" -> StringType,
        "L_RECEIPTDATE" -> StringType,
        L_SHIPINSTRUCT,
        L_SHIPMODE,
        ("L_COMMENT" -> StringType)),
        ArrayBuffer(
          PrimaryKey(List(L_ORDERKEY, L_LINENUMBER)),
          ForeignKey("LINEITEM", "ORDERS", List(("L_ORDERKEY", "O_ORDERKEY"))),
          ForeignKey("LINEITEM", "PARTSUPP", List(("L_PARTKEY", "PS_PARTKEY"), ("L_SUPPKEY", "PS_SUPPKEY")))),
        folderLocation + "lineitem.tbl", (scalingFactor * 6000000).toLong)
    }

    val regionTable = {
      val R_REGIONKEY: Attribute = "R_REGIONKEY" -> IntType
      val R_NAME = Attribute("R_NAME", StringType, List(Compressed))

      new Table("REGION", List(
        R_REGIONKEY,
        R_NAME,
        ("R_COMMENT" -> StringType)),
        ArrayBuffer(PrimaryKey(List(R_REGIONKEY)),
          Continuous(R_REGIONKEY, 0)),
        folderLocation + "region.tbl", 5)
    }

    val nationTable = {
      val N_NATIONKEY: Attribute = "N_NATIONKEY" -> IntType
      val N_NAME: Attribute = Attribute("N_NAME", StringType, List(Compressed))

      new Table("NATION", List(
        N_NATIONKEY,
        N_NAME,
        "N_REGIONKEY" -> IntType,
        ("N_COMMENT" -> StringType)),
        ArrayBuffer(
          PrimaryKey(List(N_NATIONKEY)),
          Continuous(N_NATIONKEY, 0),
          ForeignKey("NATION", "REGION", List(("N_REGIONKEY", "R_REGIONKEY")))),
        folderLocation + "nation.tbl", 25)
    }

    val supplierTable = {
      val sk: Attribute = "S_SUPPKEY" -> IntType

      new Table("SUPPLIER", List(
        sk,
        "S_NAME" -> StringType,
        ("S_ADDRESS" -> StringType),
        "S_NATIONKEY" -> IntType,
        ("S_PHONE" -> StringType),
        "S_ACCTBAL" -> DoubleType,
        ("S_COMMENT" -> StringType)),
        ArrayBuffer(
          PrimaryKey(List(sk)),
          Continuous(sk, 1),
          ForeignKey("SUPPLIER", "NATION", List(("S_NATIONKEY", "N_NATIONKEY")))),
        folderLocation + "supplier.tbl", (scalingFactor * 10000).toLong)
    }

    val partTable = {
      val P_PARTKEY: Attribute = "P_PARTKEY" -> IntType
      val P_BRAND = Attribute("P_BRAND", StringType, List(Compressed))
      val P_TYPE = Attribute("P_TYPE", StringType, List(Compressed))
      val P_MFGR = Attribute("P_MFGR", StringType, List(Compressed))
      val P_CONTAINER = Attribute("P_CONTAINER", StringType, List(Compressed))

      new Table("PART", List(
        P_PARTKEY,
        "P_NAME" -> StringType,
        P_MFGR,
        P_BRAND,
        P_TYPE,
        "P_SIZE" -> IntType,
        P_CONTAINER,
        "P_RETAILPRICE" -> DoubleType,
        ("P_COMMENT" -> StringType)),
        ArrayBuffer(
          PrimaryKey(List(P_PARTKEY)),
          Continuous(P_PARTKEY, 1)),
        folderLocation + "part.tbl", (scalingFactor * 200000).toLong)
    }

    val partsuppTable = {
      val pk: Attribute = "PS_PARTKEY" -> IntType
      val sk: Attribute = "PS_SUPPKEY" -> IntType

      new Table("PARTSUPP", List(
        pk,
        sk,
        "PS_AVAILQTY" -> IntType,
        "PS_SUPPLYCOST" -> DoubleType,
        ("PS_COMMENT" -> StringType)),
        ArrayBuffer(
          PrimaryKey(List(pk, sk)),
          ForeignKey("PARTSUPP", "PART", List(("PS_PARTKEY", "P_PARTKEY"))),
          ForeignKey("PARTSUPP", "SUPPLIER", List(("PS_SUPPKEY", "S_SUPPKEY")))),
        folderLocation + "partsupp.tbl", (scalingFactor * 800000).toLong)
    }

    val customerTable = {
      val ck: Attribute = "C_CUSTKEY" -> IntType
      val C_MKTSEGMENT = Attribute("C_MKTSEGMENT", StringType, List(Compressed))

      new Table("CUSTOMER", List(
        ck,
        ("C_NAME" -> StringType),
        ("C_ADDRESS" -> StringType),
        "C_NATIONKEY" -> IntType,
        ("C_PHONE" -> StringType),
        "C_ACCTBAL" -> DoubleType,
        C_MKTSEGMENT,
        ("C_COMMENT" -> StringType)),
        ArrayBuffer(
          PrimaryKey(List(ck)),
          Continuous(ck, 1),
          ForeignKey("CUSTOMER", "NATION", List(("C_NATIONKEY", "N_NATIONKEY")))),
        folderLocation + "customer.tbl", (scalingFactor * 150000).toLong)
    }

    val ordersTable = {
      val O_ORDERKEY: Attribute = "O_ORDERKEY" -> IntType
      val O_COMMENT = Attribute("O_COMMENT", StringType, List(Compressed))
      val O_ORDERPRIORITY = Attribute("O_ORDERPRIORITY", StringType)

      new Table("ORDERS", List(
        O_ORDERKEY,
        "O_CUSTKEY" -> IntType,
        "O_ORDERSTATUS" -> StringType,
        "O_TOTALPRICE" -> DoubleType,
        "O_ORDERDATE" -> StringType,
        O_ORDERPRIORITY,
        ("O_CLERK" -> StringType),
        "O_SHIPPRIORITY" -> IntType,
        O_COMMENT),
        ArrayBuffer(
          PrimaryKey(List(O_ORDERKEY)),
          ForeignKey("ORDERS", "CUSTOMER", List(("O_CUSTKEY", "C_CUSTKEY")))),
        folderLocation + "orders.tbl", (scalingFactor * 1500000).toLong)
    }

    val tpchSchema = new Schema(List(lineItemTable, regionTable, nationTable, supplierTable, partTable, partsuppTable, customerTable, ordersTable))

    val YEARS = 7

    tpchSchema
  }
}

object TPCHLoader {

  val datapath: String = "/placeholder/i/deleted/the/config"
  def tpchSchema: Schema = TPCHSchema.getSchema(datapath, getScalingFactor)
  def getScalingFactor: Double = datapath.slice(datapath.lastIndexOfSlice("sf") + 2, datapath.length - 1).toDouble
  def getTable(tableName: String): Table = tpchSchema.tables.find(t => t.name == tableName).get

  import Loader.loadTable

  //def loadRegion() = loadTable[Region](getTable("REGION"))

  def loadPartsupp() = loadTable[PartSupp](getTable("PARTSUPP"))

  def loadPart() = loadTable[Part](getTable("PART"))

  //def loadNation() = loadTable[NATIONRecord](getTable("NATION"))

  def loadSupplier() = loadTable[Supplier](getTable("SUPPLIER"))

  def loadLineitem() = loadTable[Lineitem](getTable("LINEITEM"))

  def loadOrders() = loadTable[Orders](getTable("ORDERS"))

  def loadCustomer() = loadTable[Customer](getTable("CUSTOMER"))
}

