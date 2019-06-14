package shredding.examples.tpch

import shredding.loader.csv._
import shredding.core._
import scala.collection.mutable.ArrayBuffer

/**
  * Schema file for TPCH, which supports loading to TPCH relation from csv files
  */

case class PartSupp(ps_partkey: Int, ps_suppkey: Int, ps_availqty: Int, ps_supplycost: Double, ps_comment: String)

case class Part(p_partkey: Int, p_name: String, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_retailprice: Double, p_comment: String)

case class Customer(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Int, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String)

case class Orders(o_orderkey: Int, o_custkey: Int, o_orderstatus: String, o_totalprice: Double, o_orderdate: String, o_orderpriority: String, o_clerk: String, o_shippriority: Int, o_comment: String)

case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String, l_comment: String)

case class Supplier(s_suppkey: Int, s_name: String, s_address: String, s_nationkey: Int, s_phone: String, s_acctbal: Double, s_comment: String)

case class Region(r_regionkey: Int, r_name: String, r_comment: String)

case class Nation(n_nationkey: Int, n_name: String, n_regionkey: Int, n_comment: String)

object TPCHSchema {
  val folderLocation = Config.datapath
  val scalingFactor = 1
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

  def getSchema(): Schema = {

    val tpchSchema = new Schema(List(lineItemTable, regionTable, nationTable, supplierTable, partTable, partsuppTable, customerTable, ordersTable))

    val YEARS = 7

    tpchSchema
  }

  def tableType(t: Table): BagType = BagType(TupleType(t.attributes.map(a => 
                                      a.name -> a.dataType.asInstanceOf[TupleAttributeType]).toMap))

  val parttype = tableType(partTable)

  val customertype = tableType(customerTable) 

  val orderstype = tableType(ordersTable) 

  val lineittype = tableType(lineItemTable) 

  val suppliertype = tableType(supplierTable) 

  val partsupptype = tableType(partsuppTable) 

  val nationtype = tableType(nationTable)

  val regiontype = tableType(regionTable)

  var tpchInputs:Map[Type, String] = Map(partsupptype.tp -> "PartSupp", 
                                     suppliertype.tp -> "Supplier", 
                                     lineittype.tp -> "Lineitem", 
                                     orderstype.tp -> "Orders",
                                     customertype.tp -> "Customer", 
                                     parttype.tp -> "Part",
                                     nationtype.tp -> "Nation",
                                     regiontype.tp -> "Region")
  
  var tpchShredInputs:Map[Type, String] = Map(
    BagDictType(partsupptype, TupleDictType(Map[String, TupleDictAttributeType]())) -> "PartSupp", 
    BagDictType(suppliertype, TupleDictType(Map[String, TupleDictAttributeType]())) -> "Supplier", 
    BagDictType(lineittype, TupleDictType(Map[String, TupleDictAttributeType]())) -> "Lineitem", 
    BagDictType(orderstype, TupleDictType(Map[String, TupleDictAttributeType]())) -> "Orders",
    BagDictType(customertype, TupleDictType(Map[String, TupleDictAttributeType]())) -> "Customer", 
    BagDictType(parttype, TupleDictType(Map[String, TupleDictAttributeType]())) -> "Part",
    BagDictType(nationtype, TupleDictType(Map[String, TupleDictAttributeType]())) -> "Nation",
    BagDictType(regiontype, TupleDictType(Map[String, TupleDictAttributeType]())) -> "Region")

}
