package shredding.queries.tpch

import shredding.core._
import shredding.loader.csv._

sealed trait TpchRow
case class PartSupp(ps_partkey: Long, ps_suppkey: Long, ps_availqty: Int, ps_supplycost: Double, ps_comment: String) extends TpchRow

case class Part(p_partkey: Int, p_name: String, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_retailprice: Double, p_comment: String) extends TpchRow

case class Customer(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Long, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String) extends TpchRow

case class Orders(o_orderkey: Int, o_custkey: Long, o_orderstatus: String, o_totalprice: String, o_orderdate: String, o_orderpriority: String, o_clerk: String, o_shippriority: Int, o_comment: String) extends TpchRow

case class Lineitem(l_orderkey: Long, l_partkey: Long, l_suppkey: Long, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String, l_comment: String) extends TpchRow

case class Supplier(s_suppkey: Int, s_name: String, s_address: String, s_nationkey: Long, s_phone: String, s_acctbal: Double, s_comment: String) extends TpchRow

// object TpchSchema {


//   val customertype = BagType(TupleType("c_custkey" -> IntType, "c_name" -> StringType, "c_address" -> StringType, "c_nationkey" -> IntType, "c_phone" -> StringType, "c_acctbal" -> DoubleType, "c_mktsegment" -> StringType, "c_comment" -> StringType))

//   val orderstype = BagType(TupleType("o_orderkey" -> IntType, "o_custkey" -> IntType, "o_orderstatus" -> StringType, "o_totalprice" -> StringType, "o_orderdate" -> StringType, "o_orderpriority" -> StringType, "o_clerk" -> StringType, "o_shippriority" -> IntType, "o_comment" -> StringType))

//   val lineittype = BagType(TupleType("l_orderkey" -> IntType, "l_partkey" -> IntType, "l_suppkey" -> IntType, "l_linenumber" -> IntType, "l_quantity" -> DoubleType, "l_extendedprice" -> DoubleType, "l_discount" -> DoubleType, "l_tax" -> DoubleType, "l_returnflag" -> StringType, "l_linestatus" -> StringType, "l_shipdate" -> StringType, "l_commitdate" -> StringType, "l_receiptdate" -> StringType, "l_shipinstruct" -> StringType, "l_shipmode" -> StringType, "l_comment" -> StringType))

//   val suppliertype = BagType(TupleType("s_suppkey" -> IntType, "s_name" -> StringType, "s_address" -> StringType, "s_nationkey" -> IntType, "s_phone" -> StringType, "s_acctbal" -> DoubleType, "s_comment" -> StringType))


//   val partsupptype = BagType(TupleType("ps_partkey" -> IntType, "ps_suppkey" -> IntType, "ps_availqty" -> IntType, "ps_supplycost" -> DoubleType, "ps_comment" -> StringType))


//   var tpchInputs:Map[Type, String] = Map(partsupptype.tp -> "PartSupp", 
//                                      suppliertype.tp -> "Supplier", 
//                                      lineittype.tp -> "Lineitem", 
//                                      orderstype.tp -> "Orders",
//                                      customertype.tp -> "Customer", 
//                                      parttype.tp -> "Part")

// }
