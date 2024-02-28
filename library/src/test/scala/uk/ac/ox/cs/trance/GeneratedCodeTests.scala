package uk.ac.ox.cs.trance


import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.utilities.{JoinContext, Symbol}
import Wrapper.DataFrameImplicit
import org.apache.spark.sql.expressions.scalalang._
import framework.examples.tpch._

import scala.collection.mutable.HashMap
import framework.examples.tpch.Lineitem
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import uk.ac.ox.cs.trance.utilities.SkewDataset.DatasetOps
import uk.ac.ox.cs.trance.utilities.TPCHDataframes.{spark, Customer => CustomerDF, LineItem => LineItemDF, Order => OrderDF, Part => PartDF}

import scala.collection.Seq

case class Record3041b3a67b2041dea5ac5ad96c1fa9d1(c_name: String, c_custkey: Int)
case class Recordc910c6699d6b4c41afca822c2eb073f7(c_name: String, c_custkey: Int, Customer_index: Long)
case class Recordfdb1a68833134e3ba1c2297950787fcd(o_orderdate: String, o_custkey: Int, o_orderkey: Int)
case class Recordc6464b7f916c4c55a9144964453f6429(o_orderdate: String, o_custkey: Int, o_orderkey: Int, Order_index: Long)
case class Recordfac46092219f43648daae695147b86c3(o_orderdate: Option[String], o_custkey: Option[Int], Order_index: Option[Long], c_name: String, Customer_index: Long, c_custkey: Int, o_orderkey: Option[Int])
case class Recordb2a8c6cc16f24a409ec2cd42523cc26d(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record87161bcd76a24578843c27fb0cbd5c1d(o_orderdate: Option[String], l_quantity: Option[Double], Order_index: Option[Long], c_name: String, Customer_index: Long, l_partkey: Option[Int], o_orderkey: Option[Int], l_orderkey: Option[Int])
case class Record75eddf4412b24fcc926b982c24658287(o_orderdate: Option[String], l_quantity: Option[Double], Order_index: Option[Long], c_name: String, Customer_index: Long, l_partkey: Option[Int])
case class Record5598f371ca364b98b69ef614463bed37(o_orderdate: Option[String], Order_index: Option[Long], c_name: String, Customer_index: Long)
case class Record696b2cad24ac42279dcf807979950027(l_partkey: Int, l_quantity: Double)
case class Record6e79fa25c6ea435d9a4c85b39254f274(o_orderdate: Option[String], Order_index: Option[Long], c_name: String, o_parts: Seq[Record696b2cad24ac42279dcf807979950027], Customer_index: Long)
case class Record6f4aaaacb7414214b7ebb4fea88a7436(Customer_index: Long, c_name: String, o_orderdate: Option[String], o_parts: Seq[Record696b2cad24ac42279dcf807979950027])
case class Recorda7fbee2632544b008a150aca7d3dabff(c_name: String, Customer_index: Long)
case class Record7a4ae3f4bfe748e9a7e551b36a77f38d(o_orderdate: String, o_parts: Seq[Record696b2cad24ac42279dcf807979950027])
case class Recordb81406f258e94c839d3cae86cfa7f2ff(Customer_index: Long, c_name: String, c_orders: Seq[Record7a4ae3f4bfe748e9a7e551b36a77f38d])

case class Record872cb3a876524293b3a337b102fc2bba(o_orderdate: String, o_orderkey: Int)
case class Record7566c6d3707c48ffa94da4a7244126cf(o_orderdate: String, o_orderkey: Int, Order_index: Long)
case class Record937c65a2ef754a13bfa2853a91ed26ab(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record4c531fedbf1348c8aad1e02da9b65e69(o_orderdate: String, l_quantity: Option[Double], Order_index: Long, l_partkey: Option[Int], o_orderkey: Int, l_orderkey: Option[Int])
case class Recorde9860175945245f7b3f5f01c6dbe78a6(Order_index: Long, o_orderdate: String, l_partkey: Option[Int], l_quantity: Option[Double])
case class Record89585b829f004e2280c96709ae66679f(o_orderdate: String, Order_index: Long)
case class Recorde9563047ce6a47dca308080deb04072b(l_partkey: Int, l_quantity: Double)
case class Recordff30c7814b6d4a40a39de2e7225f6ad0(Order_index: Long, o_orderdate: String, o_parts: Seq[Recorde9563047ce6a47dca308080deb04072b])

case class Recordf7ba324c84564cd4be34e9781e7fcf68(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Recordd83edc684e0940d39b048a584b69be4e(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recordf7ba324c84564cd4be34e9781e7fcf68], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record4c1c9f4a7b6548efbc431824cbf2c2b9(c_name: String, c_orders: Seq[Recordd83edc684e0940d39b048a584b69be4e])
case class Record0b94591285f94327b2c77f47c2073b98(c_name: String, c_orders: Seq[Recordd83edc684e0940d39b048a584b69be4e], Test2Full_index: Long)
case class Recordaf046ffd9cac4009aa92b4b0a24323fe(c_name: String, c_orders: Seq[Recordd83edc684e0940d39b048a584b69be4e], Test2Full_index: Long, c_orders_index: Long)
case class Recordd1d5bc6ece264775aea2145439a0a040(c_orders_index: Long, o_orderdate: Option[String], c_name: String, o_parts: Option[Seq[Recordf7ba324c84564cd4be34e9781e7fcf68]], Test2Full_index: Long)
case class Record177e89dab2c24dcaa28f91f86c2be01b(c_orders_index: Long, o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, Test2Full_index: Long, l_partkey: Option[Int])
case class Record17a1241e842d40d89068acfa5d003fa2(c_orders_index: Long, o_orderdate: Option[String], c_name: String, Test2Full_index: Long, l_partkey: Option[Int])
case class Record8312915a66844c539580a903e16a9e3f(p_name: String, p_retailprice: Double, p_partkey: Int)
case class Recordf754d1d92a9949bc9c7ded6bcd1b1769(p_name: Option[String], c_orders_index: Long, o_orderdate: Option[String], l_quantity: Double, p_retailprice: Option[Double], c_name: String, Test2Full_index: Long, l_partkey: Option[Int], p_partkey: Option[Int])
case class Recordedc158e4c91d4db4aaad5f25dfc72bcf(p_name: Option[String], c_orders_index: Long, o_orderdate: Option[String], total: Double, c_name: String, Test2Full_index: Long)
case class Recorda7c53519ba3246f385ba6a5e5a5b34bc(p_name: Option[String], c_orders_index: Long, o_orderdate: Option[String], c_name: String, Test2Full_index: Long)
case class Recordeebfd34b0d9f4ef883300d447b47160b(c_orders_index: Long, o_orderdate: Option[String], c_name: String, Test2Full_index: Long)
case class Recorda8a26120cd264ae493f8162ff5af06e9(p_name: String, total: Double)
case class Recordcdc3a0cbfc5347ee9c9d4d49113c4eb5(c_orders_index: Long, o_orderdate: Option[String], c_name: String, o_parts: Seq[Recorda8a26120cd264ae493f8162ff5af06e9], Test2Full_index: Long)
case class Recordcc499be223114b28a48c5b6c827b609a(c_name: String, Test2Full_index: Long, o_orderdate: Option[String], o_parts: Seq[Recorda8a26120cd264ae493f8162ff5af06e9])
case class Recordb22461c585ad411683a87afbc5ba6cfb(c_name: String, Test2Full_index: Long)
case class Recordf28c7812ec5e435ca6bea61bfc0dfd2a(o_orderdate: String, o_parts: Seq[Recorda8a26120cd264ae493f8162ff5af06e9])
case class Record97a6e22e71ca4a9fad9e596ca0081ab6(c_name: String, Test2Full_index: Long, c_orders: Seq[Recordf28c7812ec5e435ca6bea61bfc0dfd2a])


case class Recordf071514c62c046c4a4f183bb484fa3ec(c_name: String, o_parts: Seq[Record1401a1a294e340e6b75d6d1a9fcacdf3])
case class Recordda3d7afd6f8c47eaacb2fbcc0d70e830(c_name: String, l_quantity: Double, l_partkey: Int)
case class Recordd1050b4424f24794a99d04f1db5c17e2(c_name: String, l_partkey: Int)
case class Recordbc666a79818e4b99a54068a55af0777a(l_quantity: Double, p_retailprice: Double, c_name: String, l_partkey: Int, p_partkey: Int)
case class Record18ad9a018fe7469f93c9237c44881d27(c_name: String, total: Double)
case class Record6a46d99a2a6545c891d52849ef40e630(c_name: String)
case class Record6f474cf9f1434a18a0e1da6193e22130(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record1401a1a294e340e6b75d6d1a9fcacdf3], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recordfdb46d6882fe467297b851b46910b2bb(o_shippriority: Int, l_returnflag: Option[String], l_comment: Option[String], l_linestatus: Option[String], o_orderdate: String, l_shipmode: Option[String], o_custkey: Int, l_shipinstruct: Option[String], l_quantity: Option[Double], o_orderpriority: String, Order_index: Long, l_receiptdate: Option[String], l_linenumber: Option[Int], l_tax: Option[Double], l_shipdate: Option[String], l_extendedprice: Option[Double], o_clerk: String, o_orderstatus: String, l_partkey: Option[Int], l_discount: Option[Double], l_commitdate: Option[String], l_suppkey: Option[Int], o_totalprice: Double, o_orderkey: Int, l_orderkey: Option[Int], o_comment: String)
case class Record1401a1a294e340e6b75d6d1a9fcacdf3(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record00c257793c9a4d749aad1a51f23af8dd(c_acctbal: Double, c_name: String, Customer_index: Long, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Record6f474cf9f1434a18a0e1da6193e22130], c_mktsegment: String, c_phone: String)
case class Recorde60c0ce764b645fb87c260a64b9a87c2(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record37cc01f1b534458c88868a0964596118(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Record892ef1247c0f4b08a9961667a5ec9736(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, Order_index: Long, o_parts: Seq[Record1401a1a294e340e6b75d6d1a9fcacdf3], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record70e0c29dbe084736b94ab5d5f337509f(c_acctbal: Double, c_name: String, Customer_index: Long, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Recordfae560f231d8427dbb964481bbff76b3(o_shippriority: Option[Int], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], o_orderpriority: Option[String], c_name: String, o_parts: Option[Seq[Record1401a1a294e340e6b75d6d1a9fcacdf3]], Customer_index: Long, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String])
case class Recordc5abf315da0b4414861961b21427e639(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, Order_index: Long, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record3ed1d466dd594574a26247c0abc39540(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Record283315d0ab6b41e5b2c19bc437430181(c_acctbal: Double, c_name: String, Customer_index: Long, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Recordc41cdc2010654b008615343809c83732(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record23f268edda084396b04729dc2bddcb0b(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, Order_index: Long, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recorded3f0a3906aa4848b76586c4793b618f(o_shippriority: Option[Int], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], o_orderpriority: Option[String], Order_index: Option[Long], c_name: String, Customer_index: Long, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String])
case class Recorde080827799ee42d39ba80b8a2ca92268(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record7a8ed782201a4815a3bf89342fb7a8d0(o_shippriority: Option[Int], l_returnflag: Option[String], l_comment: Option[String], l_linestatus: Option[String], c_acctbal: Double, o_orderdate: Option[String], l_shipmode: Option[String], o_custkey: Option[Int], l_shipinstruct: Option[String], l_quantity: Option[Double], o_orderpriority: Option[String], Order_index: Option[Long], c_name: String, l_receiptdate: Option[String], l_linenumber: Option[Int], l_tax: Option[Double], Customer_index: Long, l_shipdate: Option[String], c_nationkey: Int, l_extendedprice: Option[Double], o_clerk: Option[String], o_orderstatus: Option[String], l_partkey: Option[Int], l_discount: Option[Double], l_commitdate: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, l_suppkey: Option[Int], o_totalprice: Option[Double], o_orderkey: Option[Int], l_orderkey: Option[Int], c_phone: String, o_comment: Option[String])
case class Record3631daa05e4b4052ab24fa05574e4d2c(o_shippriority: Option[Int], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], o_orderpriority: Option[String], Order_index: Option[Long], c_name: String, o_parts: Seq[Recorde080827799ee42d39ba80b8a2ca92268], Customer_index: Long, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String])
case class Recorde472cccaa8f14c7c9ba9dd7e28d5e80a(o_shippriority: Option[Int], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], o_orderpriority: Option[String], c_name: String, o_parts: Seq[Recorde080827799ee42d39ba80b8a2ca92268], Customer_index: Long, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String])
case class Record8453913032b741b5a79f742b0ba59cc1(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recorde080827799ee42d39ba80b8a2ca92268], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record2dd20d3f8e8449b3837be1ed5abc10b2(c_acctbal: Double, c_name: String, Customer_index: Long, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Record8453913032b741b5a79f742b0ba59cc1], c_mktsegment: String, c_phone: String)
case class Recordf1e5331e34d448f99399bbd6c31547b3(p_name: String, l_quantity: Double, l_partkey: Int, p_partkey: Int)
case class Recordfb4c08eaba4d4834be190e9e40707767(p_name: String, l_qty: Double)

case class Record05b49fcbda2d4d22ac480c878f32a320(o_orderdate: String, o_orderkey: Int)
case class Recordaa9f95557e254a3ba33583f7fc3c196b(o_orderdate: String, o_orderkey: Int, Order_index: Long)
case class Recordd8cc57aa66b349889ab310d46057228a(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record66ccbf5a0c90423abdb287046d46e9b8(o_orderdate: String, l_quantity: Option[Double], Order_index: Long, l_partkey: Option[Int], o_orderkey: Int, l_orderkey: Option[Int])
case class Record498b917b1b41462e94fedad6a312da2c(p_name: String, p_partkey: Int)
case class Recordc847d9b2ca0c46a295ef2c85d3ad9adc(p_name: Option[String], o_orderdate: String, l_quantity: Option[Double], Order_index: Long, l_partkey: Option[Int], p_partkey: Option[Int])
case class Recordd1d169c32e8f4974aab7b2762e9608b9(o_orderdate: String, Order_index: Long)
case class Recordf0ce7bf3af9a4a81b0c65594187a21d2(p_name: String, l_qty: Double)
case class Record0a61e3af806a483a949a90937e5bda02(o_orderdate: String, Order_index: Long, o_parts: Seq[Recordf0ce7bf3af9a4a81b0c65594187a21d2])
case class Record60529d46356c4ad4b6c0cd49b1c4b0cc(orderdate: String, o_parts: Seq[Recordf0ce7bf3af9a4a81b0c65594187a21d2])

case class Record8dd0378e7ca94b06aac0e061e650c623(o_orderdate: String, l_quantity: Double, l_partkey: Int)
case class Recordc5c246937a1748b99480bb485044f7b6(o_orderdate: String, l_partkey: Int)
case class Recordca3ca0c17eff492881a70852cae9c38a(o_orderdate: String, l_quantity: Double, p_retailprice: Double, l_partkey: Int, p_partkey: Int)
case class Record2444e40ce12849a7b7a55450a3467447(o_orderdate: String, total: Double)
case class Recordfe39850b725d4bd19e771f16fe6e390e(o_orderdate: String)

case class Record4c0a2f1a21b64d348c528cea6c3a2efa(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record4b6632dfec1144cf8d27939986447cc4(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, Order_index: Long, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record5f85b5442b0843e6916288a85866aaad(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record1d13fccc0e04425bb1e7692895a05d7f(o_shippriority: Int, l_returnflag: Option[String], l_comment: Option[String], l_linestatus: Option[String], o_orderdate: String, l_shipmode: Option[String], o_custkey: Int, l_shipinstruct: Option[String], l_quantity: Option[Double], o_orderpriority: String, Order_index: Long, l_receiptdate: Option[String], l_linenumber: Option[Int], l_tax: Option[Double], l_shipdate: Option[String], l_extendedprice: Option[Double], o_clerk: String, o_orderstatus: String, l_partkey: Option[Int], l_discount: Option[Double], l_commitdate: Option[String], l_suppkey: Option[Int], o_totalprice: Double, o_orderkey: Int, l_orderkey: Option[Int], o_comment: String)
case class Record76a6f61c1d114c68b590bab06456b418(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, Order_index: Long, o_parts: Seq[Record5f85b5442b0843e6916288a85866aaad], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)

case class Recordf6c0a3c3ff7340cb8eb95794de3a1c54(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record1a2f8332279c47e794eadcd07b9eaeb4(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recordf6c0a3c3ff7340cb8eb95794de3a1c54], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recordafd875756e7b4367a9dd5d5de38b6234(o_shippriority: Int, Test1Full_index: Long, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recordf6c0a3c3ff7340cb8eb95794de3a1c54], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recordbe052fb87c6e4d33a5e6087d467f33fa(o_shippriority: Int, Test1Full_index: Long, o_orderdate: String, o_custkey: Int, l_quantity: Option[Double], o_orderpriority: String, o_clerk: String, o_orderstatus: String, l_partkey: Option[Int], o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record208322e606424f4a8611bf152c55c58a(o_shippriority: Int, Test1Full_index: Long, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, l_partkey: Option[Int], o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recorde5298436562d443d8dcaf47986ecb869(p_retailprice: Double, p_partkey: Int)
case class Record1e7426cde2fb4db4bd1651c5042566ff(o_shippriority: Int, Test1Full_index: Long, o_orderdate: String, o_custkey: Int, l_quantity: Double, o_orderpriority: String, p_retailprice: Option[Double], o_clerk: String, o_orderstatus: String, l_partkey: Option[Int], p_partkey: Option[Int], o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record73c2cc0b4c35414eb667d009ae3da8fe(o_shippriority: Int, Test1Full_index: Long, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, subtotal: Double, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recordc25d1a7bd0144fd7bf5e43dd49ea10e8(o_shippriority: Int, Test1Full_index: Long, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record91be4c8e506d40439058f94358237cc2(subtotal: Double)
case class Record26bdbe1096564c8f8349c356d2fc6b16(o_shippriority: Int, Test1Full_index: Long, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record91be4c8e506d40439058f94358237cc2], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record94a0c813d85b45b2b3e78db042571eae(o_orderdate: String, subtotal: Double)
case class Recordf90630f4fe07421282e845caa0d47b03(o_orderdate: String)
case class Recordc3296abfbfd94296b30e3d1d7a69ce92(o_orderdate: String, total: Double)
class GeneratedCodeTests extends AnyFunSpec with BeforeAndAfterEach with Serializable {


  def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.collect() sameElements result.collect())
  }

  def assertDataFrameSchemaEqual(df1: DataFrame, df2: DataFrame): Unit = {
    val schema1 = df1.schema.fields.map(f => f.copy(nullable = false))
    val schema2 = df2.schema.fields.map(f => f.copy(nullable = false))

    schema1 shouldEqual schema2
  }

  def assertDataFramesAreEquivalent(df1: DataFrame, df2: DataFrame): Unit = {
    implicit val anyOrdering: Ordering[Any] = Ordering.fromLessThan {
      case (a, b) => a.toString < b.toString
    }

    val count1 = df1.collect().map(_.toSeq.sorted).groupBy(identity).mapValues(_.length)
    val count2 = df2.collect().map(_.toSeq.sorted).groupBy(identity).mapValues(_.length)

    count1 shouldEqual count2
  }

  val Test1Full = {

    import spark.implicits._
    val x13 = OrderDF


    val x14 = x13.withColumn("Order_index", monotonically_increasing_id())
      .as[Record4b6632dfec1144cf8d27939986447cc4]

    val x16: Dataset[Lineitem] = LineItemDF.as[Lineitem]


    val x19 = x14.equiJoin(x16,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record1d13fccc0e04425bb1e7692895a05d7f]

    val x21 = x19


    val x23 = x21.groupByKey(x22 => Record4b6632dfec1144cf8d27939986447cc4(x22.o_shippriority, x22.o_orderdate, x22.o_custkey, x22.o_orderpriority, x22.Order_index, x22.o_clerk, x22.o_orderstatus, x22.o_totalprice, x22.o_orderkey, x22.o_comment)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x22 =>
          (x22.l_tax, x22.l_receiptdate, x22.l_shipdate, x22.l_shipinstruct, x22.l_shipmode, x22.l_discount, x22.l_partkey, x22.l_returnflag, x22.l_orderkey, x22.l_comment, x22.l_linenumber, x22.l_quantity, x22.l_suppkey, x22.l_commitdate, x22.l_extendedprice, x22.l_linestatus) match {
            case (None, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, None, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, None, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, None, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, None, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, None, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, None, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, None, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, None, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, None, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, None, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, None, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, _, None, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, _, _, None, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, _, _, _, None, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, None) => Seq()
            case _ => Seq(Record5f85b5442b0843e6916288a85866aaad(x22.l_returnflag match { case Some(x) => x; case _ => "null" }, x22.l_comment match { case Some(x) => x; case _ => "null" }, x22.l_linestatus match { case Some(x) => x; case _ => "null" }, x22.l_shipmode match { case Some(x) => x; case _ => "null" }, x22.l_shipinstruct match { case Some(x) => x; case _ => "null" }, x22.l_quantity match { case Some(x) => x; case _ => 0.0 }, x22.l_receiptdate match { case Some(x) => x; case _ => "null" }, x22.l_linenumber match { case Some(x) => x; case _ => 0 }, x22.l_tax match { case Some(x) => x; case _ => 0.0 }, x22.l_shipdate match { case Some(x) => x; case _ => "null" }, x22.l_extendedprice match { case Some(x) => x; case _ => 0.0 }, x22.l_partkey match { case Some(x) => x; case _ => 0 }, x22.l_discount match { case Some(x) => x; case _ => 0.0 }, x22.l_commitdate match { case Some(x) => x; case _ => "null" }, x22.l_suppkey match { case Some(x) => x; case _ => 0 }, x22.l_orderkey match { case Some(x) => x; case _ => 0 }))
          }).toSeq
        Record76a6f61c1d114c68b590bab06456b418(key.o_shippriority, key.o_orderdate, key.o_custkey, key.o_orderpriority, key.Order_index, grp, key.o_clerk, key.o_orderstatus, key.o_totalprice, key.o_orderkey, key.o_comment)
    }.as[Record76a6f61c1d114c68b590bab06456b418]

    val x24 = x23
    val Test1Full = x24
    Test1Full
  }

  val Test2Full = {
    import spark.implicits._
    val x20 = CustomerDF


    val x21 = x20.withColumn("Customer_index", monotonically_increasing_id())
      .as[Record283315d0ab6b41e5b2c19bc437430181]

    val x23 = OrderDF


    val x24 = x23.withColumn("Order_index", monotonically_increasing_id())
      .as[Record23f268edda084396b04729dc2bddcb0b]

    val x27 = x21.equiJoin(x24,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Recorded3f0a3906aa4848b76586c4793b618f]

    val x29 = LineItemDF.as[Lineitem]


    val x32 = x27.equiJoin(x29,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record7a8ed782201a4815a3bf89342fb7a8d0]

    val x34 = x32


    val x36 = x34.groupByKey(x35 => Recorded3f0a3906aa4848b76586c4793b618f(x35.o_shippriority, x35.c_acctbal, x35.o_orderdate, x35.o_custkey, x35.o_orderpriority, x35.Order_index, x35.c_name, x35.Customer_index, x35.c_nationkey, x35.o_clerk, x35.o_orderstatus, x35.c_custkey, x35.c_comment, x35.c_address, x35.c_mktsegment, x35.o_totalprice, x35.o_orderkey, x35.c_phone, x35.o_comment)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x35 =>
          (x35.l_partkey, x35.l_discount, x35.l_quantity, x35.l_tax, x35.l_commitdate, x35.l_shipinstruct, x35.l_extendedprice, x35.l_shipmode, x35.l_comment, x35.l_suppkey, x35.l_orderkey, x35.l_linenumber, x35.l_linestatus, x35.l_shipdate, x35.l_receiptdate, x35.l_returnflag) match {
            case (None, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, None, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, None, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, None, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, None, _, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, None, _, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, None, _, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, None, _, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, None, _, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, None, _, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, None, _, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, None, _, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, _, None, _, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, _, _, None, _, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, _, _, _, None, _) => Seq()
            case (_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, None) => Seq()
            case _ => Seq(Recorde080827799ee42d39ba80b8a2ca92268(x35.l_returnflag match { case Some(x) => x; case _ => "null" }, x35.l_comment match { case Some(x) => x; case _ => "null" }, x35.l_linestatus match { case Some(x) => x; case _ => "null" }, x35.l_shipmode match { case Some(x) => x; case _ => "null" }, x35.l_shipinstruct match { case Some(x) => x; case _ => "null" }, x35.l_quantity match { case Some(x) => x; case _ => 0.0 }, x35.l_receiptdate match { case Some(x) => x; case _ => "null" }, x35.l_linenumber match { case Some(x) => x; case _ => 0 }, x35.l_tax match { case Some(x) => x; case _ => 0.0 }, x35.l_shipdate match { case Some(x) => x; case _ => "null" }, x35.l_extendedprice match { case Some(x) => x; case _ => 0.0 }, x35.l_partkey match { case Some(x) => x; case _ => 0 }, x35.l_discount match { case Some(x) => x; case _ => 0.0 }, x35.l_commitdate match { case Some(x) => x; case _ => "null" }, x35.l_suppkey match { case Some(x) => x; case _ => 0 }, x35.l_orderkey match { case Some(x) => x; case _ => 0 }))
          }).toSeq
        Record3631daa05e4b4052ab24fa05574e4d2c(key.o_shippriority, key.c_acctbal, key.o_orderdate, key.o_custkey, key.o_orderpriority, key.Order_index, key.c_name, grp, key.Customer_index, key.c_nationkey, key.o_clerk, key.o_orderstatus, key.c_custkey, key.c_comment, key.c_address, key.c_mktsegment, key.o_totalprice, key.o_orderkey, key.c_phone, key.o_comment)
    }.as[Record3631daa05e4b4052ab24fa05574e4d2c]

    val x38 = x36.select("o_shippriority", "c_acctbal", "o_orderdate", "o_custkey", "o_orderpriority", "c_name", "o_parts", "Customer_index", "c_nationkey", "o_clerk", "o_orderstatus", "c_custkey", "c_comment", "c_address", "c_mktsegment", "o_totalprice", "o_orderkey", "c_phone", "o_comment")

      .as[Recorde472cccaa8f14c7c9ba9dd7e28d5e80a]

    val x40 = x38.groupByKey(x39 => Record283315d0ab6b41e5b2c19bc437430181(x39.c_acctbal, x39.c_name, x39.Customer_index, x39.c_nationkey, x39.c_custkey, x39.c_comment, x39.c_address, x39.c_mktsegment, x39.c_phone)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x39 =>
          (x39.o_totalprice, x39.o_orderdate, x39.o_orderstatus, x39.o_shippriority, x39.o_custkey, x39.o_orderkey, x39.o_comment, x39.o_orderpriority, x39.o_clerk) match {
            case (None, _, _, _, _, _, _, _, _) => Seq()
            case (_, None, _, _, _, _, _, _, _) => Seq()
            case (_, _, None, _, _, _, _, _, _) => Seq()
            case (_, _, _, None, _, _, _, _, _) => Seq()
            case (_, _, _, _, None, _, _, _, _) => Seq()
            case (_, _, _, _, _, None, _, _, _) => Seq()
            case (_, _, _, _, _, _, None, _, _) => Seq()
            case (_, _, _, _, _, _, _, None, _) => Seq()
            case (_, _, _, _, _, _, _, _, None) => Seq()
            case _ => Seq(Record8453913032b741b5a79f742b0ba59cc1(x39.o_shippriority match { case Some(x) => x; case _ => 0 }, x39.o_orderdate match { case Some(x) => x; case _ => "null" }, x39.o_custkey match { case Some(x) => x; case _ => 0 }, x39.o_orderpriority match { case Some(x) => x; case _ => "null" }, x39.o_parts, x39.o_clerk match { case Some(x) => x; case _ => "null" }, x39.o_orderstatus match { case Some(x) => x; case _ => "null" }, x39.o_totalprice match { case Some(x) => x; case _ => 0.0 }, x39.o_orderkey match { case Some(x) => x; case _ => 0 }, x39.o_comment match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Record2dd20d3f8e8449b3837be1ed5abc10b2(key.c_acctbal, key.c_name, key.Customer_index, key.c_nationkey, key.c_custkey, key.c_comment, key.c_address, grp, key.c_mktsegment, key.c_phone)
    }.as[Record2dd20d3f8e8449b3837be1ed5abc10b2]

    val x41 = x40
    x41
  }


  override protected def afterEach(): Unit = {
    Symbol.freshClear()
    JoinContext.freshClear()
    super.afterEach()
  }

  describe("FlatToNested") {
    it("Test1") {
      import spark.implicits._
      val x13 = OrderDF.select("o_orderdate", "o_orderkey")

        .as[Record872cb3a876524293b3a337b102fc2bba]

      val x14 = x13.withColumn("Order_index", monotonically_increasing_id())
        .as[Record7566c6d3707c48ffa94da4a7244126cf]

      val x16 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

        .as[Record937c65a2ef754a13bfa2853a91ed26ab]

      val x19 = x14.equiJoin(x16,
        Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record4c531fedbf1348c8aad1e02da9b65e69]

      val x21 = x19.select("Order_index", "o_orderdate", "l_partkey", "l_quantity")

        .as[Recorde9860175945245f7b3f5f01c6dbe78a6]

      val x23 = x21.groupByKey(x22 => Record89585b829f004e2280c96709ae66679f(x22.o_orderdate, x22.Order_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x22 =>
            (x22.l_partkey, x22.l_quantity) match {
              case (None, _) => Seq()
              case (_, None) => Seq()
              case _ => Seq(Recorde9563047ce6a47dca308080deb04072b(x22.l_partkey match { case Some(x) => x; case _ => 0 }, x22.l_quantity match { case Some(x) => x; case _ => 0.0 }))
            }).toSeq
          Recordff30c7814b6d4a40a39de2e7225f6ad0(key.Order_index, key.o_orderdate, grp)
      }.as[Recordff30c7814b6d4a40a39de2e7225f6ad0]

      val x24 = x23
      val Test1 = x24

      Test1.show(false)
      Test1.printSchema()
    }

    it("Test2") {
      import spark.implicits._

      val x20 = CustomerDF.select("c_name", "c_custkey")

        .as[Record3041b3a67b2041dea5ac5ad96c1fa9d1]

      val x21 = x20.withColumn("Customer_index", monotonically_increasing_id())
        .as[Recordc910c6699d6b4c41afca822c2eb073f7]

      val x23 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

        .as[Recordfdb1a68833134e3ba1c2297950787fcd]

      val x24 = x23.withColumn("Order_index", monotonically_increasing_id())
        .as[Recordc6464b7f916c4c55a9144964453f6429]

      val x27 = x21.equiJoin(x24,
        Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Recordfac46092219f43648daae695147b86c3]

      val x29 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

        .as[Recordb2a8c6cc16f24a409ec2cd42523cc26d]

      val x32 = x27.equiJoin(x29,
        Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record87161bcd76a24578843c27fb0cbd5c1d]

      val x34 = x32.select("o_orderdate", "l_quantity", "Order_index", "c_name", "Customer_index", "l_partkey")

        .as[Record75eddf4412b24fcc926b982c24658287]

      val x36 = x34.groupByKey(x35 => Record5598f371ca364b98b69ef614463bed37(x35.o_orderdate, x35.Order_index, x35.c_name, x35.Customer_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x35 =>
            (x35.l_quantity, x35.l_partkey) match {
              case (None, _) => Seq()
              case (_, None) => Seq()
              case _ => Seq(Record696b2cad24ac42279dcf807979950027(x35.l_partkey match { case Some(x) => x; case _ => 0 }, x35.l_quantity match { case Some(x) => x; case _ => 0.0 }))
            }).toSeq
          Record6e79fa25c6ea435d9a4c85b39254f274(key.o_orderdate, key.Order_index, key.c_name, grp, key.Customer_index)
      }.as[Record6e79fa25c6ea435d9a4c85b39254f274]

      val x38 = x36.select("Customer_index", "c_name", "o_orderdate", "o_parts")

        .as[Record6f4aaaacb7414214b7ebb4fea88a7436]

      val x40 = x38.groupByKey(x39 => Recorda7fbee2632544b008a150aca7d3dabff(x39.c_name, x39.Customer_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x39 =>
            (x39.o_orderdate) match {
              case (None) => Seq()
              case _ => Seq(Record7a4ae3f4bfe748e9a7e551b36a77f38d(x39.o_orderdate match { case Some(x) => x; case _ => "null" }, x39.o_parts))
            }).toSeq
          Recordb81406f258e94c839d3cae86cfa7f2ff(key.Customer_index, key.c_name, grp)
      }.as[Recordb81406f258e94c839d3cae86cfa7f2ff]

      val x41 = x40

      x41.show(false)
      x41.printSchema()
    }


    it("Test2Flat") {
      import spark.implicits._
      val x24 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

        .as[Recordb80e3302266a42e4a981836379570249]

      val x25 = x24.withColumn("Order_index", monotonically_increasing_id())
        .as[Recordd16d0fb04d69452d83bec9ee88449806]

      val x27 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

        .as[Record80e931fa827f40cca9ea94aa8b0a308a]

      val x30 = x25.equiJoin(x27,
        Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Recordbf846df439974b07b6abb5120e933eb8]

      val x32 = x30.select("o_orderdate", "o_custkey", "l_quantity", "Order_index", "l_partkey")

        .as[Recordd74a63caf68e4419868a969376dbf27b]

      val x34 = x32.groupByKey(x33 => Recorde64677bfead1481e9fe09c808070d452(x33.o_orderdate, x33.o_custkey, x33.Order_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x33 =>
            (x33.l_quantity, x33.l_partkey) match {
              case (None, _) => Seq()
              case (_, None) => Seq()
              case _ => Seq(Record0f24c31b203c447584d817d42bc7dd63(x33.l_partkey match { case Some(x) => x; case _ => 0 }, x33.l_quantity match { case Some(x) => x; case _ => 0.0 }))
            }).toSeq
          Record07ff0f6ff4644ebeadc4697bd56a2d39(key.o_orderdate, key.o_custkey, key.Order_index, grp)
      }.as[Record07ff0f6ff4644ebeadc4697bd56a2d39]

      val x35 = x34
      val orders = x35
      //orders.cache
      //orders.count
      val x37 = CustomerDF.select("c_name", "c_custkey")

        .as[Recordea3dc6f9b6ed4825a085a971a8423122]

      val x38 = x37.withColumn("Customer_index", monotonically_increasing_id())
        .as[Record9f1664be1da24f109f1aea1ab3e17c27]

      val x40 = orders


      val x43 = x38.equiJoin(x40,
        Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record53d1eaf1461b4a32adb18c8ef293ff5e]

      val x45 = x43.select("Customer_index", "c_name", "o_orderdate", "o_parts")

        .as[Recordfbdaef1028e84a7f828a554c285e6e01]

      val x47 = x45.groupByKey(x46 => Record16e3a795c8c148c0a5a11f439e060620(x46.c_name, x46.Customer_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x46 =>
            (x46.o_orderdate, x46.o_parts) match {
              case (None, _) => Seq()
              case (_, None) => Seq()
              case _ => Seq(Record93f64805c41d4615a2e3ea7d2dc10155(x46.o_orderdate match { case Some(x) => x; case _ => "null" }, x46.o_parts match { case Some(x) => x; case _ => null }))
            }).toSeq
          Record1b47e1a717b84648887c65f20c823179(key.Customer_index, key.c_name, grp)
      }.as[Record1b47e1a717b84648887c65f20c823179]

      val x48 = x47
      val Test2 = x48

      Test2.show(false)
      Test2.printSchema()
    }


    it("Test0Join") {
      import spark.implicits._

      val x0 = LineItemDF.as[Lineitem]


      val x11 = x0.equiJoin(PartDF.as[Part],
        Seq("l_partkey"), Seq("p_partkey"), "inner")

      val x13 = x11.select("p_name", "l_quantity")
        .withColumnRenamed("l_quantity", "l_qty")
        .withColumn("l_qty", when(col("l_qty").isNull, 0.0).otherwise(col("l_qty")))
        .as[Recordfb4c08eaba4d4834be190e9e40707767]

      x13.show(false)
      x13.printSchema()
    }

    it("Test1Join") {
      import spark.implicits._

      val x17 = OrderDF.select("o_orderdate", "o_orderkey")

        .as[Record05b49fcbda2d4d22ac480c878f32a320]

      val x18 = x17.withColumn("Order_index", monotonically_increasing_id())
        .as[Recordaa9f95557e254a3ba33583f7fc3c196b]

      val x20 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

        .as[Recordd8cc57aa66b349889ab310d46057228a]

      val x23 = x18.equiJoin(x20,
        Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record66ccbf5a0c90423abdb287046d46e9b8]

      val x25 = PartDF.select("p_name", "p_partkey")

        .as[Record498b917b1b41462e94fedad6a312da2c]

      val x28 = x23.equiJoin(x25,
        Seq("l_partkey"), Seq("p_partkey"), "left_outer").as[Recordc847d9b2ca0c46a295ef2c85d3ad9adc]

      val x30 = x28.groupByKey(x29 => Recordd1d169c32e8f4974aab7b2762e9608b9(x29.o_orderdate, x29.Order_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x29 =>
            (x29.p_name, x29.l_quantity) match {
              case (None, _) => Seq()
              case (_, None) => Seq()
              case _ => Seq(Recordf0ce7bf3af9a4a81b0c65594187a21d2(x29.p_name match { case Some(x) => x; case _ => "null" }, x29.l_quantity match { case Some(x) => x; case _ => 0.0 }))
            }).toSeq
          Record0a61e3af806a483a949a90937e5bda02(key.o_orderdate, key.Order_index, grp)
      }.as[Record0a61e3af806a483a949a90937e5bda02]

      val x32 = x30.select("o_orderdate", "o_parts")
        .withColumnRenamed("o_orderdate", "orderdate")
        .as[Record60529d46356c4ad4b6c0cd49b1c4b0cc]


      x32.show(false)
      x32.printSchema()
    }

    it("Test1Full") {
      import spark.implicits._

      val x13 = OrderDF


      val x14 = x13.withColumn("Order_index", monotonically_increasing_id())
        .as[Record4b6632dfec1144cf8d27939986447cc4]

      val x16: Dataset[Lineitem] = LineItemDF.as[Lineitem]


      val x19 = x14.equiJoin(x16,
        Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record1d13fccc0e04425bb1e7692895a05d7f]

      val x21 = x19


      val x23 = x21.groupByKey(x22 => Record4b6632dfec1144cf8d27939986447cc4(x22.o_shippriority, x22.o_orderdate, x22.o_custkey, x22.o_orderpriority, x22.Order_index, x22.o_clerk, x22.o_orderstatus, x22.o_totalprice, x22.o_orderkey, x22.o_comment)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x22 =>
            (x22.l_tax, x22.l_receiptdate, x22.l_shipdate, x22.l_shipinstruct, x22.l_shipmode, x22.l_discount, x22.l_partkey, x22.l_returnflag, x22.l_orderkey, x22.l_comment, x22.l_linenumber, x22.l_quantity, x22.l_suppkey, x22.l_commitdate, x22.l_extendedprice, x22.l_linestatus) match {
              case (None, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, None, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, None, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, None, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, None, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, None, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, None, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, None, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, None, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, None, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, None, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, None, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, _, None, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, _, _, None, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, _, _, _, None, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, None) => Seq()
              case _ => Seq(Record5f85b5442b0843e6916288a85866aaad(x22.l_returnflag match { case Some(x) => x; case _ => "null" }, x22.l_comment match { case Some(x) => x; case _ => "null" }, x22.l_linestatus match { case Some(x) => x; case _ => "null" }, x22.l_shipmode match { case Some(x) => x; case _ => "null" }, x22.l_shipinstruct match { case Some(x) => x; case _ => "null" }, x22.l_quantity match { case Some(x) => x; case _ => 0.0 }, x22.l_receiptdate match { case Some(x) => x; case _ => "null" }, x22.l_linenumber match { case Some(x) => x; case _ => 0 }, x22.l_tax match { case Some(x) => x; case _ => 0.0 }, x22.l_shipdate match { case Some(x) => x; case _ => "null" }, x22.l_extendedprice match { case Some(x) => x; case _ => 0.0 }, x22.l_partkey match { case Some(x) => x; case _ => 0 }, x22.l_discount match { case Some(x) => x; case _ => 0.0 }, x22.l_commitdate match { case Some(x) => x; case _ => "null" }, x22.l_suppkey match { case Some(x) => x; case _ => 0 }, x22.l_orderkey match { case Some(x) => x; case _ => 0 }))
            }).toSeq
          Record76a6f61c1d114c68b590bab06456b418(key.o_shippriority, key.o_orderdate, key.o_custkey, key.o_orderpriority, key.Order_index, grp, key.o_clerk, key.o_orderstatus, key.o_totalprice, key.o_orderkey, key.o_comment)
      }.as[Record76a6f61c1d114c68b590bab06456b418]

      val x24 = x23
      val Test1Full = x24

      x24.show(false)
      x24.printSchema()
    }

    it("Test2Full") {
      Test2Full.show(false)
      Test2Full.printSchema()
    }

    it("Test1Agg1") {
      import spark.implicits._

      val x18 = Test1Full.flatMap { case x14 =>
        x14.o_parts.foldLeft(HashMap.empty[Recordc5c246937a1748b99480bb485044f7b6, Double].withDefaultValue(0.0))(
          (acc, x15) => {
            acc(Recordc5c246937a1748b99480bb485044f7b6(x14.o_orderdate, x15.l_partkey)) += x15.l_quantity.asInstanceOf[Double];
            acc
          }
        ).map(x15 => Record8dd0378e7ca94b06aac0e061e650c623(x15._1.o_orderdate, x15._2, x15._1.l_partkey))

      }.as[Record8dd0378e7ca94b06aac0e061e650c623]

      val x21 = x18.equiJoin(PartDF.as[Part],
        Seq("l_partkey"), Seq("p_partkey"), "inner").as[Recordca3ca0c17eff492881a70852cae9c38a]

      val x23 = x21.select("o_orderdate", "l_quantity", "p_retailprice")
        .withColumn("total", (col("l_quantity") * col("p_retailprice")))
        .withColumn("total", when(col("total").isNull, 0.0).otherwise(col("total")))
        .as[Record2444e40ce12849a7b7a55450a3467447]

      val x25 = x23.groupByKey(x24 => Recordfe39850b725d4bd19e771f16fe6e390e(x24.o_orderdate))
        .agg(typed.sum[Record2444e40ce12849a7b7a55450a3467447](x24 => x24.total)
        ).mapPartitions { it =>
        it.map { case (key, total) =>
          Record2444e40ce12849a7b7a55450a3467447(key.o_orderdate, total)
        }
      }.as[Record2444e40ce12849a7b7a55450a3467447]

      val x26 = x25
      val Test1Agg1 = x26

      Test1Agg1.show(false)
      Test1Agg1.printSchema()
    }

    it("Test1AggS") {
      import spark.implicits._

      val x28 = Test1Full


      val x29 = x28.withColumn("Test1Full_index", monotonically_increasing_id())
        .as[Recordafd875756e7b4367a9dd5d5de38b6234]

      val x34 = x29.flatMap {
        case x30 =>
          if (x30.o_parts.isEmpty) Seq(Recordbe052fb87c6e4d33a5e6087d467f33fa(x30.o_shippriority, x30.Test1Full_index, x30.o_orderdate, x30.o_custkey, None, x30.o_orderpriority, x30.o_clerk, x30.o_orderstatus, None, x30.o_totalprice, x30.o_orderkey, x30.o_comment))
          else x30.o_parts.foldLeft(HashMap.empty[Record208322e606424f4a8611bf152c55c58a, Double].withDefaultValue(0.0))(
            (acc, x31) => {
              acc(Record208322e606424f4a8611bf152c55c58a(x30.o_shippriority, x30.Test1Full_index, x30.o_orderdate, x30.o_custkey, x30.o_orderpriority, x30.o_clerk, x30.o_orderstatus, Some(x31.l_partkey), x30.o_totalprice, x30.o_orderkey, x30.o_comment)) += x31.l_quantity.asInstanceOf[Double];
              acc
            }
          ).map(x31 => Recordbe052fb87c6e4d33a5e6087d467f33fa(x31._1.o_shippriority, x31._1.Test1Full_index, x31._1.o_orderdate, x31._1.o_custkey, Some(x31._2), x31._1.o_orderpriority, x31._1.o_clerk, x31._1.o_orderstatus, x31._1.l_partkey, x31._1.o_totalprice, x31._1.o_orderkey, x31._1.o_comment))


      }.as[Recordbe052fb87c6e4d33a5e6087d467f33fa]

      val x36 = PartDF.select("p_retailprice", "p_partkey")

        .as[Recorde5298436562d443d8dcaf47986ecb869]

      val x39 = x34.equiJoin(x36,
        Seq("l_partkey"), Seq("p_partkey"), "left_outer").as[Record1e7426cde2fb4db4bd1651c5042566ff]

      val x41 = x39.select("o_shippriority", "Test1Full_index", "o_orderdate", "o_custkey", "l_quantity", "o_orderpriority", "p_retailprice", "o_clerk", "o_orderstatus", "o_totalprice", "o_orderkey", "o_comment")
        .withColumn("subtotal", (col("l_quantity") * col("p_retailprice")))
        .withColumn("subtotal", when(col("subtotal").isNull, 0.0).otherwise(col("subtotal")))
        .as[Record73c2cc0b4c35414eb667d009ae3da8fe]

      val x43 = x41.groupByKey(x42 => Recordc25d1a7bd0144fd7bf5e43dd49ea10e8(x42.o_shippriority, x42.Test1Full_index, x42.o_orderdate, x42.o_custkey, x42.o_orderpriority, x42.o_clerk, x42.o_orderstatus, x42.o_totalprice, x42.o_orderkey, x42.o_comment))
        .agg(typed.sum[Record73c2cc0b4c35414eb667d009ae3da8fe](x42 => x42.subtotal)
        ).mapPartitions { it =>
        it.map { case (key, subtotal) =>
          Record73c2cc0b4c35414eb667d009ae3da8fe(key.o_shippriority, key.Test1Full_index, key.o_orderdate, key.o_custkey, key.o_orderpriority, key.o_clerk, key.o_orderstatus, subtotal, key.o_totalprice, key.o_orderkey, key.o_comment)
        }
      }.as[Record73c2cc0b4c35414eb667d009ae3da8fe]

      val x45 = x43


      val x47 = x45.groupByKey(x46 => Recordc25d1a7bd0144fd7bf5e43dd49ea10e8(x46.o_shippriority, x46.Test1Full_index, x46.o_orderdate, x46.o_custkey, x46.o_orderpriority, x46.o_clerk, x46.o_orderstatus, x46.o_totalprice, x46.o_orderkey, x46.o_comment)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x46 =>
            () match {

              case _ => Seq(Record91be4c8e506d40439058f94358237cc2(x46.subtotal))
            }).toSeq
          Record26bdbe1096564c8f8349c356d2fc6b16(key.o_shippriority, key.Test1Full_index, key.o_orderdate, key.o_custkey, key.o_orderpriority, grp, key.o_clerk, key.o_orderstatus, key.o_totalprice, key.o_orderkey, key.o_comment)
      }.as[Record26bdbe1096564c8f8349c356d2fc6b16]

      val x48 = x47
      val step1 = x48
      //step1.cache
      //step1.count
      val x53 = step1.flatMap { case x49 =>
        x49.o_parts.foldLeft(HashMap.empty[Recordf90630f4fe07421282e845caa0d47b03, Double].withDefaultValue(0.0))(
          (acc, x50) => {
            acc(Recordf90630f4fe07421282e845caa0d47b03(x49.o_orderdate)) += x50.subtotal.asInstanceOf[Double];
            acc
          }
        ).map(x50 => Record94a0c813d85b45b2b3e78db042571eae(x50._1.o_orderdate, x50._2))

      }.as[Record94a0c813d85b45b2b3e78db042571eae]

      val x55 = x53
        .withColumnRenamed("subtotal", "total")
        .withColumn("total", when(col("total").isNull, 0.0).otherwise(col("total")))
        .as[Recordc3296abfbfd94296b30e3d1d7a69ce92]

      val x57 = x55.groupByKey(x56 => Recordf90630f4fe07421282e845caa0d47b03(x56.o_orderdate))
        .agg(typed.sum[Recordc3296abfbfd94296b30e3d1d7a69ce92](x56 => x56.total)
        ).mapPartitions { it =>
        it.map { case (key, total) =>
          Recordc3296abfbfd94296b30e3d1d7a69ce92(key.o_orderdate, total)
        }
      }.as[Recordc3296abfbfd94296b30e3d1d7a69ce92]

      val x58 = x57
      x58.show(false)
      x58.printSchema()
    }

    it("Test2Agg2") {
      import spark.implicits._


      val x363 = OrderDF


      val x364 = x363.withColumn("Order_index", monotonically_increasing_id())
        .as[Recordc5abf315da0b4414861961b21427e639]

      val x366 = LineItemDF.as[Lineitem]


      val x369 = x364.equiJoin(x366,
        Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Recordfdb46d6882fe467297b851b46910b2bb]

      val x371 = x369


      val x373 = x371.groupByKey(x372 => Recordc5abf315da0b4414861961b21427e639(x372.o_shippriority, x372.o_orderdate, x372.o_custkey, x372.o_orderpriority, x372.Order_index, x372.o_clerk, x372.o_orderstatus, x372.o_totalprice, x372.o_orderkey, x372.o_comment)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x372 =>
            (x372.l_quantity, x372.l_shipinstruct, x372.l_extendedprice, x372.l_returnflag, x372.l_commitdate, x372.l_comment, x372.l_linestatus, x372.l_orderkey, x372.l_discount, x372.l_shipmode, x372.l_linenumber, x372.l_receiptdate, x372.l_tax, x372.l_partkey, x372.l_suppkey, x372.l_shipdate) match {
              case (None, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, None, _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, None, _, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, None, _, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, None, _, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, None, _, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, None, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, None, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, None, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, None, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, None, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, None, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, _, None, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, _, _, None, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, _, _, _, None, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, None) => Seq()
              case _ => Seq(Record1401a1a294e340e6b75d6d1a9fcacdf3(x372.l_returnflag match { case Some(x) => x; case _ => "null" }, x372.l_comment match { case Some(x) => x; case _ => "null" }, x372.l_linestatus match { case Some(x) => x; case _ => "null" }, x372.l_shipmode match { case Some(x) => x; case _ => "null" }, x372.l_shipinstruct match { case Some(x) => x; case _ => "null" }, x372.l_quantity match { case Some(x) => x; case _ => 0.0 }, x372.l_receiptdate match { case Some(x) => x; case _ => "null" }, x372.l_linenumber match { case Some(x) => x; case _ => 0 }, x372.l_tax match { case Some(x) => x; case _ => 0.0 }, x372.l_shipdate match { case Some(x) => x; case _ => "null" }, x372.l_extendedprice match { case Some(x) => x; case _ => 0.0 }, x372.l_partkey match { case Some(x) => x; case _ => 0 }, x372.l_discount match { case Some(x) => x; case _ => 0.0 }, x372.l_commitdate match { case Some(x) => x; case _ => "null" }, x372.l_suppkey match { case Some(x) => x; case _ => 0 }, x372.l_orderkey match { case Some(x) => x; case _ => 0 }))
            }).toSeq
          Record892ef1247c0f4b08a9961667a5ec9736(key.o_shippriority, key.o_orderdate, key.o_custkey, key.o_orderpriority, key.Order_index, grp, key.o_clerk, key.o_orderstatus, key.o_totalprice, key.o_orderkey, key.o_comment)
      }.as[Record892ef1247c0f4b08a9961667a5ec9736]

      val x374 = x373
      val orders = x374

      val x376 = CustomerDF


      val x377 = x376.withColumn("Customer_index", monotonically_increasing_id())
        .as[Record70e0c29dbe084736b94ab5d5f337509f]

      val x379 = orders


      val x382 = x377.equiJoin(x379,
        Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Recordfae560f231d8427dbb964481bbff76b3]

      val x384 = x382


      val x386 = x384.groupByKey(x385 => Record70e0c29dbe084736b94ab5d5f337509f(x385.c_acctbal, x385.c_name, x385.Customer_index, x385.c_nationkey, x385.c_custkey, x385.c_comment, x385.c_address, x385.c_mktsegment, x385.c_phone)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x385 =>
            (x385.o_parts, x385.o_comment, x385.o_custkey, x385.o_shippriority, x385.o_orderpriority, x385.o_orderdate, x385.o_orderstatus, x385.o_orderkey, x385.o_clerk, x385.o_totalprice) match {
              case (None, _, _, _, _, _, _, _, _, _) => Seq()
              case (_, None, _, _, _, _, _, _, _, _) => Seq()
              case (_, _, None, _, _, _, _, _, _, _) => Seq()
              case (_, _, _, None, _, _, _, _, _, _) => Seq()
              case (_, _, _, _, None, _, _, _, _, _) => Seq()
              case (_, _, _, _, _, None, _, _, _, _) => Seq()
              case (_, _, _, _, _, _, None, _, _, _) => Seq()
              case (_, _, _, _, _, _, _, None, _, _) => Seq()
              case (_, _, _, _, _, _, _, _, None, _) => Seq()
              case (_, _, _, _, _, _, _, _, _, None) => Seq()
              case _ => Seq(Record6f474cf9f1434a18a0e1da6193e22130(x385.o_shippriority match { case Some(x) => x; case _ => 0 }, x385.o_orderdate match { case Some(x) => x; case _ => "null" }, x385.o_custkey match { case Some(x) => x; case _ => 0 }, x385.o_orderpriority match { case Some(x) => x; case _ => "null" }, x385.o_parts match { case Some(x) => x; case _ => null }, x385.o_clerk match { case Some(x) => x; case _ => "null" }, x385.o_orderstatus match { case Some(x) => x; case _ => "null" }, x385.o_totalprice match { case Some(x) => x; case _ => 0.0 }, x385.o_orderkey match { case Some(x) => x; case _ => 0 }, x385.o_comment match { case Some(x) => x; case _ => "null" }))
            }).toSeq
          Record00c257793c9a4d749aad1a51f23af8dd(key.c_acctbal, key.c_name, key.Customer_index, key.c_nationkey, key.c_custkey, key.c_comment, key.c_address, grp, key.c_mktsegment, key.c_phone)
      }.as[Record00c257793c9a4d749aad1a51f23af8dd]

      val x387 = x386
      val Test2FullIn = x387


      val x405 = Test2FullIn.flatMap { case x403 =>
        x403.c_orders.map(x404 => Recordf071514c62c046c4a4f183bb484fa3ec(x403.c_name, x404.o_parts))
      }.as[Recordf071514c62c046c4a4f183bb484fa3ec]

      val x410 = x405.flatMap { case x406 =>
        x406.o_parts.foldLeft(HashMap.empty[Recordd1050b4424f24794a99d04f1db5c17e2, Double].withDefaultValue(0.0))(
          (acc, x407) => {
            acc(Recordd1050b4424f24794a99d04f1db5c17e2(x406.c_name, x407.l_partkey)) += x407.l_quantity.asInstanceOf[Double];
            acc
          }
        ).map(x407 => Recordda3d7afd6f8c47eaacb2fbcc0d70e830(x407._1.c_name, x407._2, x407._1.l_partkey))

      }.as[Recordda3d7afd6f8c47eaacb2fbcc0d70e830]

      val x413 = x410.equiJoin(PartDF.as[Part],
        Seq("l_partkey"), Seq("p_partkey"), "inner").as[Recordbc666a79818e4b99a54068a55af0777a]

      val x415 = x413.select("c_name", "l_quantity", "p_retailprice")
        .withColumn("total", (col("l_quantity") * col("p_retailprice")))
        .withColumn("total", when(col("total").isNull, 0.0).otherwise(col("total")))
        .as[Record18ad9a018fe7469f93c9237c44881d27]

      val x417 = x415.groupByKey(x416 => Record6a46d99a2a6545c891d52849ef40e630(x416.c_name))
        .agg(typed.sum[Record18ad9a018fe7469f93c9237c44881d27](x416 => x416.total)
        ).mapPartitions { it =>
        it.map { case (key, total) =>
          Record18ad9a018fe7469f93c9237c44881d27(key.c_name, total)
        }
      }.as[Record18ad9a018fe7469f93c9237c44881d27]

      val x418 = x417
      x418.show(false)
      x418.printSchema()
    }

    //    it("Test1Agg1") {
    //      import spark.implicits._
    //
    //      val x18 = Test1Full.flatMap { case x14 =>
    //        x14.o_parts.foldLeft(HashMap.empty[Recordc5c246937a1748b99480bb485044f7b6, Double].withDefaultValue(0.0))(
    //          (acc, x15) => {
    //            acc(Recordc5c246937a1748b99480bb485044f7b6(x14.o_orderdate, x15.l_partkey)) += x15.l_quantity.asInstanceOf[Double]; acc
    //          }
    //        ).map(x15 => Record8dd0378e7ca94b06aac0e061e650c623(x15._1.o_orderdate, x15._2, x15._1.l_partkey))
    //
    //      }.as[Record8dd0378e7ca94b06aac0e061e650c623]
    //
    //      val x21 = x18.equiJoin(Part,
    //        Seq("l_partkey"), Seq("p_partkey"), "inner").as[Recordca3ca0c17eff492881a70852cae9c38a]
    //
    //      val x23 = x21.select("o_orderdate", "l_quantity", "p_retailprice")
    //        .withColumn("total", (col("l_quantity") * col("p_retailprice")))
    //        .withColumn("total", when(col("total").isNull, 0.0).otherwise(col("total")))
    //        .as[Record2444e40ce12849a7b7a55450a3467447]
    //
    //      val x25 = x23.groupByKey(x24 => Recordfe39850b725d4bd19e771f16fe6e390e(x24.o_orderdate))
    //        .agg(typed.sum[Record2444e40ce12849a7b7a55450a3467447](x24 => x24.total)
    //        ).mapPartitions { it =>
    //        it.map { case (key, total) =>
    //          Record2444e40ce12849a7b7a55450a3467447(key.o_orderdate, total)
    //        }
    //      }.as[Record2444e40ce12849a7b7a55450a3467447]
    //    }
  }

  describe("Nested to Nested") {
    it("Test2NN") {
      import spark.implicits._

      val x24 = Test2Full.select("c_name", "c_orders")

        .as[Record4c1c9f4a7b6548efbc431824cbf2c2b9]

      val x25 = x24.withColumn("Test2Full_index", monotonically_increasing_id())
        .as[Record0b94591285f94327b2c77f47c2073b98]

      val x26 = x25.withColumn("c_orders_index", monotonically_increasing_id())
        .as[Recordaf046ffd9cac4009aa92b4b0a24323fe]

      val x29 = x26.flatMap {
        case x27 =>
          if (x27.c_orders.isEmpty) Seq(Recordd1d5bc6ece264775aea2145439a0a040(x27.c_orders_index, None, x27.c_name, None, x27.Test2Full_index))
          else x27.c_orders.map(x28 => Recordd1d5bc6ece264775aea2145439a0a040(x27.c_orders_index, Some(x28.o_orderdate), x27.c_name, Some(x28.o_parts), x27.Test2Full_index))

      }.as[Recordd1d5bc6ece264775aea2145439a0a040]

      val x35 = x29.flatMap {
        case x30 =>
          x30.o_parts match {
            case Some(o_parts) if o_parts.nonEmpty =>
              o_parts.foldLeft(HashMap.empty[Record17a1241e842d40d89068acfa5d003fa2, Double].withDefaultValue(0.0))(
                (acc, x32) => {
                  acc(Record17a1241e842d40d89068acfa5d003fa2(x30.c_orders_index, x30.o_orderdate, x30.c_name, x30.Test2Full_index, Some(x32.l_partkey))) += x32.l_quantity.asInstanceOf[Double]; acc
                }
              ).map(x32 => Record177e89dab2c24dcaa28f91f86c2be01b(x32._1.c_orders_index, x32._1.o_orderdate, Some(x32._2), x32._1.c_name, x32._1.Test2Full_index, x32._1.l_partkey))

            case _ => Seq(Record177e89dab2c24dcaa28f91f86c2be01b(x30.c_orders_index, x30.o_orderdate, None, x30.c_name, x30.Test2Full_index, None))
          }

      }.as[Record177e89dab2c24dcaa28f91f86c2be01b]

      val x37 = PartDF.select("p_name", "p_retailprice", "p_partkey")

        .as[Record8312915a66844c539580a903e16a9e3f]

      val x40 = x35.equiJoin(x37,
        Seq("l_partkey"), Seq("p_partkey"), "left_outer").as[Recordf754d1d92a9949bc9c7ded6bcd1b1769]

      val x42 = x40.select("p_name", "c_orders_index", "o_orderdate", "l_quantity", "p_retailprice", "c_name", "Test2Full_index")
        .withColumn("total", (col("l_quantity") * col("p_retailprice")))
        .withColumn("total", when(col("total").isNull, 0.0).otherwise(col("total")))
        .as[Recordedc158e4c91d4db4aaad5f25dfc72bcf]

      val x44 = x42.groupByKey(x43 => Recorda7c53519ba3246f385ba6a5e5a5b34bc(x43.p_name, x43.c_orders_index, x43.o_orderdate, x43.c_name, x43.Test2Full_index))
        .agg(typed.sum[Recordedc158e4c91d4db4aaad5f25dfc72bcf](x43 => x43.total)
        ).mapPartitions { it =>
        it.map { case (key, total) =>
          Recordedc158e4c91d4db4aaad5f25dfc72bcf(key.p_name, key.c_orders_index, key.o_orderdate, total, key.c_name, key.Test2Full_index)
        }
      }.as[Recordedc158e4c91d4db4aaad5f25dfc72bcf]

      val x46 = x44


      val x48 = x46.groupByKey(x47 => Recordeebfd34b0d9f4ef883300d447b47160b(x47.c_orders_index, x47.o_orderdate, x47.c_name, x47.Test2Full_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x47 =>
            (x47.p_name) match {
              case (None) => Seq()
              case _ => Seq(Recorda8a26120cd264ae493f8162ff5af06e9(x47.p_name match { case Some(x) => x; case _ => "null" }, x47.total))
            }).toSeq
          Recordcdc3a0cbfc5347ee9c9d4d49113c4eb5(key.c_orders_index, key.o_orderdate, key.c_name, grp, key.Test2Full_index)
      }.as[Recordcdc3a0cbfc5347ee9c9d4d49113c4eb5]

      val x50 = x48.select("c_name", "Test2Full_index", "o_orderdate", "o_parts")

        .as[Recordcc499be223114b28a48c5b6c827b609a]

      val x52 = x50.groupByKey(x51 => Recordb22461c585ad411683a87afbc5ba6cfb(x51.c_name, x51.Test2Full_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x51 =>
            (x51.o_orderdate) match {
              case (None) => Seq()
              case _ => Seq(Recordf28c7812ec5e435ca6bea61bfc0dfd2a(x51.o_orderdate match { case Some(x) => x; case _ => "null" }, x51.o_parts))
            }).toSeq
          Record97a6e22e71ca4a9fad9e596ca0081ab6(key.c_name, key.Test2Full_index, grp)
      }.as[Record97a6e22e71ca4a9fad9e596ca0081ab6]

      val x53 = x52
      x53.show(false)
      x53.printSchema()
    }
  }

}
