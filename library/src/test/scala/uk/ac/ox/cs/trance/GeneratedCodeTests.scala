package uk.ac.ox.cs.trance


import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import uk.ac.ox.cs.trance.utilities.{JoinContext, Symbol}
import org.apache.spark.sql.expressions.scalalang._
import framework.examples.tpch._

import scala.collection.mutable.HashMap
import framework.examples.tpch.Lineitem
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import uk.ac.ox.cs.trance.utilities.SkewDataset.DatasetOps
import uk.ac.ox.cs.trance.records._
import uk.ac.ox.cs.trance.utilities.TPCHDataframes.{spark, Region => RegionDF, Nation => NationDF, Supplier => SupplierDF, LineItem => LineItemDF, Customer => CustomerDF, Part => PartDF, Order => OrderDF}

object GeneratedCodeTests {
  import spark.implicits._

  def Test0() = {
    val x6 = LineItemDF.as[Lineitem].select("l_quantity", "l_partkey").as[Record88388f468d5048a08ce8634058ef9f64]
    x6
  }
  def Test0Full() = {
    LineItemDF.as[Lineitem]
  }
  def Test1() = {
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

    x23
  }
  def Test1Full() = {
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

    x23
  }
  def Test2() = {
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

    x40
  }
  def Test2Filter() = {
    val x20 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

      .as[Record2dc37b6c982f4fc5864bfea8830b976e]

    val x21 = x20.withColumn("Customer_index", monotonically_increasing_id())
      .as[Record99b6f24fbc634ee9b3639e57d1bf20c1]

    val x23 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Recorde3649c70440340cfad4b3323d08e2e3b]

    val x24 = x23.withColumn("Order_index", monotonically_increasing_id())
      .as[Record6687cebc34a748a8bb09d5668686082e]

    val x27 = x21.join(x24, col("c_custkey") === col("o_custkey") && col("c_nationkey") === 1, "left_outer")
      .as[Recordd220fd669d7e41f1b86a3aabecac4237]

    val x29 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

      .as[Record478e4064b2aa4a81882aa61634d044c9]

    val x32 = x27.equiJoin(x29,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record811a07786a1742d68b5b3e724eb7e675]

    val x34 = x32.select("o_orderdate", "l_quantity", "Order_index", "c_name", "Customer_index", "l_partkey")

      .as[Record7b14754fea1d449e84f101370561bb95]

    val x36 = x34.groupByKey(x35 => Record673d683609d24f698c9fab8a214ccb52(x35.o_orderdate, x35.Order_index, x35.c_name, x35.Customer_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x35 =>
          (x35.l_quantity, x35.l_partkey) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recorda14eb38fa0b14107a1e4e87ea2d35fde(x35.l_partkey match { case Some(x) => x; case _ => 0 }, x35.l_quantity match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Recordbcc1e90432f548a2b9eb13a23cafb152(key.o_orderdate, key.Order_index, key.c_name, grp, key.Customer_index)
    }.as[Recordbcc1e90432f548a2b9eb13a23cafb152]

    val x38 = x36.select("Customer_index", "c_name", "o_orderdate", "o_parts")

      .as[Record190f73c1f64d49a297297765c57c8bde]

    val x40 = x38.groupByKey(x39 => Record5c1622d89e264a809d3f13e0d4a12ed8(x39.c_name, x39.Customer_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x39 =>
          (x39.o_orderdate) match {
            case (None) => Seq()
            case _ => Seq(Record77304327a217441fa8f29bdd4a041d34(x39.o_orderdate match { case Some(x) => x; case _ => "null" }, x39.o_parts))
          }).toSeq
        Record3df1924806c44fcdafc4395b39bd3b4c(key.Customer_index, key.c_name, grp)
    }.as[Record3df1924806c44fcdafc4395b39bd3b4c]

    x40
  }
  def Test2Flat() = {
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

    x47
  }
  def Test2Full() = {
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
  def Test3() = {
    val x27 = NationDF.select("n_nationkey", "n_name")

      .as[Record4bc96273798a48dbbd8b9b59e602f98f]

    val x28 = x27.withColumn("Nation_index", monotonically_increasing_id())
      .as[Record7652bde34e734b7290fed733e14083f5]

    val x30 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

      .as[Recordc5bad097b62849d3ad5f59b89fdacfff]

    val x31 = x30.withColumn("Customer_index", monotonically_increasing_id())
      .as[Recordc708acd0959b4cf4ae9dfdbe03d0b6d8]

    val x34 = x28.equiJoin(x31,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Recordb60a8344502f47fab8ef9919ef841108]

    val x36 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Recordd7d67760d83d485cb0155efaa979de76]

    val x37 = x36.withColumn("Order_index", monotonically_increasing_id())
      .as[Record0c5f1ba29ab542198e75d05f4f91d188]

    val x40 = x34.equiJoin(x37,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record0965651bd6d64b5b97462337046a785c]

    val x42 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

      .as[Recordead73b9327c845b19a576d55c8772608]

    val x45 = x40.equiJoin(x42,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record50222bda0d3f456580322dc256ea021f]

    val x47 = x45.select("o_orderdate", "l_quantity", "Order_index", "c_name", "Customer_index", "n_name", "l_partkey", "Nation_index")

      .as[Recordd72b1fa4412744f6bb579aeeb320167e]

    val x49 = x47.groupByKey(x48 => Record841d75b461834c73a49a68381353797f(x48.o_orderdate, x48.Order_index, x48.c_name, x48.Customer_index, x48.n_name, x48.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x48 =>
          (x48.l_quantity, x48.l_partkey) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record9eb9e264b3dc4542938c6da74698494f(x48.l_partkey match { case Some(x) => x; case _ => 0 }, x48.l_quantity match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Recordccaf0d739c1540fa8adf486cd6159a52(key.o_orderdate, key.Order_index, key.c_name, grp, key.Customer_index, key.n_name, key.Nation_index)
    }.as[Recordccaf0d739c1540fa8adf486cd6159a52]

    val x51 = x49.select("o_orderdate", "c_name", "o_parts", "Customer_index", "n_name", "Nation_index")

      .as[Record72cd036e6e5b43c0b075318c9e0b7aa2]

    val x53 = x51.groupByKey(x52 => Recordd0aa0c5866c84b1dbc4dd56fa3719017(x52.c_name, x52.Customer_index, x52.n_name, x52.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x52 =>
          (x52.o_orderdate) match {
            case (None) => Seq()
            case _ => Seq(Recordfa8a4c9d393e423aad0f1a12e741e0ce(x52.o_orderdate match { case Some(x) => x; case _ => "null" }, x52.o_parts))
          }).toSeq
        Record5efd772475f148f689106c5d73a834d4(key.c_name, key.Customer_index, key.n_name, key.Nation_index, grp)
    }.as[Record5efd772475f148f689106c5d73a834d4]

    val x55 = x53.select("Nation_index", "n_name", "c_name", "c_orders")

      .as[Record7f46b46034344e9d8a85cf816f335fc2]

    val x57 = x55.groupByKey(x56 => Record8eca9a05ebb5485bab4baf21de0467f3(x56.n_name, x56.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x56 =>
          (x56.c_name) match {
            case (None) => Seq()
            case _ => Seq(Record58c5ef6ef627444da6b8c09a198ded34(x56.c_name match { case Some(x) => x; case _ => "null" }, x56.c_orders))
          }).toSeq
        Record557fc3d79aef4b4a8f5f9bc375304b29(key.Nation_index, key.n_name, grp)
    }.as[Record557fc3d79aef4b4a8f5f9bc375304b29]

    x57
  }
  def Test3Flat() = {
    val x100 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Record308caff09dd846339ca9a4bcecc6e4b9]

    val x101 = x100.withColumn("Order_index", monotonically_increasing_id())
      .as[Recordff8cb3dc0ee841d8bbdb6f1e68382b7f]

    val x103 = LineItemDF.as[Lineitem].select("l_quantity", "l_partkey", "l_orderkey")

      .as[Record6bcf63d4b9f948ad8420752abb3bf19e]

    val x106 = x101.equiJoin(x103,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record45e99d516378432e8b755bb0f1a7b171]

    val x108 = x106.select("o_orderdate", "o_custkey", "l_quantity", "Order_index", "l_partkey")

      .as[Record56e1dc84adef4623b33155d9871eefe7]

    val x110 = x108.groupByKey(x109 => Recordd97ff0d82fa148a3a87b9261aff9360c(x109.o_orderdate, x109.o_custkey, x109.Order_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x109 =>
          (x109.l_quantity, x109.l_partkey) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recorde4ff9c3988894e379b56975ff7c0f048(x109.l_partkey match { case Some(x) => x; case _ => 0 }, x109.l_quantity match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Record94e92e870ada4a8c88a0c018425013e8(key.o_orderdate, key.o_custkey, key.Order_index, grp)
    }.as[Record94e92e870ada4a8c88a0c018425013e8]

    val x111 = x110
    val orders = x111
    //orders.cache
    //orders.count
    val x113 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

      .as[Recordd9032eb3082045c3838d7d3f8502c447]

    val x114 = x113.withColumn("Customer_index", monotonically_increasing_id())
      .as[Record89bfa633628a4932843316aca8d79b42]

    val x116 = orders


    val x119 = x114.equiJoin(x116,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Recordb73e042006a2438fab495271c4d583f3]

    val x121 = x119.select("o_orderdate", "c_name", "o_parts", "Customer_index", "c_nationkey")

      .as[Recordbed4ee211bf14c77a3f883f363a98a9a]

    val x123 = x121.groupByKey(x122 => Record56946bd650ac4df487f47319b7d2ec04(x122.c_name, x122.Customer_index, x122.c_nationkey)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x122 =>
          (x122.o_orderdate, x122.o_parts) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record237c6092cc144a30bdcdf78fc515081d(x122.o_orderdate match { case Some(x) => x; case _ => "null" }, x122.o_parts match { case Some(x) => x; case _ => null }))
          }).toSeq
        Record87b7dfe3237547ac8ddc4a0a3ee1aff0(key.c_name, key.Customer_index, key.c_nationkey, grp)
    }.as[Record87b7dfe3237547ac8ddc4a0a3ee1aff0]

    val x124 = x123
    val customers = x124
    //customers.cache
    //customers.count
    val x126 = NationDF.select("n_nationkey", "n_name")

      .as[Recorda7fd1b3565e44bc6a0e9b389a7d73798]

    val x127 = x126.withColumn("Nation_index", monotonically_increasing_id())
      .as[Record88b1410b46674508a579ea2bcaa70623]

    val x129 = customers


    val x132 = x127.equiJoin(x129,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Record941e281521c44dcabcd3a6f23def4098]

    val x134 = x132.select("Nation_index", "n_name", "c_name", "c_orders")

      .as[Record81489576e80c4ce98220f3a59436c964]

    val x136 = x134.groupByKey(x135 => Record0e3fe5e6f6b6496a9571e8d03b180fbe(x135.n_name, x135.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x135 =>
          (x135.c_name, x135.c_orders) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record850cc7de4c5a436087ecfb8ab8cdba1d(x135.c_name match { case Some(x) => x; case _ => "null" }, x135.c_orders match { case Some(x) => x; case _ => null }))
          }).toSeq
        Record82604242888c4353a8037d8b1fd9100b(key.Nation_index, key.n_name, grp)
    }.as[Record82604242888c4353a8037d8b1fd9100b]

    x136
  }
  def Test3Full() = {
    val x34 = NationDF


    val x35 = x34.withColumn("Nation_index", monotonically_increasing_id())
      .as[Recordd296bd470a3d4561bc89c72659278dc4]

    val x37 = CustomerDF


    val x38 = x37.withColumn("Customer_index", monotonically_increasing_id())
      .as[Record29a8f2f9ed12488f8c0ee5e744b33731]

    val x41 = x35.equiJoin(x38,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Record9d691fc50b6242b8ada42e48bbd616b5]

    val x43 = OrderDF


    val x44 = x43.withColumn("Order_index", monotonically_increasing_id())
      .as[Record15cf66c1c3ec4fddb84261b3331af0ec]

    val x47 = x41.equiJoin(x44,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record2a1121a4b7594e5eaff390fe369d0391]

    val x49 = LineItemDF.as[Lineitem]


    val x52 = x47.equiJoin(x49,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record0f3e469479e741c6bf54d77b1ddc0958]

    val x54 = x52


    val x56 = x54.groupByKey(x55 => Record2a1121a4b7594e5eaff390fe369d0391(x55.o_shippriority, x55.c_acctbal, x55.n_regionkey, x55.n_nationkey, x55.o_orderdate, x55.o_custkey, x55.o_orderpriority, x55.Order_index, x55.c_name, x55.Customer_index, x55.c_nationkey, x55.n_name, x55.o_clerk, x55.o_orderstatus, x55.c_custkey, x55.c_comment, x55.Nation_index, x55.c_address, x55.c_mktsegment, x55.n_comment, x55.o_totalprice, x55.o_orderkey, x55.c_phone, x55.o_comment)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x55 =>
          (x55.l_commitdate, x55.l_orderkey, x55.l_discount, x55.l_linenumber, x55.l_receiptdate, x55.l_linestatus, x55.l_quantity, x55.l_suppkey, x55.l_tax, x55.l_shipinstruct, x55.l_partkey, x55.l_comment, x55.l_returnflag, x55.l_shipmode, x55.l_extendedprice, x55.l_shipdate) match {
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
            case _ => Seq(Recordea71b56b7f4c4652b8ab31215b8fda19(x55.l_returnflag match { case Some(x) => x; case _ => "null" }, x55.l_comment match { case Some(x) => x; case _ => "null" }, x55.l_linestatus match { case Some(x) => x; case _ => "null" }, x55.l_shipmode match { case Some(x) => x; case _ => "null" }, x55.l_shipinstruct match { case Some(x) => x; case _ => "null" }, x55.l_quantity match { case Some(x) => x; case _ => 0.0 }, x55.l_receiptdate match { case Some(x) => x; case _ => "null" }, x55.l_linenumber match { case Some(x) => x; case _ => 0 }, x55.l_tax match { case Some(x) => x; case _ => 0.0 }, x55.l_shipdate match { case Some(x) => x; case _ => "null" }, x55.l_extendedprice match { case Some(x) => x; case _ => 0.0 }, x55.l_partkey match { case Some(x) => x; case _ => 0 }, x55.l_discount match { case Some(x) => x; case _ => 0.0 }, x55.l_commitdate match { case Some(x) => x; case _ => "null" }, x55.l_suppkey match { case Some(x) => x; case _ => 0 }, x55.l_orderkey match { case Some(x) => x; case _ => 0 }))
          }).toSeq
        Record3ad3872c1f4a43e6a2fe473dc7e6a1a2(key.o_shippriority, key.c_acctbal, key.n_regionkey, key.n_nationkey, key.o_orderdate, key.o_custkey, key.o_orderpriority, key.Order_index, key.c_name, grp, key.Customer_index, key.c_nationkey, key.n_name, key.o_clerk, key.o_orderstatus, key.c_custkey, key.c_comment, key.Nation_index, key.c_address, key.c_mktsegment, key.n_comment, key.o_totalprice, key.o_orderkey, key.c_phone, key.o_comment)
    }.as[Record3ad3872c1f4a43e6a2fe473dc7e6a1a2]

    val x58 = x56.select("o_shippriority", "c_acctbal", "n_regionkey", "n_nationkey", "o_orderdate", "o_custkey", "o_orderpriority", "c_name", "o_parts", "Customer_index", "c_nationkey", "n_name", "o_clerk", "o_orderstatus", "c_custkey", "c_comment", "Nation_index", "c_address", "c_mktsegment", "n_comment", "o_totalprice", "o_orderkey", "c_phone", "o_comment")

      .as[Record0dc3b05c9a7341b3b7b7655e8911adf2]

    val x60 = x58.groupByKey(x59 => Record9d691fc50b6242b8ada42e48bbd616b5(x59.c_acctbal, x59.n_regionkey, x59.n_nationkey, x59.c_name, x59.Customer_index, x59.c_nationkey, x59.n_name, x59.c_custkey, x59.c_comment, x59.Nation_index, x59.c_address, x59.c_mktsegment, x59.n_comment, x59.c_phone)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x59 =>
          (x59.o_comment, x59.o_totalprice, x59.o_shippriority, x59.o_custkey, x59.o_orderstatus, x59.o_orderpriority, x59.o_orderdate, x59.o_clerk, x59.o_orderkey) match {
            case (None, _, _, _, _, _, _, _, _) => Seq()
            case (_, None, _, _, _, _, _, _, _) => Seq()
            case (_, _, None, _, _, _, _, _, _) => Seq()
            case (_, _, _, None, _, _, _, _, _) => Seq()
            case (_, _, _, _, None, _, _, _, _) => Seq()
            case (_, _, _, _, _, None, _, _, _) => Seq()
            case (_, _, _, _, _, _, None, _, _) => Seq()
            case (_, _, _, _, _, _, _, None, _) => Seq()
            case (_, _, _, _, _, _, _, _, None) => Seq()
            case _ => Seq(Record664b86e98a004545ab053e5d9b73378f(x59.o_shippriority match { case Some(x) => x; case _ => 0 }, x59.o_orderdate match { case Some(x) => x; case _ => "null" }, x59.o_custkey match { case Some(x) => x; case _ => 0 }, x59.o_orderpriority match { case Some(x) => x; case _ => "null" }, x59.o_parts, x59.o_clerk match { case Some(x) => x; case _ => "null" }, x59.o_orderstatus match { case Some(x) => x; case _ => "null" }, x59.o_totalprice match { case Some(x) => x; case _ => 0.0 }, x59.o_orderkey match { case Some(x) => x; case _ => 0 }, x59.o_comment match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Recordc4e932d27c2d401cbc59a4af57877a82(key.c_acctbal, key.n_regionkey, key.n_nationkey, key.c_name, key.Customer_index, key.c_nationkey, key.n_name, key.c_custkey, key.c_comment, key.Nation_index, key.c_address, grp, key.c_mktsegment, key.n_comment, key.c_phone)
    }.as[Recordc4e932d27c2d401cbc59a4af57877a82]

    val x62 = x60.select("c_acctbal", "n_regionkey", "n_nationkey", "c_name", "c_nationkey", "n_name", "c_custkey", "c_comment", "Nation_index", "c_address", "c_orders", "c_mktsegment", "n_comment", "c_phone")

      .as[Record0b0955d789274ccf8956fc8721e835c2]

    val x64 = x62.groupByKey(x63 => Recordd296bd470a3d4561bc89c72659278dc4(x63.n_regionkey, x63.n_nationkey, x63.n_name, x63.Nation_index, x63.n_comment)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x63 =>
          (x63.c_nationkey, x63.c_acctbal, x63.c_address, x63.c_mktsegment, x63.c_name, x63.c_comment, x63.c_custkey, x63.c_phone) match {
            case (None, _, _, _, _, _, _, _) => Seq()
            case (_, None, _, _, _, _, _, _) => Seq()
            case (_, _, None, _, _, _, _, _) => Seq()
            case (_, _, _, None, _, _, _, _) => Seq()
            case (_, _, _, _, None, _, _, _) => Seq()
            case (_, _, _, _, _, None, _, _) => Seq()
            case (_, _, _, _, _, _, None, _) => Seq()
            case (_, _, _, _, _, _, _, None) => Seq()
            case _ => Seq(Recordae040856c52a47ebb1c42b0de677c33c(x63.c_acctbal match { case Some(x) => x; case _ => 0.0 }, x63.c_name match { case Some(x) => x; case _ => "null" }, x63.c_nationkey match { case Some(x) => x; case _ => 0 }, x63.c_custkey match { case Some(x) => x; case _ => 0 }, x63.c_comment match { case Some(x) => x; case _ => "null" }, x63.c_address match { case Some(x) => x; case _ => "null" }, x63.c_orders, x63.c_mktsegment match { case Some(x) => x; case _ => "null" }, x63.c_phone match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Recordaad9856bcd554d918ec96d4ca0c51372(key.n_regionkey, key.n_nationkey, grp, key.n_name, key.Nation_index, key.n_comment)
    }.as[Recordaad9856bcd554d918ec96d4ca0c51372]

    x64
  }
  def Test3FullFlat() = {
    val x172 = OrderDF


    val x173 = x172.withColumn("Order_index", monotonically_increasing_id())
      .as[Recordc827c44e52e24155a92791fd148a236c]

    val x175 = LineItemDF.as[Lineitem]


    val x178 = x173.equiJoin(x175,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record99ada78e17d6454f902a4b464a2ca615]

    val x180 = x178


    val x182 = x180.groupByKey(x181 => Recordc827c44e52e24155a92791fd148a236c(x181.o_shippriority, x181.o_orderdate, x181.o_custkey, x181.o_orderpriority, x181.Order_index, x181.o_clerk, x181.o_orderstatus, x181.o_totalprice, x181.o_orderkey, x181.o_comment)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x181 =>
          (x181.l_comment, x181.l_discount, x181.l_shipinstruct, x181.l_linestatus, x181.l_returnflag, x181.l_orderkey, x181.l_linenumber, x181.l_suppkey, x181.l_receiptdate, x181.l_shipdate, x181.l_quantity, x181.l_commitdate, x181.l_tax, x181.l_shipmode, x181.l_extendedprice, x181.l_partkey) match {
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
            case _ => Seq(Record14d65268338644e8a283b1d8f5b7574b(x181.l_returnflag match { case Some(x) => x; case _ => "null" }, x181.l_comment match { case Some(x) => x; case _ => "null" }, x181.l_linestatus match { case Some(x) => x; case _ => "null" }, x181.l_shipmode match { case Some(x) => x; case _ => "null" }, x181.l_shipinstruct match { case Some(x) => x; case _ => "null" }, x181.l_quantity match { case Some(x) => x; case _ => 0.0 }, x181.l_receiptdate match { case Some(x) => x; case _ => "null" }, x181.l_linenumber match { case Some(x) => x; case _ => 0 }, x181.l_tax match { case Some(x) => x; case _ => 0.0 }, x181.l_shipdate match { case Some(x) => x; case _ => "null" }, x181.l_extendedprice match { case Some(x) => x; case _ => 0.0 }, x181.l_partkey match { case Some(x) => x; case _ => 0 }, x181.l_discount match { case Some(x) => x; case _ => 0.0 }, x181.l_commitdate match { case Some(x) => x; case _ => "null" }, x181.l_suppkey match { case Some(x) => x; case _ => 0 }, x181.l_orderkey match { case Some(x) => x; case _ => 0 }))
          }).toSeq
        Record812a21384bd8443a8505adf11dcb5b42(key.o_shippriority, key.o_orderdate, key.o_custkey, key.o_orderpriority, key.Order_index, grp, key.o_clerk, key.o_orderstatus, key.o_totalprice, key.o_orderkey, key.o_comment)
    }.as[Record812a21384bd8443a8505adf11dcb5b42]

    val x183 = x182
    val orders = x183
    //orders.cache
    //orders.count
    val x185 = CustomerDF


    val x186 = x185.withColumn("Customer_index", monotonically_increasing_id())
      .as[Recordc986fa3a9ed74726a858745f0e0eda36]

    val x188 = orders


    val x191 = x186.equiJoin(x188,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record5c4674fd244b493d8379bd94d9efe168]

    val x193 = x191


    val x195 = x193.groupByKey(x194 => Recordc986fa3a9ed74726a858745f0e0eda36(x194.c_acctbal, x194.c_name, x194.Customer_index, x194.c_nationkey, x194.c_custkey, x194.c_comment, x194.c_address, x194.c_mktsegment, x194.c_phone)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x194 =>
          (x194.o_clerk, x194.o_orderpriority, x194.o_custkey, x194.o_orderkey, x194.o_orderstatus, x194.o_totalprice, x194.o_shippriority, x194.o_comment, x194.o_parts, x194.o_orderdate) match {
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
            case _ => Seq(Recordb0cde6f8f24b47238586c795db47cc70(x194.o_shippriority match { case Some(x) => x; case _ => 0 }, x194.o_orderdate match { case Some(x) => x; case _ => "null" }, x194.o_custkey match { case Some(x) => x; case _ => 0 }, x194.o_orderpriority match { case Some(x) => x; case _ => "null" }, x194.o_parts match { case Some(x) => x; case _ => null }, x194.o_clerk match { case Some(x) => x; case _ => "null" }, x194.o_orderstatus match { case Some(x) => x; case _ => "null" }, x194.o_totalprice match { case Some(x) => x; case _ => 0.0 }, x194.o_orderkey match { case Some(x) => x; case _ => 0 }, x194.o_comment match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Recordfe060cba2b85491ab08b130f7ec5c28e(key.c_acctbal, key.c_name, key.Customer_index, key.c_nationkey, key.c_custkey, key.c_comment, key.c_address, grp, key.c_mktsegment, key.c_phone)
    }.as[Recordfe060cba2b85491ab08b130f7ec5c28e]

    val x196 = x195
    val customers = x196
    //customers.cache
    //customers.count
    val x198 = NationDF


    val x199 = x198.withColumn("Nation_index", monotonically_increasing_id())
      .as[Recorda9dda84a5f3e41208b5f2fa7aba0274c]

    val x201 = customers


    val x204 = x199.equiJoin(x201,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Record7cf99a9476fd46ee86da320a8be8c148]

    val x206 = x204


    val x208 = x206.groupByKey(x207 => Recorda9dda84a5f3e41208b5f2fa7aba0274c(x207.n_regionkey, x207.n_nationkey, x207.n_name, x207.Nation_index, x207.n_comment)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x207 =>
          (x207.c_custkey, x207.c_mktsegment, x207.c_nationkey, x207.c_acctbal, x207.c_orders, x207.c_phone, x207.c_name, x207.c_comment, x207.c_address) match {
            case (None, _, _, _, _, _, _, _, _) => Seq()
            case (_, None, _, _, _, _, _, _, _) => Seq()
            case (_, _, None, _, _, _, _, _, _) => Seq()
            case (_, _, _, None, _, _, _, _, _) => Seq()
            case (_, _, _, _, None, _, _, _, _) => Seq()
            case (_, _, _, _, _, None, _, _, _) => Seq()
            case (_, _, _, _, _, _, None, _, _) => Seq()
            case (_, _, _, _, _, _, _, None, _) => Seq()
            case (_, _, _, _, _, _, _, _, None) => Seq()
            case _ => Seq(Record340fa58c765f48dba075ca809e34c5d6(x207.c_acctbal match { case Some(x) => x; case _ => 0.0 }, x207.c_name match { case Some(x) => x; case _ => "null" }, x207.c_nationkey match { case Some(x) => x; case _ => 0 }, x207.c_custkey match { case Some(x) => x; case _ => 0 }, x207.c_comment match { case Some(x) => x; case _ => "null" }, x207.c_address match { case Some(x) => x; case _ => "null" }, x207.c_orders match { case Some(x) => x; case _ => null }, x207.c_mktsegment match { case Some(x) => x; case _ => "null" }, x207.c_phone match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Record9d37b785e4ff4e5eb99de4743471a106(key.n_regionkey, key.n_nationkey, grp, key.n_name, key.Nation_index, key.n_comment)
    }.as[Record9d37b785e4ff4e5eb99de4743471a106]

    x208
  }
  def Test4() = {
    val x243 = RegionDF.select("r_regionkey", "r_name")

      .as[Record0f76bb7a92a04a1c81317754fb38d89f]

    val x244 = x243.withColumn("Region_index", monotonically_increasing_id())
      .as[Record2bce90e2069f42679d75e3608f7aa8c6]

    val x246 = NationDF.select("n_nationkey", "n_name", "n_regionkey")

      .as[Record5e3f299021934980b3dc0924a98519f0]

    val x247 = x246.withColumn("Nation_index", monotonically_increasing_id())
      .as[Recordcfb3b5bc3c9041ef865de4c2487d9fd2]

    val x250 = x244.equiJoin(x247,
      Seq("r_regionkey"), Seq("n_regionkey"), "left_outer").as[Recorda471b1200c9a4ea099243b36ccf6b8e4]

    val x252 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

      .as[Recorda5be757d86324e5793327dcf3c8304aa]

    val x253 = x252.withColumn("Customer_index", monotonically_increasing_id())
      .as[Recordd5fb0f83a3ad4c33866003d6da826322]

    val x256 = x250.equiJoin(x253,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Recordae9358a77abd40568afadffbcae3b69c]

    val x258 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Recorded32d27346454fc9ad38b6bd13035d22]

    val x259 = x258.withColumn("Order_index", monotonically_increasing_id())
      .as[Record4ceea5a61ab74d40b8ea5d5c78f2d5ee]

    val x262 = x256.equiJoin(x259,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record29c7737ed15b4146b17770cadf2c8704]

    val x264 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

      .as[Recordcd55bbe231b24de98cf4653144547193]

    val x267 = x262.equiJoin(x264,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record75f1a44d6e5a4f9daa8df36faf6d02d4]

    val x269 = x267.select("o_orderdate", "l_quantity", "Order_index", "c_name", "Customer_index", "n_name", "l_partkey", "r_name", "Nation_index", "Region_index")

      .as[Record1893a8dff663458784b81570f023f376]

    val x271 = x269.groupByKey(x270 => Recordacc116145b9448ebb24acdcddf7220c9(x270.o_orderdate, x270.Order_index, x270.c_name, x270.Customer_index, x270.n_name, x270.r_name, x270.Nation_index, x270.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x270 =>
          (x270.l_quantity, x270.l_partkey) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recordae74b81efc06425c9cf9dc5e81e81af5(x270.l_partkey match { case Some(x) => x; case _ => 0 }, x270.l_quantity match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Record6a0ae12f3225441fa49ea83696f94bbe(key.o_orderdate, key.Order_index, key.c_name, grp, key.Customer_index, key.n_name, key.r_name, key.Nation_index, key.Region_index)
    }.as[Record6a0ae12f3225441fa49ea83696f94bbe]

    val x273 = x271.select("o_orderdate", "c_name", "o_parts", "Customer_index", "n_name", "r_name", "Nation_index", "Region_index")

      .as[Recordffe8673685804bd4b07ee477e03ddc4c]

    val x275 = x273.groupByKey(x274 => Recordb505d584a03d416bb4e966910a46ba21(x274.c_name, x274.Customer_index, x274.n_name, x274.r_name, x274.Nation_index, x274.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x274 =>
          (x274.o_orderdate) match {
            case (None) => Seq()
            case _ => Seq(Recordefee6f1a927946b4b32d80e73c816f70(x274.o_orderdate match { case Some(x) => x; case _ => "null" }, x274.o_parts))
          }).toSeq
        Record367d99efcf504091a84b3c32c520831d(key.c_name, key.Customer_index, key.n_name, key.r_name, key.Nation_index, grp, key.Region_index)
    }.as[Record367d99efcf504091a84b3c32c520831d]

    val x277 = x275.select("c_name", "n_name", "r_name", "Nation_index", "c_orders", "Region_index")

      .as[Recorde611cde1286947bf9c36b1e9f21802ac]

    val x279 = x277.groupByKey(x278 => Recorda143f4d2a7224315b59ea4f36069224a(x278.n_name, x278.r_name, x278.Nation_index, x278.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x278 =>
          (x278.c_name) match {
            case (None) => Seq()
            case _ => Seq(Record31d3a16ba1f14dde92d0088646447a0a(x278.c_name match { case Some(x) => x; case _ => "null" }, x278.c_orders))
          }).toSeq
        Record1f0a4083dd7b4fea89716a976279a3e8(grp, key.n_name, key.r_name, key.Nation_index, key.Region_index)
    }.as[Record1f0a4083dd7b4fea89716a976279a3e8]

    val x281 = x279.select("r_name", "Region_index", "n_name", "n_custs")

      .as[Recorde7c23e6fd6e34eb9a85b026100f9ce61]

    val x283 = x281.groupByKey(x282 => Record71ee61a1749c454d8f9a5a550314383c(x282.r_name, x282.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x282 =>
          (x282.n_name) match {
            case (None) => Seq()
            case _ => Seq(Record6a13018d973142ae9c8d15d297cff005(x282.n_name match { case Some(x) => x; case _ => "null" }, x282.n_custs))
          }).toSeq
        Record396b31014b574fcb8bf9c5795b934411(key.r_name, key.Region_index, grp)
    }.as[Record396b31014b574fcb8bf9c5795b934411]

    x283
  }
  def Test4Flat() = {
    val x330 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Recordb1a701a8b5cd40b296bbe4fc633b1af9]

    val x331 = x330.withColumn("Order_index", monotonically_increasing_id())
      .as[Recordaf11f8aff70e4253911b50d4ebf200fc]

    val x333 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

      .as[Record5c2c420ea3d34169b5d43a2eb030c3f3]

    val x336 = x331.equiJoin(x333,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Recordf7596ec1015b4ed2a3e01b60316e1c1b]

    val x338 = x336.select("o_orderdate", "o_custkey", "l_quantity", "Order_index", "l_partkey")

      .as[Recorde873ce2d74624be1a3c030ea8360fa59]

    val x340 = x338.groupByKey(x339 => Recordb8a957a01bbd44a0874b0fe4aa59b6ba(x339.o_orderdate, x339.o_custkey, x339.Order_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x339 =>
          (x339.l_quantity, x339.l_partkey) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record6133447d0aaa41b39bb2fd74a781e699(x339.l_partkey match { case Some(x) => x; case _ => 0 }, x339.l_quantity match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Record275aac49292749f7a89d8891e4458bf8(key.o_orderdate, key.o_custkey, key.Order_index, grp)
    }.as[Record275aac49292749f7a89d8891e4458bf8]

    val x341 = x340
    val orders = x341
    //orders.cache
    //orders.count
    val x343 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

      .as[Record553ea4ed7c9841bcabc7ec7f7dc7578b]

    val x344 = x343.withColumn("Customer_index", monotonically_increasing_id())
      .as[Recordacf07e09cd854d3885b14b2e7e6ebeb3]

    val x346 = orders


    val x349 = x344.equiJoin(x346,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record03ccb7097fd04b32b58a3fb59b393e0f]

    val x351 = x349.select("o_orderdate", "c_name", "o_parts", "Customer_index", "c_nationkey")

      .as[Record2007a42a14c24a89b2c8141b8adb7f74]

    val x353 = x351.groupByKey(x352 => Record29ebdc5669124c91b1ba337802d0319e(x352.c_name, x352.Customer_index, x352.c_nationkey)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x352 =>
          (x352.o_orderdate, x352.o_parts) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recordcbfaa178ce7247a48c6c96be0d0dadda(x352.o_orderdate match { case Some(x) => x; case _ => "null" }, x352.o_parts match { case Some(x) => x; case _ => null }))
          }).toSeq
        Record550dba97131545c9b0f69fce101a21aa(key.c_name, key.Customer_index, key.c_nationkey, grp)
    }.as[Record550dba97131545c9b0f69fce101a21aa]

    val x354 = x353
    val customers = x354
    //customers.cache
    //customers.count
    val x356 = NationDF.select("n_nationkey", "n_name", "n_regionkey")

      .as[Record8ad5f2f7262a41279c29f32dd3684550]

    val x357 = x356.withColumn("Nation_index", monotonically_increasing_id())
      .as[Recordfe1f8d6f1ad14aef9d0d247bb3ff8829]

    val x359 = customers


    val x362 = x357.equiJoin(x359,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Record8f231823d8464ff1b6da34783e3ca2f9]

    val x364 = x362.select("n_regionkey", "c_name", "n_name", "Nation_index", "c_orders")

      .as[Recorde8d4662a07cc4a1eae31853d687d6c86]

    val x366 = x364.groupByKey(x365 => Recorda9482ef1d0b54a0b8ff2eef5c508f9d6(x365.n_regionkey, x365.n_name, x365.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x365 =>
          (x365.c_name, x365.c_orders) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record9b31036dd5094efc986b4a5ef4d63793(x365.c_name match { case Some(x) => x; case _ => "null" }, x365.c_orders match { case Some(x) => x; case _ => null }))
          }).toSeq
        Recordbeac6de3dd2740a9abd3db4e68b63f25(key.n_regionkey, grp, key.n_name, key.Nation_index)
    }.as[Recordbeac6de3dd2740a9abd3db4e68b63f25]

    val x367 = x366
    val nations = x367
    //nations.cache
    //nations.count
    val x369 = RegionDF.select("r_regionkey", "r_name")

      .as[Record7615621448a047d6ac571f2c288253ab]

    val x370 = x369.withColumn("Region_index", monotonically_increasing_id())
      .as[Record94f38d8004d94e86ae38d0336e609656]

    val x372 = nations


    val x375 = x370.equiJoin(x372,
      Seq("r_regionkey"), Seq("n_regionkey"), "left_outer").as[Record49c80a498d6542468ef960f2b80cbdd0]

    val x377 = x375.select("r_name", "Region_index", "n_name", "n_custs")

      .as[Record480a73bace7e40e294a94a58a16d440e]

    val x379 = x377.groupByKey(x378 => Record531a8eccc067401d9756a984f2a177a5(x378.r_name, x378.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x378 =>
          (x378.n_name, x378.n_custs) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record0147dc21045842babc363357291b7d32(x378.n_name match { case Some(x) => x; case _ => "null" }, x378.n_custs match { case Some(x) => x; case _ => null }))
          }).toSeq
        Recordc7841ef1c4fd4069a4baa2f6feae3d68(key.r_name, key.Region_index, grp)
    }.as[Recordc7841ef1c4fd4069a4baa2f6feae3d68]

    x379
  }
  def Test4Full() = {
    val x510 = RegionDF


    val x511 = x510.withColumn("Region_index", monotonically_increasing_id())
      .as[Record8330f1af0c474f1ba9ea76d0dcb0e244]

    val x513 = NationDF


    val x514 = x513.withColumn("Nation_index", monotonically_increasing_id())
      .as[Record9870a1c47fc64082a68bf55f5c15e639]

    val x517 = x511.equiJoin(x514,
      Seq("r_regionkey"), Seq("n_regionkey"), "left_outer").as[Record65d7fd2be79a4dc78040ba2054a8bba8]

    val x519 = CustomerDF


    val x520 = x519.withColumn("Customer_index", monotonically_increasing_id())
      .as[Recorddaf8e0bfc8c9493499263718d6500b1e]

    val x523 = x517.equiJoin(x520,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Recorda351c3c5be9b4e5c8a84a7ab4686f842]

    val x525 = OrderDF


    val x526 = x525.withColumn("Order_index", monotonically_increasing_id())
      .as[Recordb8ab26ca08834acfb15e10b6d5b23044]

    val x529 = x523.equiJoin(x526,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record8712a6fc315b43139669a0410e309320]

    val x531 = LineItemDF.as[Lineitem]


    val x534 = x529.equiJoin(x531,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record6f9dfdb6be884e6e883c40457f7fa6e4]

    val x536 = x534


    val x538 = x536.groupByKey(x537 => Record8712a6fc315b43139669a0410e309320(x537.o_shippriority, x537.c_acctbal, x537.n_regionkey, x537.n_nationkey, x537.o_orderdate, x537.o_custkey, x537.r_regionkey, x537.o_orderpriority, x537.Order_index, x537.r_comment, x537.c_name, x537.Customer_index, x537.c_nationkey, x537.n_name, x537.o_clerk, x537.o_orderstatus, x537.r_name, x537.c_custkey, x537.c_comment, x537.Nation_index, x537.c_address, x537.c_mktsegment, x537.n_comment, x537.o_totalprice, x537.o_orderkey, x537.c_phone, x537.Region_index, x537.o_comment)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x537 =>
          (x537.l_shipdate, x537.l_extendedprice, x537.l_discount, x537.l_shipmode, x537.l_shipinstruct, x537.l_orderkey, x537.l_suppkey, x537.l_quantity, x537.l_tax, x537.l_linenumber, x537.l_returnflag, x537.l_commitdate, x537.l_comment, x537.l_linestatus, x537.l_receiptdate, x537.l_partkey) match {
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
            case _ => Seq(Record65c4af33c6f740df9846315e3d64cb2a(x537.l_returnflag match { case Some(x) => x; case _ => "null" }, x537.l_comment match { case Some(x) => x; case _ => "null" }, x537.l_linestatus match { case Some(x) => x; case _ => "null" }, x537.l_shipmode match { case Some(x) => x; case _ => "null" }, x537.l_shipinstruct match { case Some(x) => x; case _ => "null" }, x537.l_quantity match { case Some(x) => x; case _ => 0.0 }, x537.l_receiptdate match { case Some(x) => x; case _ => "null" }, x537.l_linenumber match { case Some(x) => x; case _ => 0 }, x537.l_tax match { case Some(x) => x; case _ => 0.0 }, x537.l_shipdate match { case Some(x) => x; case _ => "null" }, x537.l_extendedprice match { case Some(x) => x; case _ => 0.0 }, x537.l_partkey match { case Some(x) => x; case _ => 0 }, x537.l_discount match { case Some(x) => x; case _ => 0.0 }, x537.l_commitdate match { case Some(x) => x; case _ => "null" }, x537.l_suppkey match { case Some(x) => x; case _ => 0 }, x537.l_orderkey match { case Some(x) => x; case _ => 0 }))
          }).toSeq
        Recorda6f680fe6b904c2e8677a0111c3e695b(key.o_shippriority, key.c_acctbal, key.n_regionkey, key.n_nationkey, key.o_orderdate, key.o_custkey, key.r_regionkey, key.o_orderpriority, key.Order_index, key.r_comment, key.c_name, grp, key.Customer_index, key.c_nationkey, key.n_name, key.o_clerk, key.o_orderstatus, key.r_name, key.c_custkey, key.c_comment, key.Nation_index, key.c_address, key.c_mktsegment, key.n_comment, key.o_totalprice, key.o_orderkey, key.c_phone, key.Region_index, key.o_comment)
    }.as[Recorda6f680fe6b904c2e8677a0111c3e695b]

    val x540 = x538.select("o_shippriority", "c_acctbal", "n_regionkey", "n_nationkey", "o_orderdate", "o_custkey", "r_regionkey", "o_orderpriority", "r_comment", "c_name", "o_parts", "Customer_index", "c_nationkey", "n_name", "o_clerk", "o_orderstatus", "r_name", "c_custkey", "c_comment", "Nation_index", "c_address", "c_mktsegment", "n_comment", "o_totalprice", "o_orderkey", "c_phone", "Region_index", "o_comment")

      .as[Recordb6a030928f084dd3929bef03382ea540]

    val x542 = x540.groupByKey(x541 => Recorda351c3c5be9b4e5c8a84a7ab4686f842(x541.c_acctbal, x541.n_regionkey, x541.n_nationkey, x541.r_regionkey, x541.r_comment, x541.c_name, x541.Customer_index, x541.c_nationkey, x541.n_name, x541.r_name, x541.c_custkey, x541.c_comment, x541.Nation_index, x541.c_address, x541.c_mktsegment, x541.n_comment, x541.c_phone, x541.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x541 =>
          (x541.o_shippriority, x541.o_orderpriority, x541.o_totalprice, x541.o_orderdate, x541.o_comment, x541.o_orderkey, x541.o_orderstatus, x541.o_custkey, x541.o_clerk) match {
            case (None, _, _, _, _, _, _, _, _) => Seq()
            case (_, None, _, _, _, _, _, _, _) => Seq()
            case (_, _, None, _, _, _, _, _, _) => Seq()
            case (_, _, _, None, _, _, _, _, _) => Seq()
            case (_, _, _, _, None, _, _, _, _) => Seq()
            case (_, _, _, _, _, None, _, _, _) => Seq()
            case (_, _, _, _, _, _, None, _, _) => Seq()
            case (_, _, _, _, _, _, _, None, _) => Seq()
            case (_, _, _, _, _, _, _, _, None) => Seq()
            case _ => Seq(Record2962daaf88e040f2b372be1cc6d7bf4f(x541.o_shippriority match { case Some(x) => x; case _ => 0 }, x541.o_orderdate match { case Some(x) => x; case _ => "null" }, x541.o_custkey match { case Some(x) => x; case _ => 0 }, x541.o_orderpriority match { case Some(x) => x; case _ => "null" }, x541.o_parts, x541.o_clerk match { case Some(x) => x; case _ => "null" }, x541.o_orderstatus match { case Some(x) => x; case _ => "null" }, x541.o_totalprice match { case Some(x) => x; case _ => 0.0 }, x541.o_orderkey match { case Some(x) => x; case _ => 0 }, x541.o_comment match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Recorda4ba42a7be284fba8f7439edc2f6e72d(key.c_acctbal, key.n_regionkey, key.n_nationkey, key.r_regionkey, key.r_comment, key.c_name, key.Customer_index, key.c_nationkey, key.n_name, key.r_name, key.c_custkey, key.c_comment, key.Nation_index, key.c_address, grp, key.c_mktsegment, key.n_comment, key.c_phone, key.Region_index)
    }.as[Recorda4ba42a7be284fba8f7439edc2f6e72d]

    val x544 = x542.select("c_acctbal", "n_regionkey", "n_nationkey", "r_regionkey", "r_comment", "c_name", "c_nationkey", "n_name", "r_name", "c_custkey", "c_comment", "Nation_index", "c_address", "c_orders", "c_mktsegment", "n_comment", "c_phone", "Region_index")

      .as[Record258b0f4e352a4c56a6677d4ebb58a3b7]

    val x546 = x544.groupByKey(x545 => Record65d7fd2be79a4dc78040ba2054a8bba8(x545.n_regionkey, x545.n_nationkey, x545.r_regionkey, x545.r_comment, x545.n_name, x545.r_name, x545.Nation_index, x545.n_comment, x545.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x545 =>
          (x545.c_name, x545.c_phone, x545.c_acctbal, x545.c_comment, x545.c_nationkey, x545.c_address, x545.c_mktsegment, x545.c_custkey) match {
            case (None, _, _, _, _, _, _, _) => Seq()
            case (_, None, _, _, _, _, _, _) => Seq()
            case (_, _, None, _, _, _, _, _) => Seq()
            case (_, _, _, None, _, _, _, _) => Seq()
            case (_, _, _, _, None, _, _, _) => Seq()
            case (_, _, _, _, _, None, _, _) => Seq()
            case (_, _, _, _, _, _, None, _) => Seq()
            case (_, _, _, _, _, _, _, None) => Seq()
            case _ => Seq(Record3ab78fdd34e74e558a3c213d4f6961a9(x545.c_acctbal match { case Some(x) => x; case _ => 0.0 }, x545.c_name match { case Some(x) => x; case _ => "null" }, x545.c_nationkey match { case Some(x) => x; case _ => 0 }, x545.c_custkey match { case Some(x) => x; case _ => 0 }, x545.c_comment match { case Some(x) => x; case _ => "null" }, x545.c_address match { case Some(x) => x; case _ => "null" }, x545.c_orders, x545.c_mktsegment match { case Some(x) => x; case _ => "null" }, x545.c_phone match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Record7932ff30025e4652ac24e394ca5b2909(key.n_regionkey, key.n_nationkey, grp, key.r_regionkey, key.r_comment, key.n_name, key.r_name, key.Nation_index, key.n_comment, key.Region_index)
    }.as[Record7932ff30025e4652ac24e394ca5b2909]

    val x548 = x546.select("n_regionkey", "n_nationkey", "n_custs", "r_regionkey", "r_comment", "n_name", "r_name", "n_comment", "Region_index")

      .as[Record7e8214f68f8946f58d17c7d6c24b2c24]

    val x550 = x548.groupByKey(x549 => Record8330f1af0c474f1ba9ea76d0dcb0e244(x549.r_regionkey, x549.r_name, x549.r_comment, x549.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x549 =>
          (x549.n_regionkey, x549.n_nationkey, x549.n_name, x549.n_comment) match {
            case (None, _, _, _) => Seq()
            case (_, None, _, _) => Seq()
            case (_, _, None, _) => Seq()
            case (_, _, _, None) => Seq()
            case _ => Seq(Record8056172166ef4ed0a5291d5a02c1d45e(x549.n_regionkey match { case Some(x) => x; case _ => 0 }, x549.n_nationkey match { case Some(x) => x; case _ => 0 }, x549.n_custs, x549.n_name match { case Some(x) => x; case _ => "null" }, x549.n_comment match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Recordfca24ee05bbb47eb85416ab250ca50cb(key.r_regionkey, key.r_comment, grp, key.r_name, key.Region_index)
    }.as[Recordfca24ee05bbb47eb85416ab250ca50cb]

    x550
  }
  def Test4FullFlat() = {
    val x426 = OrderDF


    val x427 = x426.withColumn("Order_index", monotonically_increasing_id())
      .as[Recorddea2915488de48ea950609a53b06e408]

    val x429 = LineItemDF.as[Lineitem]


    val x432 = x427.equiJoin(x429,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record123cb5cafa7e4f64959a801954af523c]

    val x434 = x432


    val x436 = x434.groupByKey(x435 => Recorddea2915488de48ea950609a53b06e408(x435.o_shippriority, x435.o_orderdate, x435.o_custkey, x435.o_orderpriority, x435.Order_index, x435.o_clerk, x435.o_orderstatus, x435.o_totalprice, x435.o_orderkey, x435.o_comment)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x435 =>
          (x435.l_partkey, x435.l_extendedprice, x435.l_shipmode, x435.l_shipdate, x435.l_receiptdate, x435.l_commitdate, x435.l_shipinstruct, x435.l_quantity, x435.l_returnflag, x435.l_discount, x435.l_tax, x435.l_linestatus, x435.l_comment, x435.l_suppkey, x435.l_linenumber, x435.l_orderkey) match {
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
            case _ => Seq(Recordfc87dbce9ec945259de59e86516321df(x435.l_returnflag match { case Some(x) => x; case _ => "null" }, x435.l_comment match { case Some(x) => x; case _ => "null" }, x435.l_linestatus match { case Some(x) => x; case _ => "null" }, x435.l_shipmode match { case Some(x) => x; case _ => "null" }, x435.l_shipinstruct match { case Some(x) => x; case _ => "null" }, x435.l_quantity match { case Some(x) => x; case _ => 0.0 }, x435.l_receiptdate match { case Some(x) => x; case _ => "null" }, x435.l_linenumber match { case Some(x) => x; case _ => 0 }, x435.l_tax match { case Some(x) => x; case _ => 0.0 }, x435.l_shipdate match { case Some(x) => x; case _ => "null" }, x435.l_extendedprice match { case Some(x) => x; case _ => 0.0 }, x435.l_partkey match { case Some(x) => x; case _ => 0 }, x435.l_discount match { case Some(x) => x; case _ => 0.0 }, x435.l_commitdate match { case Some(x) => x; case _ => "null" }, x435.l_suppkey match { case Some(x) => x; case _ => 0 }, x435.l_orderkey match { case Some(x) => x; case _ => 0 }))
          }).toSeq
        Record25b2096f4fef4c6db624e56e4605ad56(key.o_shippriority, key.o_orderdate, key.o_custkey, key.o_orderpriority, key.Order_index, grp, key.o_clerk, key.o_orderstatus, key.o_totalprice, key.o_orderkey, key.o_comment)
    }.as[Record25b2096f4fef4c6db624e56e4605ad56]

    val x437 = x436
    val orders = x437
    //orders.cache
    //orders.count
    val x439 = CustomerDF


    val x440 = x439.withColumn("Customer_index", monotonically_increasing_id())
      .as[Record805e78cb27f14188b73b5a70b9bacdb1]

    val x442 = orders


    val x445 = x440.equiJoin(x442,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record257736a7ebca4ee0bee0bc001a417399]

    val x447 = x445


    val x449 = x447.groupByKey(x448 => Record805e78cb27f14188b73b5a70b9bacdb1(x448.c_acctbal, x448.c_name, x448.Customer_index, x448.c_nationkey, x448.c_custkey, x448.c_comment, x448.c_address, x448.c_mktsegment, x448.c_phone)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x448 =>
          (x448.o_parts, x448.o_custkey, x448.o_orderkey, x448.o_orderstatus, x448.o_orderdate, x448.o_comment, x448.o_orderpriority, x448.o_shippriority, x448.o_clerk, x448.o_totalprice) match {
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
            case _ => Seq(Record3d10466ca2564b28badf97b9ee1690f4(x448.o_shippriority match { case Some(x) => x; case _ => 0 }, x448.o_orderdate match { case Some(x) => x; case _ => "null" }, x448.o_custkey match { case Some(x) => x; case _ => 0 }, x448.o_orderpriority match { case Some(x) => x; case _ => "null" }, x448.o_parts match { case Some(x) => x; case _ => null }, x448.o_clerk match { case Some(x) => x; case _ => "null" }, x448.o_orderstatus match { case Some(x) => x; case _ => "null" }, x448.o_totalprice match { case Some(x) => x; case _ => 0.0 }, x448.o_orderkey match { case Some(x) => x; case _ => 0 }, x448.o_comment match { case Some(x) => x; case _ => "null" }))
          }).toSeq
        Recordc290b62937e04f33a451c67c1fe54572(key.c_acctbal, key.c_name, key.Customer_index, key.c_nationkey, key.c_custkey, key.c_comment, key.c_address, grp, key.c_mktsegment, key.c_phone)
    }.as[Recordc290b62937e04f33a451c67c1fe54572]

    x449
  }
def Test0Join() = {
  val x0 = LineItemDF.as[Lineitem]


  val x11 = x0.equiJoin(PartDF.as[Part],
    Seq("l_partkey"), Seq("p_partkey"), "inner")

  val x13 = x11.select("p_name", "l_quantity")
    .withColumnRenamed("l_quantity", "l_qty")
    .withColumn("l_qty", when(col("l_qty").isNull, 0.0).otherwise(col("l_qty")))
    .as[Recordfb4c08eaba4d4834be190e9e40707767]

  x13
}

  def Test1Join() = {
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

    x32
  }

  def Test1JoinFlat() = {
    val x22 = LineItemDF.as[Lineitem].equiJoin(PartDF.as[Part],
      Seq("l_partkey"), Seq("p_partkey"), "inner").as[Record606561f1b8d4468fbcd84b4e1ba669a9]

    val x24 = x22.select("l_orderkey", "p_name", "l_quantity")
      .withColumnRenamed("l_quantity", "l_qty")
      .withColumn("l_qty", when(col("l_qty").isNull, 0.0).otherwise(col("l_qty")))
      .as[Record23bf6274c4ba4812b9decfd93d00e2b0]

    val x25 = x24
    val parts = x25
    //parts.cache
    //parts.count
    val x27 = OrderDF.select("o_orderdate", "o_orderkey")

      .as[Recordd8ed41e33adc4e48ab641f18b056c0de]

    val x28 = x27.withColumn("Order_index", monotonically_increasing_id())
      .as[Record792c91ca99ea49d3b912f9d8bf02e125]

    val x30 = parts


    val x33 = x28.equiJoin(x30,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record3d4af23308684fffa256fccd3ab6bc82]

    val x35 = x33.select("Order_index", "o_orderdate", "p_name", "l_qty")

      .as[Record61c40a136b674c8891affe4be0de8331]

    val x37 = x35.groupByKey(x36 => Record96f88ae133ff44ed82f3e3664fcc6118(x36.o_orderdate, x36.Order_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x36 =>
          (x36.p_name, x36.l_qty) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recordb283e22c356c4fac93dadc6cb15db4a4(x36.p_name match { case Some(x) => x; case _ => "null" }, x36.l_qty match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Recorddaaae85320d3458f87e78ccd848a9117(key.Order_index, key.o_orderdate, grp)
    }.as[Recorddaaae85320d3458f87e78ccd848a9117]

    x37
  }
  def Test2Join() = {
    val x616 = CustomerDF.select("c_name", "c_custkey")

      .as[Record4f8b23616b1f46e5a4f686cad05beb9b]

    val x617 = x616.withColumn("Customer_index", monotonically_increasing_id())
      .as[Recordea10a658f3d248dc9805830bb50f0a02]

    val x619 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Record0e2b8d829f484b9ca473ca4dda85738e]

    val x620 = x619.withColumn("Order_index", monotonically_increasing_id())
      .as[Record06d899db93904e5bac94aa537cbc4dbd]

    val x623 = x617.equiJoin(x620,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Recordffa78d1b22874196ac848313fc38c25e]

    val x625 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

      .as[Record168d1b04d2be4237a7c213f9f542135a]

    val x628 = x623.equiJoin(x625,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record574f2f0a88354eddaaa48d4b461fc2ef]

    val x630 = PartDF.select("p_name", "p_partkey")

      .as[Recordf5c38ca2f3f24d27950c22bc2147d8ff]

    val x633 = x628.equiJoin(x630,
      Seq("l_partkey"), Seq("p_partkey"), "left_outer").as[Record1da5f174d2da49089bf409d6ab998711]

    val x635 = x633.select("p_name", "o_orderdate", "l_quantity", "Order_index", "c_name", "Customer_index")

      .as[Record5b2109abcfc041b4964a62f40abe4516]

    val x637 = x635.groupByKey(x636 => Record1db84829c64b44e09a2e711fad949cc9(x636.o_orderdate, x636.Order_index, x636.c_name, x636.Customer_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x636 =>
          (x636.p_name, x636.l_quantity) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recorde1b10844d10e4fb3a2aa57cbbfaaff36(x636.p_name match { case Some(x) => x; case _ => "null" }, x636.l_quantity match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Record19c4e01f65d9451caf63b67cbb39f9e2(key.o_orderdate, key.Order_index, key.c_name, grp, key.Customer_index)
    }.as[Record19c4e01f65d9451caf63b67cbb39f9e2]

    val x639 = x637.select("Customer_index", "c_name", "o_orderdate", "o_parts")

      .as[Record3a6aaef9efe048b9b4c032c6c6c590bd]

    val x641 = x639.groupByKey(x640 => Record67b3b09f957b4a5babf99aaeacb18ccc(x640.c_name, x640.Customer_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x640 =>
          (x640.o_orderdate) match {
            case (None) => Seq()
            case _ => Seq(Record3c513c247b0748c493464d03dcc28b0e(x640.o_orderdate match { case Some(x) => x; case _ => "null" }, x640.o_parts))
          }).toSeq
        Record93a31d9caa7a48709e17507d57f200a6(key.Customer_index, key.c_name, grp)
    }.as[Record93a31d9caa7a48709e17507d57f200a6]

    x641
  }
  def Test2JoinFlat() = {
    val x675 = LineItemDF.as[Lineitem].equiJoin(PartDF.as[Part],
      Seq("l_partkey"), Seq("p_partkey"), "inner").as[Recordbd97c42dea9246edb014230f83c086b2]

    val x677 = x675.select("l_orderkey", "p_name", "l_quantity")
      .withColumnRenamed("l_quantity", "l_qty")
      .withColumn("l_qty", when(col("l_qty").isNull, 0.0).otherwise(col("l_qty")))
      .as[Record562ce1565c9f4a4c82644bcd77250358]

    val x678 = x677
    val parts = x678
    //parts.cache
    //parts.count
    val x680 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Record8954b5214bb14a71a9b009d19f6e5b54]

    val x681 = x680.withColumn("Order_index", monotonically_increasing_id())
      .as[Record746699b906b74d89a5335c6f8ced0048]

    val x683 = parts


    val x686 = x681.equiJoin(x683,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record212013d4a11c497489fba433f9d6e9c0]

    val x688 = x686.select("p_name", "l_qty", "o_orderdate", "o_custkey", "Order_index")

      .as[Recordb191b13119b64a61b4ec1a57a15f728d]

    val x690 = x688.groupByKey(x689 => Record4935426c140543b5ba979df2fc2ef07c(x689.o_orderdate, x689.o_custkey, x689.Order_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x689 =>
          (x689.p_name, x689.l_qty) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recordac04bda5238240f1996455e866d40224(x689.p_name match { case Some(x) => x; case _ => "null" }, x689.l_qty match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Record8a728146f7ac456c9a79fa8aa1b85e15(key.o_orderdate, key.o_custkey, key.Order_index, grp)
    }.as[Record8a728146f7ac456c9a79fa8aa1b85e15]

    val x691 = x690
    val orders = x691
    //orders.cache
    //orders.count
    val x693 = CustomerDF.select("c_name", "c_custkey")

      .as[Recordcf5b1216de1842fbb14db711793e572e]

    val x694 = x693.withColumn("Customer_index", monotonically_increasing_id())
      .as[Record4a2077d910c840e09d2fde62a5e21dd0]

    val x696 = orders


    val x699 = x694.equiJoin(x696,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Recorda2d1b80a28f542d7b6f84a87c8a475ba]

    val x701 = x699.select("c_name", "Customer_index", "o_orderdate", "o_parts")

      .as[Record557e18d246694c08a368ce9f7d52c9a4]

    val x703 = x701.groupByKey(x702 => Recordf57a1152f9a64b958b394659d311f384(x702.c_name, x702.Customer_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x702 =>
          (x702.o_orderdate, x702.o_parts) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record52354f6e2cb0478bb7e9829d6166f83a(x702.o_orderdate match { case Some(x) => x; case _ => "null" }, x702.o_parts match { case Some(x) => x; case _ => null }))
          }).toSeq
        Record3f24f239146749158ed14e10a737fb0d(key.c_name, key.Customer_index, grp)
    }.as[Record3f24f239146749158ed14e10a737fb0d]

    x703
  }
def Test3Join() = {
  val x931 = NationDF.select("n_nationkey", "n_name")

    .as[Record68d0b943329f443d87d9e3f0daa18f66]

  val x932 = x931.withColumn("Nation_index", monotonically_increasing_id())
    .as[Recordffce9b9666b74aa2b0f87198b086f8fc]

  val x934 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

    .as[Record5e42eb17cfb74a7a9f4d6cb3f0e9e4b8]

  val x935 = x934.withColumn("Customer_index", monotonically_increasing_id())
    .as[Record4e70bfb13c4a43739b051a0543ba8364]

  val x938 = x932.equiJoin(x935,
    Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Record16251308cb0947258f0cc4940d15a2f8]

  val x940 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

    .as[Record98d5a3ec385446c6b70caf72b73da4fe]

  val x941 = x940.withColumn("Order_index", monotonically_increasing_id())
    .as[Record8b0b9492e3b64da4976a35d23b96ffd1]

  val x944 = x938.equiJoin(x941,
    Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record2222f88a84ba4a0abf188742b994dccb]

  val x946 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

    .as[Recordacded01bf3e44b04bf9b6fdab2065c3e]

  val x949 = x944.equiJoin(x946,
    Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record5de0dea501aa4069b19f51e6e9fd1502]

  val x951 = PartDF.select("p_name", "p_partkey")

    .as[Record1ced386503f54dde8ff2c0e503595a9d]

  val x954 = x949.equiJoin(x951,
    Seq("l_partkey"), Seq("p_partkey"), "left_outer").as[Record009bbf7e86f94310a794bbe3d924b473]

  val x956 = x954.select("p_name", "o_orderdate", "l_quantity", "Order_index", "c_name", "Customer_index", "n_name", "Nation_index")

    .as[Record9fca75095dd64686a24a061af5e0ee31]

  val x958 = x956.groupByKey(x957 => Record4dd5e479a7504d7d8fd8441e7cd0ec94(x957.o_orderdate, x957.Order_index, x957.c_name, x957.Customer_index, x957.n_name, x957.Nation_index)).mapGroups {
    case (key, value) =>
      val grp = value.flatMap(x957 =>
        (x957.p_name, x957.l_quantity) match {
          case (None, _) => Seq()
          case (_, None) => Seq()
          case _ => Seq(Record2a1daa78aef142a5acd6398ebaa97d9e(x957.p_name match { case Some(x) => x; case _ => "null" }, x957.l_quantity match { case Some(x) => x; case _ => 0.0 }))
        }).toSeq
      Recordd7ceffee59d642fab445cb0df4c93668(key.o_orderdate, key.Order_index, key.c_name, grp, key.Customer_index, key.n_name, key.Nation_index)
  }.as[Recordd7ceffee59d642fab445cb0df4c93668]

  val x960 = x958.select("o_orderdate", "c_name", "o_parts", "Customer_index", "n_name", "Nation_index")

    .as[Record7d6dc30f279148728e51dee4ba747253]

  val x962 = x960.groupByKey(x961 => Record759f25319b6e4037bf64d6bb36be2934(x961.c_name, x961.Customer_index, x961.n_name, x961.Nation_index)).mapGroups {
    case (key, value) =>
      val grp = value.flatMap(x961 =>
        (x961.o_orderdate) match {
          case (None) => Seq()
          case _ => Seq(Record1f10ab2e7d874a5292398c240e0ce373(x961.o_orderdate match { case Some(x) => x; case _ => "null" }, x961.o_parts))
        }).toSeq
      Record04db13982d9d4cd7bae09fcf7b222fb5(key.c_name, key.Customer_index, key.n_name, key.Nation_index, grp)
  }.as[Record04db13982d9d4cd7bae09fcf7b222fb5]

  val x964 = x962.select("n_name", "Nation_index", "c_name", "c_orders")

    .as[Record65fdb89bb5df4efa931faf5f77ba0b5f]

  val x966 = x964.groupByKey(x965 => Record7792966799e045aa874cf41897f1963c(x965.n_name, x965.Nation_index)).mapGroups {
    case (key, value) =>
      val grp = value.flatMap(x965 =>
        (x965.c_name) match {
          case (None) => Seq()
          case _ => Seq(Record781e3409d40a4403b9f95e6a8cf122e5(x965.c_name match { case Some(x) => x; case _ => "null" }, x965.c_orders))
        }).toSeq
      Recordee6734a99b10470ba48df04110a625f5(key.n_name, key.Nation_index, grp)
  }.as[Recordee6734a99b10470ba48df04110a625f5]

  x966
}
  def Test3JoinFlat() = {
    val x748 = LineItemDF.as[Lineitem].equiJoin(PartDF.as[Part],
      Seq("l_partkey"), Seq("p_partkey"), "inner").as[Recordbe345c7d5c524ffd8b4497b37c062a23]

    val x750 = x748.select("l_orderkey", "p_name", "l_quantity")
      .withColumnRenamed("l_quantity", "l_qty")
      .withColumn("l_qty", when(col("l_qty").isNull, 0.0).otherwise(col("l_qty")))
      .as[Recorde05b95cb485648b6bf372e7445062f21]

    val x751 = x750
    val parts = x751
    //parts.cache
    //parts.count
    val x753 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Record3c583f03e4054885a163bac88cb503f7]

    val x754 = x753.withColumn("Order_index", monotonically_increasing_id())
      .as[Recordfff89fe1d66c4e2b97d9359358f4ef26]

    val x756 = parts


    val x759 = x754.equiJoin(x756,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record9022b298683c46d48f0ff34333eb186f]

    val x761 = x759.select("p_name", "l_qty", "o_orderdate", "o_custkey", "Order_index")

      .as[Record954f4395737c4a39a1fa9837bca512eb]

    val x763 = x761.groupByKey(x762 => Recordee5475e5587949d790d77b3aa424d40a(x762.o_orderdate, x762.o_custkey, x762.Order_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x762 =>
          (x762.p_name, x762.l_qty) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record58e9f90104c74682a99f63efc29d4303(x762.p_name match { case Some(x) => x; case _ => "null" }, x762.l_qty match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Record9dcd833fc46e489fa0bfedd3db6727a8(key.o_orderdate, key.o_custkey, key.Order_index, grp)
    }.as[Record9dcd833fc46e489fa0bfedd3db6727a8]

    val x764 = x763
    val orders = x764
    //orders.cache
    //orders.count
    val x766 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

      .as[Record20a58b370dd24b4b91900045197318fb]

    val x767 = x766.withColumn("Customer_index", monotonically_increasing_id())
      .as[Record09369657861e4522b716012c4e57d77b]

    val x769 = orders


    val x772 = x767.equiJoin(x769,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Recordb2cbc58d49a847579a8746fe23e99736]

    val x774 = x772.select("o_orderdate", "c_name", "o_parts", "Customer_index", "c_nationkey")

      .as[Record1e3520b324f5404ebbaf5b2188486b35]

    val x776 = x774.groupByKey(x775 => Record1a6ee244f1374e05ac54ffda67a4fe04(x775.c_name, x775.Customer_index, x775.c_nationkey)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x775 =>
          (x775.o_orderdate, x775.o_parts) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record99964c78aa06433d8aefd5d20ebafca7(x775.o_orderdate match { case Some(x) => x; case _ => "null" }, x775.o_parts match { case Some(x) => x; case _ => null }))
          }).toSeq
        Record79476de885144a1ea5a17ee737b9fdd9(key.c_name, key.Customer_index, key.c_nationkey, grp)
    }.as[Record79476de885144a1ea5a17ee737b9fdd9]

    val x777 = x776
    val customers = x777
    //customers.cache
    //customers.count
    val x779 = NationDF.select("n_nationkey", "n_name")

      .as[Record9c7264b3637140539f50ae7dc658cc33]

    val x780 = x779.withColumn("Nation_index", monotonically_increasing_id())
      .as[Recordaa321b8d330d4510934e67bf600eeb1d]

    val x782 = customers


    val x785 = x780.equiJoin(x782,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Record28b4ff1bc82a4ba6ab627d78d2aac9cb]

    val x787 = x785.select("Nation_index", "n_name", "c_name", "c_orders")

      .as[Recordeca21cee38644ea9baa5ed91d08b06ce]

    val x789 = x787.groupByKey(x788 => Record86d856dd6ca145e8a1a7b3ae22d777b4(x788.n_name, x788.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x788 =>
          (x788.c_name, x788.c_orders) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recordff34764f4e854cfdabec84aee9f99176(x788.c_name match { case Some(x) => x; case _ => "null" }, x788.c_orders match { case Some(x) => x; case _ => null }))
          }).toSeq
        Record397f991ae85d4d7ba86f25de5d6374a6(key.Nation_index, key.n_name, grp)
    }.as[Record397f991ae85d4d7ba86f25de5d6374a6]

    x789
  }
  def Test4Join() = {
    val x1001 = RegionDF.select("r_regionkey", "r_name")

      .as[Recorde818a7c4f450485595a75795e0bd0167]

    val x1002 = x1001.withColumn("Region_index", monotonically_increasing_id())
      .as[Record43ceb76d411a48e0b64c5bd5f6c96ebc]

    val x1004 = NationDF.select("n_nationkey", "n_name", "n_regionkey")

      .as[Record16573a99a6c54d4fbf7395cb16b216db]

    val x1005 = x1004.withColumn("Nation_index", monotonically_increasing_id())
      .as[Recordd535cf884b744d0a8aa46275f98a9430]

    val x1008 = x1002.equiJoin(x1005,
      Seq("r_regionkey"), Seq("n_regionkey"), "left_outer").as[Record262432c1986f4570bf7b9f5125523224]

    val x1010 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

      .as[Record4424e5b6bdab4ba9bb5ed68da5ae2502]

    val x1011 = x1010.withColumn("Customer_index", monotonically_increasing_id())
      .as[Record0f3ccfa4a03c492fad4ae1a6e012c9e7]

    val x1014 = x1008.equiJoin(x1011,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Record10abb3bce7ec45718878b53e8e2f96db]

    val x1016 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Recordf3389feba1bc4eed99c5a737eb634fed]

    val x1017 = x1016.withColumn("Order_index", monotonically_increasing_id())
      .as[Record96e1330372bb4790a11860b7c4f2643b]

    val x1020 = x1014.equiJoin(x1017,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record931e27bc18314bdeb8f4e2dcbb022f73]

    val x1022 = LineItemDF.select("l_quantity", "l_partkey", "l_orderkey")

      .as[Record8af745688b904aadacff197f21aed496]

    val x1025 = x1020.equiJoin(x1022,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record894dbf206e464ed6a314283a737f3e58]

    val x1027 = x1025.select("o_orderdate", "l_quantity", "Order_index", "c_name", "Customer_index", "n_name", "l_partkey", "r_name", "Nation_index", "Region_index")

      .as[Recordfc383ae46e9a4fd086c6a7745ff54126]

    val x1029 = x1027.groupByKey(x1028 => Record2072177b36b94d56847c15f28ef501ac(x1028.o_orderdate, x1028.Order_index, x1028.c_name, x1028.Customer_index, x1028.n_name, x1028.r_name, x1028.Nation_index, x1028.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x1028 =>
          (x1028.l_quantity, x1028.l_partkey) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record071610c4592649cab3e5f45e01cba4b3(x1028.l_partkey match { case Some(x) => x; case _ => 0 }, x1028.l_quantity match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Recordfa7edcb6da3c443fb97e9128c650df36(key.o_orderdate, key.Order_index, key.c_name, grp, key.Customer_index, key.n_name, key.r_name, key.Nation_index, key.Region_index)
    }.as[Recordfa7edcb6da3c443fb97e9128c650df36]

    val x1031 = x1029.select("o_orderdate", "c_name", "o_parts", "Customer_index", "n_name", "r_name", "Nation_index", "Region_index")

      .as[Record7fe6e1039dcb48b28590e86be4f426be]

    val x1033 = x1031.groupByKey(x1032 => Record4dcb6094997b406fb067cc4dc257aecd(x1032.c_name, x1032.Customer_index, x1032.n_name, x1032.r_name, x1032.Nation_index, x1032.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x1032 =>
          (x1032.o_orderdate) match {
            case (None) => Seq()
            case _ => Seq(Record1228f03ebacc49548fb888e27b906134(x1032.o_orderdate match { case Some(x) => x; case _ => "null" }, x1032.o_parts))
          }).toSeq
        Record15b07ceabe494f17a4ccaa425dfee87a(key.c_name, key.Customer_index, key.n_name, key.r_name, key.Nation_index, grp, key.Region_index)
    }.as[Record15b07ceabe494f17a4ccaa425dfee87a]

    val x1035 = x1033.select("c_name", "n_name", "r_name", "Nation_index", "c_orders", "Region_index")

      .as[Recordaa74566e4f754fe487060470dc768f90]

    val x1037 = x1035.groupByKey(x1036 => Record8e820add41c64daf87c9c0fdf8056e86(x1036.n_name, x1036.r_name, x1036.Nation_index, x1036.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x1036 =>
          (x1036.c_name) match {
            case (None) => Seq()
            case _ => Seq(Record4d286c9a2cba48b68df1df98bf130f9b(x1036.c_name match { case Some(x) => x; case _ => "null" }, x1036.c_orders))
          }).toSeq
        Record84ed2d22c2e04cc884004f5d36d4fcce(grp, key.n_name, key.r_name, key.Nation_index, key.Region_index)
    }.as[Record84ed2d22c2e04cc884004f5d36d4fcce]

    val x1039 = x1037.select("r_name", "Region_index", "n_name", "n_custs")

      .as[Record40d102db1f3342ff93d32e0e56bcfa94]

    val x1041 = x1039.groupByKey(x1040 => Recordbae95aa4510c496aa53b95a59f590194(x1040.r_name, x1040.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x1040 =>
          (x1040.n_name) match {
            case (None) => Seq()
            case _ => Seq(Record4fba3c8f3427497cb6390343c7afc361(x1040.n_name match { case Some(x) => x; case _ => "null" }, x1040.n_custs))
          }).toSeq
        Record1dcbc5ff70ea462484724d62c084128e(key.r_name, key.Region_index, grp)
    }.as[Record1dcbc5ff70ea462484724d62c084128e]

    x1041
  }
  def Test4JoinFlat() = {
    val x845 = LineItemDF.as[Lineitem].equiJoin(PartDF.as[Part],
      Seq("l_partkey"), Seq("p_partkey"), "inner").as[Record8e9b91d0af5847daab29add77882747e]

    val x847 = x845.select("l_orderkey", "p_name", "l_quantity")
      .withColumnRenamed("l_quantity", "l_qty")
      .withColumn("l_qty", when(col("l_qty").isNull, 0.0).otherwise(col("l_qty")))
      .as[Record6651ac742958450696d2ae533c10c55e]

    val x848 = x847
    val parts = x848
    //parts.cache
    //parts.count
    val x850 = OrderDF.select("o_orderdate", "o_custkey", "o_orderkey")

      .as[Record3a3a3c28b23a4881b6d794e5efa589f6]

    val x851 = x850.withColumn("Order_index", monotonically_increasing_id())
      .as[Recordf49e80f711cc46bc862a8690dc0260fe]

    val x853 = parts


    val x856 = x851.equiJoin(x853,
      Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record35365b87f79e4bd597b2fed9b1a25cb7]

    val x858 = x856.select("p_name", "l_qty", "o_orderdate", "o_custkey", "Order_index")

      .as[Recordaa7f2efe99214d44bcb7a1ee5738169c]

    val x860 = x858.groupByKey(x859 => Recordeb057673c0a740b98ee111768a6cec91(x859.o_orderdate, x859.o_custkey, x859.Order_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x859 =>
          (x859.p_name, x859.l_qty) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recordde8e5ce6c9f64fde940441ccbb88a906(x859.p_name match { case Some(x) => x; case _ => "null" }, x859.l_qty match { case Some(x) => x; case _ => 0.0 }))
          }).toSeq
        Record2bab4dc435a843e1ad1d915cd2f855ac(key.o_orderdate, key.o_custkey, key.Order_index, grp)
    }.as[Record2bab4dc435a843e1ad1d915cd2f855ac]

    val x861 = x860
    val orders = x861
    //orders.cache
    //orders.count
    val x863 = CustomerDF.select("c_name", "c_nationkey", "c_custkey")

      .as[Record52c5c939a7eb4d58a3b97e551ee0b482]

    val x864 = x863.withColumn("Customer_index", monotonically_increasing_id())
      .as[Recorddc2c021433304b3baf03bcbb8908436a]

    val x866 = orders


    val x869 = x864.equiJoin(x866,
      Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record771f711f476a49be955044f549499575]

    val x871 = x869.select("o_orderdate", "c_name", "o_parts", "Customer_index", "c_nationkey")

      .as[Record53fde9fed2da4644bd06eedc9d38d686]

    val x873 = x871.groupByKey(x872 => Recordb886459106394e9c9af5aab2f3c3ad63(x872.c_name, x872.Customer_index, x872.c_nationkey)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x872 =>
          (x872.o_orderdate, x872.o_parts) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recordea64327ddecd4e1297f007adc212c1c5(x872.o_orderdate match { case Some(x) => x; case _ => "null" }, x872.o_parts match { case Some(x) => x; case _ => null }))
          }).toSeq
        Record87f36beccaed49e48e5efe4e113f1472(key.c_name, key.Customer_index, key.c_nationkey, grp)
    }.as[Record87f36beccaed49e48e5efe4e113f1472]

    val x874 = x873
    val customers = x874
    //customers.cache
    //customers.count
    val x876 = NationDF.select("n_nationkey", "n_name", "n_regionkey")

      .as[Recorddc6e9f69dca343279e3a0983b6be8b48]

    val x877 = x876.withColumn("Nation_index", monotonically_increasing_id())
      .as[Recorda79be1bc175c47eb97480d20565751dc]

    val x879 = customers


    val x882 = x877.equiJoin(x879,
      Seq("n_nationkey"), Seq("c_nationkey"), "left_outer").as[Record450926b6309e4e30bc1173bb08c6e220]

    val x884 = x882.select("n_regionkey", "c_name", "n_name", "Nation_index", "c_orders")

      .as[Record7837dab65e6545ddb7d5705dfe237a22]

    val x886 = x884.groupByKey(x885 => Record3cafedd27d4e49cba3d743fab246d2ed(x885.n_regionkey, x885.n_name, x885.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x885 =>
          (x885.c_name, x885.c_orders) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record23b59c86e6744cabab6e0b3bd30ae892(x885.c_name match { case Some(x) => x; case _ => "null" }, x885.c_orders match { case Some(x) => x; case _ => null }))
          }).toSeq
        Record201c759f7f7c4db9ad550e8753f62e74(key.n_regionkey, grp, key.n_name, key.Nation_index)
    }.as[Record201c759f7f7c4db9ad550e8753f62e74]

    val x887 = x886
    val nations = x887
    //nations.cache
    //nations.count
    val x889 = RegionDF.select("r_regionkey", "r_name")

      .as[Recordd3c3971533734283b67b1f37094f6073]

    val x890 = x889.withColumn("Region_index", monotonically_increasing_id())
      .as[Recordf889b51a39884db4971a1e86893ac2d0]

    val x892 = nations


    val x895 = x890.equiJoin(x892,
      Seq("r_regionkey"), Seq("n_regionkey"), "left_outer").as[Record7b2cac2c99f641b9bf3ddcdc09605463]

    val x897 = x895.select("r_name", "Region_index", "n_name", "n_custs")

      .as[Record154718e31fd3422e9bd81e669deaf9f8]

    val x899 = x897.groupByKey(x898 => Record7e8a062160334c4db7593ed7639b91b8(x898.r_name, x898.Region_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x898 =>
          (x898.n_name, x898.n_custs) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record06d471a3ea8a469f9c73194479efea16(x898.n_name match { case Some(x) => x; case _ => "null" }, x898.n_custs match { case Some(x) => x; case _ => null }))
          }).toSeq
        Recordf9ef78b750814174a26e9a875bca5992(key.r_name, key.Region_index, grp)
    }.as[Recordf9ef78b750814174a26e9a875bca5992]

    val x900 = x899
    x899
  }
  def TestFN0() = {
    val x19 = OrderDF.as[Order].equiJoin(CustomerDF.as[Customer],
      Seq("o_custkey"), Seq("c_custkey"), "inner").as[Record503be495d873475a9919ae04f5e3daa7]

    val x21 = x19.select("o_orderkey", "c_name", "c_nationkey", "o_orderdate")
      .withColumnRenamed("o_orderkey", "c_orderkey")
      .withColumn("c_orderkey", when(col("c_orderkey").isNull, 0).otherwise(col("c_orderkey")))
      .as[Record826ce10bb85642c08632cf3be6164d39]

    val x22 = x21
    val customers = x22
    //customers.cache
    //customers.count
    val x25 = customers.equiJoin(LineItemDF.as[Lineitem],
      Seq("c_orderkey"), Seq("l_orderkey"), "inner").as[Record6bdb66a8492841bcab18709549a64ecc]

    val x27 = x25.select("o_orderdate", "l_quantity", "c_name", "c_nationkey", "l_partkey", "l_suppkey")
      .withColumnRenamed("l_suppkey", "c_suppkey")
      .withColumn("c_suppkey", when(col("c_suppkey").isNull, 0).otherwise(col("c_suppkey")))
      .as[Recordc54b1f15f4fa4e1ba1e4718697cac93e]

    x27
  }
  def TestFN1() = {
    val x1072 = OrderDF.as[Order].equiJoin(CustomerDF.as[Customer],
      Seq("o_custkey"), Seq("c_custkey"), "inner").as[Record7f9be5cc1d6944ecae7868d3b0c1530f]

    val x1074 = x1072.select("o_orderkey", "c_name", "c_nationkey", "o_orderdate")
      .withColumnRenamed("o_orderkey", "c_orderkey")
      .withColumn("c_orderkey", when(col("c_orderkey").isNull, 0).otherwise(col("c_orderkey")))
      .as[Recordcb3025fb5c424437bc4239099b4a055b]

    val x1075 = x1074
    val customers = x1075

    val x1078 = customers.equiJoin(LineItemDF.as[Lineitem],
      Seq("c_orderkey"), Seq("l_orderkey"), "inner").as[Record10734af4065c481f9d029652205fcbad]

    val x1080 = x1078.select("o_orderdate", "l_quantity", "c_name", "c_nationkey", "l_partkey", "l_suppkey")
      .withColumnRenamed("l_suppkey", "c_suppkey")
      .withColumn("c_suppkey", when(col("c_suppkey").isNull, 0).otherwise(col("c_suppkey")))
      .as[Record4394225410284bcdae14689cda257701]

    x1080
  }
  def TestFN2() = {
    val x1131 = OrderDF.as[Order].equiJoin(CustomerDF.as[Customer],
      Seq("o_custkey"), Seq("c_custkey"), "inner").as[Record066b26891fed460884ec7c6fca79eb7a]

    val x1133 = x1131.select("o_orderkey", "c_name", "c_nationkey", "o_orderdate")
      .withColumnRenamed("o_orderkey", "c_orderkey")
      .withColumn("c_orderkey", when(col("c_orderkey").isNull, 0).otherwise(col("c_orderkey")))
      .as[Record1d1a118c3c6a4fba9ec58b18723d2762]

    val x1134 = x1133
    val customers = x1134
    //customers.cache
    //customers.count
    val x1137 = customers.equiJoin(LineItemDF.as[Lineitem],
      Seq("c_orderkey"), Seq("l_orderkey"), "inner").as[Recordc24fe3c64c3c408e9614f45b11226bb0]

    val x1139 = x1137.select("o_orderdate", "l_quantity", "c_name", "c_nationkey", "l_partkey", "l_suppkey")
      .withColumnRenamed("l_suppkey", "c_suppkey")
      .withColumn("c_suppkey", when(col("c_suppkey").isNull, 0).otherwise(col("c_suppkey")))
      .as[Recordb407fd44846b4e0b9624b4b48ceb2dfa]

    val x1140 = x1139
    val csupps = x1140
    //csupps.cache
    //csupps.count
    val x1142 = NationDF.select("n_nationkey", "n_name")

      .as[Record59c086d080a84079a82e5b0a734bcba3]

    val x1143 = x1142.withColumn("Nation_index", monotonically_increasing_id())
      .as[Record32b29990d3084d049289761af6df4773]

    val x1145 = SupplierDF.select("s_nationkey", "s_suppkey", "s_name")

      .as[Record5a2bead45b7542739d5479a72987bc67]

    val x1146 = x1145.withColumn("Supplier_index", monotonically_increasing_id())
      .as[Recordb7f7f6e2366b41a780326d50654e7283]

    val x1149 = x1143.crossJoin(x1146)
      .as[Recordb02ad0ce49af45b3ab2f9545e4b2b70c]

    val x1151 = csupps


    val x1154 = x1149.equiJoin(x1151,
      Seq("s_suppkey"), Seq("c_suppkey"), "left_outer").as[Record98e0624ba5be46d6880c5dfc6c2c3533]

    val x1156 = x1154.select("n_nationkey", "o_orderdate", "l_quantity", "s_nationkey", "Supplier_index", "c_name", "s_name", "c_nationkey", "n_name", "l_partkey", "Nation_index")

      .as[Record2263fc16bc344bfe9358fcad2dc652f2]

    val x1158 = x1156.groupByKey(x1157 => Recordfc97c7722cab4833b317341bbc9e552a(x1157.n_nationkey, x1157.s_nationkey, x1157.Supplier_index, x1157.s_name, x1157.n_name, x1157.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x1157 =>
          (x1157.c_name, x1157.l_partkey, x1157.l_quantity, x1157.o_orderdate, x1157.c_nationkey) match {
            case (None, _, _, _, _) => Seq()
            case (_, None, _, _, _) => Seq()
            case (_, _, None, _, _) => Seq()
            case (_, _, _, None, _) => Seq()
            case (_, _, _, _, None) => Seq()
            case _ => Seq(Recordeb6156742524464c8640d6a15a946236(x1157.o_orderdate match { case Some(x) => x; case _ => "null" }, x1157.l_quantity match { case Some(x) => x; case _ => 0.0 }, x1157.c_name match { case Some(x) => x; case _ => "null" }, x1157.c_nationkey match { case Some(x) => x; case _ => 0 }, x1157.l_partkey match { case Some(x) => x; case _ => 0 }))
          }).toSeq
        Record1c657d3b5308448eb18d6aad792e65f1(key.n_nationkey, grp, key.s_nationkey, key.Supplier_index, key.s_name, key.n_name, key.Nation_index)
    }.as[Record1c657d3b5308448eb18d6aad792e65f1]

    val x1160 = x1158.select("n_nationkey", "customers2", "s_nationkey", "s_name", "n_name", "Nation_index")

      .as[Record797d26c762024522809dbb6f15d62236]

    val x1162 = x1160.groupByKey(x1161 => Record32b29990d3084d049289761af6df4773(x1161.n_nationkey, x1161.n_name, x1161.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x1161 =>
          (x1161.s_nationkey, x1161.s_name) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Record1d46e5cc80304df3a1cfc1f0061bd22a(x1161.s_name match { case Some(x) => x; case _ => "null" }, x1161.s_nationkey match { case Some(x) => x; case _ => 0 }, x1161.customers2))
          }).toSeq
        Recordc58a33a2031342029083d87eef2c61ba(key.n_nationkey, grp, key.n_name, key.Nation_index)
    }.as[Recordc58a33a2031342029083d87eef2c61ba]

    x1162
  }
  def TestFN2Full() = {
    val x1204 = OrderDF.as[Order].equiJoin(CustomerDF.as[Customer],
      Seq("o_custkey"), Seq("c_custkey"), "inner").as[Record0cedb1dcfcf74777b17737e8e18b6214]

    val x1206 = x1204.select("o_orderkey", "c_name", "c_nationkey", "o_orderdate")
      .withColumnRenamed("o_orderkey", "c_orderkey")
      .withColumn("c_orderkey", when(col("c_orderkey").isNull, 0).otherwise(col("c_orderkey")))
      .as[Record8d1d6b8ad0d649438b300790104f14fa]

    val x1207 = x1206
    val customers = x1207
    //customers.cache
    //customers.count
    val x1210 = customers.equiJoin(LineItemDF.as[Lineitem],
      Seq("c_orderkey"), Seq("l_orderkey"), "inner").as[Record1107ef2e5aa444f4b5816bb85f3cdf5e]

    val x1212 = x1210.select("o_orderdate", "l_quantity", "c_name", "c_nationkey", "l_partkey", "l_suppkey")
      .withColumnRenamed("l_suppkey", "c_suppkey")
      .withColumn("c_suppkey", when(col("c_suppkey").isNull, 0).otherwise(col("c_suppkey")))
      .as[Recordbeef89c33b634a9bb5597082628069fe]

    val x1213 = x1212
    val csupps = x1213
    //csupps.cache
    //csupps.count
    val x1215 = SupplierDF.select("s_nationkey", "s_suppkey", "s_name")

      .as[Record9e712157c92e42218e67ab2c0a1bdf62]

    val x1216 = x1215.withColumn("Supplier_index", monotonically_increasing_id())
      .as[Record7bfdde517fa24691b52a6c1f77513d77]

    val x1218 = csupps


    val x1221 = x1216.equiJoin(x1218,
      Seq("s_suppkey"), Seq("c_suppkey"), "left_outer").as[Record303002c6fa7c4024883c71b643a986bf]

    val x1223 = x1221.select("o_orderdate", "l_quantity", "s_nationkey", "Supplier_index", "c_name", "s_name", "c_nationkey", "l_partkey")

      .as[Record8703c788ec0f4f69b6ba6a48b5142187]

    val x1225 = x1223.groupByKey(x1224 => Recorddd9e64ea3b934e0c99906ca50166ee6c(x1224.s_nationkey, x1224.Supplier_index, x1224.s_name)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x1224 =>
          (x1224.l_partkey, x1224.l_quantity, x1224.c_nationkey, x1224.o_orderdate, x1224.c_name) match {
            case (None, _, _, _, _) => Seq()
            case (_, None, _, _, _) => Seq()
            case (_, _, None, _, _) => Seq()
            case (_, _, _, None, _) => Seq()
            case (_, _, _, _, None) => Seq()
            case _ => Seq(Record3a03ce1a7abb4dd9b0e71375f2792035(x1224.o_orderdate match { case Some(x) => x; case _ => "null" }, x1224.l_quantity match { case Some(x) => x; case _ => 0.0 }, x1224.c_name match { case Some(x) => x; case _ => "null" }, x1224.c_nationkey match { case Some(x) => x; case _ => 0 }, x1224.l_partkey match { case Some(x) => x; case _ => 0 }))
          }).toSeq
        Recorde305494cd3a04a6faaa7ebc68a147446(grp, key.s_nationkey, key.Supplier_index, key.s_name)
    }.as[Recorde305494cd3a04a6faaa7ebc68a147446]

    val x1226 = x1225
    val suppliers = x1226
    //suppliers.cache
    //suppliers.count
    val x1228 = NationDF.select("n_nationkey", "n_name")

      .as[Record5d759fe407f348b08500ba765679b4e7]

    val x1229 = x1228.withColumn("Nation_index", monotonically_increasing_id())
      .as[Record22930814c4244ff0ada13658a03ad6c7]

    val x1231 = suppliers


    val x1234 = x1229.equiJoin(x1231,
      Seq("n_nationkey"), Seq("s_nationkey"), "left_outer").as[Record0a76b850483f4b52bb1664a6596bd074]

    val x1236 = x1234.select("n_nationkey", "customers2", "s_name", "n_name", "Nation_index")

      .as[Record7d03ef866dcb42c191f6996b5341680d]

    val x1238 = x1236.groupByKey(x1237 => Record22930814c4244ff0ada13658a03ad6c7(x1237.n_nationkey, x1237.n_name, x1237.Nation_index)).mapGroups {
      case (key, value) =>
        val grp = value.flatMap(x1237 =>
          (x1237.customers2, x1237.s_name) match {
            case (None, _) => Seq()
            case (_, None) => Seq()
            case _ => Seq(Recordf1ee1950038f492f974bcc05846bfb36(x1237.s_name match { case Some(x) => x; case _ => "null" }, x1237.customers2 match { case Some(x) => x; case _ => null }))
          }).toSeq
        Recordd6470045b5fe4191a5fe384698d8ed1e(key.n_nationkey, grp, key.n_name, key.Nation_index)
    }.as[Recordd6470045b5fe4191a5fe384698d8ed1e]

    x1238
  }



}

class GeneratedCodeTests extends AnyFunSpec with BeforeAndAfterEach with Serializable {

  override protected def afterEach(): Unit = {
    Symbol.freshClear()
    JoinContext.freshClear()
    super.afterEach()
  }

  describe("FlatToNested") {
    it("Test0") {
      GeneratedCodeTests.Test0().show(false)
    }
    it("Test0Full") {
      GeneratedCodeTests.Test0Full().show(false)
    }
    it("Test1") {
      GeneratedCodeTests.Test1().show(false)
    }
    it("Test1Full") {
      GeneratedCodeTests.Test1Full().show(false)
    }
    it("Test2") {
      GeneratedCodeTests.Test2().show(false)
    }
    it("Test2Filter") {
      GeneratedCodeTests.Test2Filter().show(false)
    }
    it("Test2Flat") {
      GeneratedCodeTests.Test2Flat().show(false)
    }

    it("Test2Full") {
      GeneratedCodeTests.Test2Full().show(false)
    }

//    it("Test2Full Multiple ArrayType Entries") {
//      val t = Test2FullWithMultipleArrayTypes
//      t.show(false)
//      t.printSchema()
//    }

    it("Test3") {
      GeneratedCodeTests.Test3().show(false)
    }

    it("Test3Flat") {
      GeneratedCodeTests.Test3Flat().show(false)
    }

    it("Test3Full") {
      GeneratedCodeTests.Test3Full().show(false)
    }

    it("Test3FullFlat") {
      GeneratedCodeTests.Test3FullFlat().show(false)
    }

    it("Test4") {
      GeneratedCodeTests.Test4().show(false)
    }

    it("Test4Full") {
      GeneratedCodeTests.Test4Full().show(false)
    }

    it("Test4Flat") {
      GeneratedCodeTests.Test4Flat().show(false)
    }

    it("Test4FullFlat") {
      GeneratedCodeTests.Test4FullFlat().show(false)
    }


    it("Test0Join") {
      GeneratedCodeTests.Test0Join().show(false)

    }

    it("Test1Join") {
      GeneratedCodeTests.Test1Join().show(false)

    }

    it("Test1JoinFlat") {
      GeneratedCodeTests.Test1JoinFlat().show(false)
    }

    it("Test2Join") {
      GeneratedCodeTests.Test2Join().show(false)
    }

    it("Test2JoinFlat") {
      GeneratedCodeTests.Test2JoinFlat().show(false)
    }

    it("Test3Join"){
      GeneratedCodeTests.Test3Join().show(false)
    }

    it("Test3JoinFlat") {
      GeneratedCodeTests.Test3JoinFlat().show(false)
    }

    it("Test4Join") {
      GeneratedCodeTests.Test4Join().show(false)
    }

    it("Test4JoinFlat") {
      GeneratedCodeTests.Test4JoinFlat().show(false)
    }

    it("TestFN0") {
      GeneratedCodeTests.TestFN0().show(false)
    }

    it("TestFN1") {
      GeneratedCodeTests.TestFN1().show(false)
    }

    it("TestFN2") {
      GeneratedCodeTests.TestFN2().show(false)
    }

    it("TestFN2Full") {
      GeneratedCodeTests.TestFN2Full().show(false)
    }
  }

  describe("Nested2Flat") {

      it("Test1Agg1") {
        import spark.implicits._

        val x18 = GeneratedCodeTests.Test1Full.flatMap { case x14 =>
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

        val x28 = GeneratedCodeTests.Test1Full()


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

      val x24 = GeneratedCodeTests.Test2Full().select("c_name", "c_orders")

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
