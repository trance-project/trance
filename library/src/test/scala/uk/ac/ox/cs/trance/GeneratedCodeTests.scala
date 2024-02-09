package uk.ac.ox.cs.trance


import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.utilities.{JoinContext, Symbol}
import Wrapper.DataFrameImplicit
import framework.examples.tpch._
import framework.examples.tpch.Lineitem
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, when}
import uk.ac.ox.cs.trance.utilities.SkewDataset.DatasetOps
import uk.ac.ox.cs.trance.utilities.TPCHDataframes.{spark, LineItem => LineItemDF, Order => OrderDF, Part => PartDF, Customer => CustomerDF}



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

  override protected def afterEach(): Unit = {
    Symbol.freshClear()
    JoinContext.freshClear()
    super.afterEach()
  }

  describe("FlatToNested") {
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
}
