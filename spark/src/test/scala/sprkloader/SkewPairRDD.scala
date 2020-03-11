package sprkloader

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.SparkSession
// import com.holdenkarau.spark.testing._
case class Record30(l_quantity: Double, l_partkey: Int)
case class Record31(p_name: String, p_partkey: Int)
case class Record34(p_name: String, l_qty: Double)

class TestSkewPairRDD extends FunSuite with BeforeAndAfterEach {

  var spark : SparkSession = _
  override def beforeEach() {
    spark = SparkSession.builder().appName("spark testing")
      .master("local")
      .config("", "")
      .getOrCreate()
  }//with SharedSparkContext {
  
  test("sanity check") {
    val list = List(1, 2, 3, 4)
    val rdd = spark.sparkContext.parallelize(list)
    assert(rdd.count === list.length)
  }

  test("Test 0.0 Project"){
    import SkewTopRDD._
    import SkewPairRDD._
    val tpch = TPCHLoader(spark)
    val p_L = tpch.loadPart()
    val p_H = p_L.sparkContext.emptyRDD[Part]
    val p = (p_L, p_H)
    val (keyP_L, keyP_H) = p.map(x => (x.p_partkey, x))
    assert(keyP_L.count == p_L.count)
    assert(keyP_H.count == 0)
  }
  
  test("Test 0.1 Pair Project"){
    import SkewTopRDD._
    import SkewPairRDD._
    val tpch = TPCHLoader(spark)
    val p_L = tpch.loadPart()
    val p_H = p_L.sparkContext.emptyRDD[Part]
    val p = (p_L, p_H)
    val keyP = p.map(x => (x.p_partkey, x))

    val hkeys = keyP.heavyKeys._2
    assert(hkeys.isEmpty)

    val (projP_L, projP_H) = keyP.map{ case (k,v) => v}
    assert(projP_L.count == keyP._1.count)
    assert(projP_H.count == keyP._2.count)

  }

  test("Test 1.0 CoGroup Domain"){
    import SkewTopRDD._
    import SkewPairRDD._
    val tpch = TPCHLoader(spark)
    val o_L = tpch.loadOrder()
    val o_H = o_L.sparkContext.emptyRDD[Order]
    val o = (o_L, o_H)
    val keyO = o.map(x => (x.o_custkey, x))

    val c_L = tpch.loadCustomer()
    val c_H = c_L.sparkContext.emptyRDD[Customer]
    val c = (c_L, c_H)
    val top = c.createDomain(x => x.c_custkey)

    val odict = keyO.cogroupDomain(top)
    val lightDom = top.union.collect.toSet -- odict.heavyKeys.value
    val ocks = o.map(x => x.o_custkey).union.collect.toSet
    assert(odict.light.keys.collect.toSet == (lightDom intersect ocks))
    assert(odict.heavy.count == odict.heavyKeys.value.size)
    assert(odict.heavyKeys.value.nonEmpty)

  }

  override def afterEach() {
    spark.stop()
  }

}
