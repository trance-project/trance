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
    import TopRDD._
    import SkewTopRDD._
    import SkewPairRDD._
    val tpch = TPCHLoader(spark)
    val p_L = tpch.loadPart()
    val p = (p_L, p_L.empty)
    val keyP = p.map(x => (x.p_partkey, x))

    val hkeys = keyP.heavyKeys._2
    assert(hkeys.isEmpty)

    val (projP_L, projP_H) = keyP.map{ case (k,v) => v}
    assert(projP_L.count == keyP._1.count)
    assert(projP_H.count == keyP._2.count)

  }

  test("Test 0.2 Pair Project Keep Keys"){
    import TopRDD._
    import SkewTopRDD._
    import SkewPairRDD._
    val tpch = TPCHLoader(spark)
    val l_L = tpch.loadLineitem()
    val l = (l_L, l_L.empty)
    val keyL = l.map(x => (x.l_partkey, x))

    val hkeys = spark.sparkContext.broadcast(keyL.heavyKeys._2)
    assert(hkeys.value.nonEmpty)

    val projL = (keyL._1, keyL._2, hkeys).mapPartitions{ case (k,v) => (k, v.l_quantity)}
    assert(projL._3.value == hkeys.value)

  }

  test("Test 0.3 FlatMap Keep Keys"){
    import TopRDD._
    import SkewTopRDD._
    import SkewPairRDD._
    val tpch = TPCHLoader(spark)
    val l_L = tpch.loadLineitem()
    val keyL = l_L.map(x => (x.l_partkey, Vector(x)))
    val l = (keyL, keyL.empty)

    val p_L = tpch.loadPart()
    val p = (p_L, p_L.empty)
    val keyP = p.map(x => (x.p_partkey, x))

    val lp = l.join(keyP)
    val flp = lp.flatMapPartitions{
      case (pk, (ls, p)) => ls.map(l => pk -> (l, p))
    }
    assert(lp.heavyKeys.value.nonEmpty)
    assert(lp.heavyKeys.value == flp.heavyKeys.value)

    val flp2 = lp.flatMap{
      case (pk, (ls, p)) => ls.map{l => l.l_orderkey -> p}
    }
    assert(flp2.heavyKeys._2 != lp.heavyKeys.value)

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

  test("Test 2.0 Join"){
    import TopRDD._
    import SkewTopRDD._
    import SkewPairRDD._
    val tpch = TPCHLoader(spark)
    val l_L = tpch.loadLineitem()
    val l = (l_L, l_L.empty)
    val keyL = l.map(x => (x.l_partkey, x))

    val hkeys = spark.sparkContext.broadcast(keyL.heavyKeys._2)
    assert(hkeys.value.nonEmpty)

    val p_L = tpch.loadPart()
    val p = (p_L, p_L.empty)
    val keyP = p.map(x => (x.p_partkey, x))

    val lparts = keyL.joinDropKey(keyP)
    val lpwk = keyL.join(keyP)
    assert(lparts.light.count == lpwk.light.count)
    assert(lparts.heavy.count == lpwk.heavy.count)
    assert(lpwk.heavyKeys.value == hkeys.value)
    
  }

  test("Test 3.0 Join Domain"){
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

    val odict = keyO.joinDomain(top)
    val ocks = o.map(x => x.o_custkey).union.collect.toSet
    val odictkeys = odict.union.keys.collect.toSet
    assert(odictkeys == ocks)
    assert(odictkeys != top.union.collect.toSet)
    assert(odict.heavyKeys.value.nonEmpty)
  }

  test("Test 4.0 Nest"){
    
  }

  override def afterEach() {
    spark.stop()
  }

}
