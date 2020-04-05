
package sprkloader.experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap
import sprkloader._
import sprkloader.PairRDDOperations._
import sprkloader.DictRDDOperations._
import sprkloader.TopRDD._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// case class Part1(p_partkey: Int, p_name: String, p_retailprice: Double)
// case class Line1(_KEY: Int, l_partkey: Int, l_quantity: Double)
// case class Cust1(c_name: String, c_orders: Int)
// case class O1(o_custkey: o_orderdate: String, o_parts: Int)
// case class PL1(_KEY: Int, p_name: String, total: Double)
// case class OParts(p_name: String, total: Double)
case class COrders3(o_custkey: Int, o_orderdate: String, o_parts: Seq[OParts1])
case class COrders4(o_orderdate: String, o_parts: Seq[OParts1])
case class COrders5(o_orderdate: String, o_parts: Seq[OParts2])
case class Top1(c_name: String, c_orders: Seq[COrders4])
case class Top1Id(cid: Long, c_name: String, c_orders: Seq[COrders4])
case class OParts1(l_partkey: Int, l_quantity: Double)
case class OParts2(p_name: String, total: Double)
case class Top2(c_name: String, c_orders: Seq[COrders5])
case class Cid2(cid: Long, c_name: String, o_orderdate: Option[String], o_parts: Seq[OParts2])
case class Cid1(cid: Long, c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[OParts1]])
case class Cid(cid: Long, c_name: String, oid: Long, o_orderdate: Option[String], o_parts: Option[Seq[OParts1]])
case class Oid(cid: Long, c_name: String, oid: Long, o_orderdate: Option[String], l_partkey: Option[Int], l_quantity: Option[Double])
case class Flat(cid: Long, c_name: String, oid: Long, o_orderdate: Option[String], p_name: Option[String], total: Option[Double])
case class Flat1(cid: Long, c_name: String, oid: Long, o_orderdate: Option[String], p_name: Option[String], total: Double)

case class OrdersProjId(index: Long, o_orderkey: Int, o_custkey: Int, o_orderdate: String)
case class CustomerProjId(index: Long, c_custkey: Int, c_name: String)
case class LineitemProjId(index: Long, l_orderkey: Int, l_partkey: Int, l_quantity: Double) 

case class InitKey(index: Long, key: Int)
case class KeyTuple(index: Long, key: Int) {
  def canEqual(a: Any) = a.isInstanceOf[KeyTuple]
  override def equals(that: Any): Boolean = that match {
    case KeyTuple(id, k) => key == k
    case _ => false
  }
  override def hashCode: Int = key.hashCode
}


object Query4SparkDataframe extends App {
 override def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
    .setAppName("Query4SparkDataframe"+sf)
    .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
   import spark.implicits._
   import org.apache.spark.sql.expressions.scalalang._
val L = tpch.loadLineitemDF()
L.cache
L.count
val P = tpch.loadPartDF()
P.cache
P.count
val C = tpch.loadCustomersDF()
C.cache
C.count
val O = tpch.loadOrdersDF()
O.cache
O.count
implicit val ncode0 = Encoders.product[OrdersProj]
implicit val ncode5 = Encoders.product[OParts1]
implicit val ncode2 = Encoders.product[OParts2]
implicit val ncode6 = Encoders.product[COrders3]
implicit val ncode7 = Encoders.product[COrders2]
implicit val ncode8 = Encoders.product[Top1]
implicit val ncode9 = Encoders.product[COrders4]
implicit val ncode10 = Encoders.product[Flat1]
implicit val ncode11 = Encoders.product[COrders5]
implicit val ncode12 = Encoders.product[Top2]
implicit val ncode13 = Encoders.product[CustomerProj]
// implicit val ncode12 = Encoders.product[OrdersProj]

val o = O.select("o_custkey", "o_orderdate", "o_orderkey")
  .withColumn("index", monotonically_increasing_id()).as[OrdersProjId]
  .groupByKey(o => o.o_orderkey)

val l = L.select("l_orderkey", "l_partkey", "l_quantity")
  .withColumn("index", monotonically_increasing_id()).as[LineitemProjId]
  .groupByKey(l => l.l_orderkey)

val ol = o.cogroup(l)( 
  (key, orders, lines) => {
    val oparts = lines.map(l => OParts1(l.l_partkey, l.l_quantity)).toSeq
    orders.map(o => COrders3(o.o_custkey, o.o_orderdate, oparts))
  }).groupByKey(ol => ol.o_custkey)
// ol1.collect.foreach(println(_))

val Query1 = C.select("c_name", "c_custkey")
  .withColumn("index", monotonically_increasing_id()).as[CustomerProjId]
  .groupByKey(c => c.c_custkey).cogroup(ol)(
    (key, custs, orders) => {
      val corders = orders.map(o => COrders4(o.o_orderdate, o.o_parts)).toSeq
      custs.map(c => Top1(c.c_name, corders))
  })
Query1.cache
Query1.count

    def f = {
 
var start0 = System.currentTimeMillis()

val query1f = Query1.withColumn("cid", monotonically_increasing_id()).as[Top1Id].flatMap{
  c => if (c.c_orders.isEmpty) Seq(Cid1(c.cid, c.c_name, None, None))
    else c.c_orders.map(o => Cid1(c.cid, c.c_name, Some(o.o_orderdate), Some(o.o_parts)))
}.withColumn("oid", monotonically_increasing_id()).as[Cid].flatMap{
  of => of.o_parts match {
    case Some(oparts) => 
      if (oparts.isEmpty) Seq(Oid(of.cid, of.c_name, of.oid, of.o_orderdate, None, None))
      else oparts.map(pf => Oid(of.cid, of.c_name, of.oid, of.o_orderdate, Some(pf.l_partkey), Some(pf.l_quantity)))
    case _ => Seq(Oid(of.cid, of.c_name, of.oid, of.o_orderdate, None, None))
  }
}.as[Oid]

// query1f.collect.foreach(println(_))

val parts = P.select("p_partkey", "p_name", "p_retailprice").as[PartProj4]
val q1p = query1f.join(parts, query1f("l_partkey") === parts("p_partkey"), "left_outer")
  .drop("p_partkey", "l_partkey")
  .withColumn("total", $"p_retailprice"*$"l_quantity").as[Flat]
  .groupByKey(x => (x.cid, x.c_name, x.oid, x.o_orderdate, x.p_name))
  .agg(typed.sum[Flat](x => x.total match { case Some(r) => r; case _ => 0.0})).mapPartitions(it =>
    it.map{
      // case ((cid, cname, oid, odate, None), tot) => Flat1(cid, cname, oid, odate, None, 0.0)
      case ((cid, cname, oid, odate, pname), tot) => Flat1(cid, cname, oid, odate, pname, tot)})
  .groupByKey(x => (x.cid, x.c_name, x.oid, x.o_orderdate)).mapGroups{
    case ((cid, cname, oid, odate), oparts) => 
      val noparts = oparts.flatMap{
        p => p.p_name match {case Some(pname) => Seq(OParts2(pname, p.total)); case _ => Seq()}
      }.toSeq
      Cid2(cid, cname, odate, noparts)
  }.groupByKey(x => (x.cid, x.c_name)).mapGroups{
    case ((cid, cname), corders) => 
    val ncorders = corders.flatMap{
      case c => c.o_orderdate match { case Some(date) => Seq(COrders5(date, c.o_parts)); case _ => Seq()}
    }.toSeq
    Top2(cname, ncorders)
  }
// q1p.collect.foreach(println(_))
    // q1p.rdd.flatMap{
    //   c =>
    //     if (c.c_orders.isEmpty) List((c.c_name, null, null, null))
    //     else c.c_orders.flatMap{
    //       o =>
    //         if (o.o_parts.isEmpty) List((c.c_name, o.o_orderdate, null, null))
    //         else o.o_parts.map(p => (c.c_name, o.o_orderdate, p.p_name, p.total))
    //      }
    //   }.collect.foreach(println(_))

var end0 = System.currentTimeMillis() - start0
println("Flat++,Standard,Query4,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Flat++,Standard,Query4,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
