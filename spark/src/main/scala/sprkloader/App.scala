package sprkloader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{struct, collect_list}

object App{
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkDataset"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val tpch = TPCHLoader(spark)
    val c = tpch.loadCustomersDF
    c.cache
    c.count
    val o = tpch.loadOrdersDF
    o.cache
    o.count
    val l = tpch.loadLineitemDF
    l.cache
    l.count
    val p = tpch.loadPartDF
    p.cache
    p.count

    var start0 = System.currentTimeMillis()
    /**
     * get query plans from sql
     * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-catalyst-QueryPlan.html
     */
    val lpj = l.join(p, l("l_partkey") === p("p_partkey"))
              .groupBy(l("l_orderkey"))
              .agg(collect_list(struct(p("p_name"), l("l_quantity"))).as("parts"))
    lpj.show()

    val o2 = lpj.join(o, lpj("l_orderkey") === o("o_orderkey"))
              .groupBy(o("o_custkey"))
              .agg(collect_list(struct(o("o_orderdate"), lpj("parts"))).as("orders"))
    o2.show()

    val c2 = c.join(o2, c("c_custkey") === o2("o_custkey")).select("c_name", "orders")
    c2.show()

    var end0 = System.currentTimeMillis() - start0
    println("Query1SparkSlenderDS"+sf+","+Config.datapath+","+end0)

  }
}
