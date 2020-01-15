package sprkloader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, struct, collect_list}

object App{
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkDataset"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
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
    
    c.write.saveAsTable("customer")
    o.write.saveAsTable("orders")
    l.write.saveAsTable("lineitem")
    p.write.saveAsTable("part")

    spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE part COMPUTE STATISTICS")

    def query1() = {
    
      val lpj = l.join(p, l("l_partkey") === p("p_partkey"))
              .groupBy(l("l_orderkey"))
              .agg(collect_list(struct(p("p_name"), l("l_quantity"))).as("parts"))

      val o2 = lpj.join(o, lpj("l_orderkey") === o("o_orderkey"))
              .groupBy(o("o_custkey"))
              .agg(collect_list(struct(o("o_orderdate"), lpj("parts"))).as("orders"))

      val c2 = c.join(o2, c("c_custkey") === o2("o_custkey")).select("c_name", "orders")
      c2.count
    }

    def shredquery1a() = {

      val d1 = l.join(p, l("l_partkey") === p("p_partkey"))
              .select("l_orderkey", "p_name", "l_quantity")
      d1.count
    
      val d2 = c.select("c_name", "c_custkey")
      d2.count

      val d3 = o.select("o_custkey", "o_orderkey", "o_orderdate")
      d3.count
    }

    def shredquery1b() = {
    
      val M_flat1 = c.select("c_name", "c_custkey")
      val M_ctx2 = M_flat1.select("c_custkey")
    
      val M_flat2 = M_ctx2.join(o, o("o_orderkey") === M_ctx2("c_custkey"))
                        .select("o_orderdate", "o_orderkey", "o_custkey")
      val M_ctx3 = M_flat2.select("o_orderkey")
    
      val M_flat3 = M_ctx3.join(l, l("l_orderkey") === M_ctx3("o_orderkey"))
                        .join(p, col("l_partkey") === col("p_partkey"))
                        .select("p_name", "l_quantity", "l_orderkey")
      M_flat3.count
  
    }


    var start0 = System.currentTimeMillis()
    /**
     * get query plans from sql
     * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-catalyst-QueryPlan.html
     */
    query1
    var end0 = System.currentTimeMillis() - start0
    println("Query1SparkSlenderDS"+sf+","+Config.datapath+","+end0)

  }

}
