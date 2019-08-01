package sprkloader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App{
  def main(args: Array[String]){
    println("hello")
    val conf = new SparkConf().setMaster("local[*]").setAppName("VariantTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val tpch = TPCHLoader(spark)
    val customers = tpch.loadCustomers
    customers.foreach(println(_))
    val partsupp = tpch.loadPartSupp
    partsupp.foreach(println(_))
    val parts = tpch.loadPart
    parts.foreach(println(_))
    val orders = tpch.loadOrders
    orders.foreach(println(_))
    val lineitem = tpch.loadLineitem
    lineitem.foreach(println(_))
    val supplier = tpch.loadSupplier
    supplier.foreach(println(_))
    val nation = tpch.loadNation
    nation.foreach(println(_))
  }
}
