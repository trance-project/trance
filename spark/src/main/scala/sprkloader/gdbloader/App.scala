package sprkloader.gdbloader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App{
  def main(args: Array[String]){
    println("gdbLoaderTest")
//    val conf = new SparkConf().setMaster("local[*]").setAppName("gdbLoaderTest")
    val conf = new SparkConf().setMaster("spark://192.168.11.247:7077").setAppName("gdbLoaderTest")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val gdbLoader = GDBLoader(spark)
    val regions = List("rs12415800", "rs35936514")
    val l =GDBLoader (spark)
    val res = l.executeQuery(regions)
    println("gdbLoaderTest results:")
    println("Kegg data: " )
    l.parseKeg("c2.cp.v6.2.symbols.gmt").collect().foreach(println(_))
    res.foreach(println(_))

  }
}
