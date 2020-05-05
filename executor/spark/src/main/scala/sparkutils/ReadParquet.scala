package sprkloader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, struct, collect_list}
import SkewPairRDD._

object ReadParquet extends App{

  object Query4{
    def run(){
      val sf = Config.datapath.split("/").last
      val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkDataset"+sf)
      val spark = SparkSession.builder().config(conf).getOrCreate()

      import spark.implicits._
      val df = spark.read.parquet("query1Parquet")
      df.show()
    }
  }
  Query4.run()
}
