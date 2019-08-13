package shredding.examples

import java.io.ByteArrayInputStream

import com.oda.gdbspark._
import htsjdk.variant.variantcontext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object GDBtoVEP {

  def createProcess(index: Int, input: List[(String, VariantContext)]): Stream[String] = {
    import scala.sys.process._
    val formatted = input.flatMap(v => List(v._2.getAlternateAlleles).map(a =>
      s"${v._2.getContig}\t${v._2.getStart}\t${v._2.getID}\t${v._2.getReference}\t$a\t.\tPASS\t."))
    println("\n\nFormatted input: \n" + formatted.mkString("\n") + "\n")

    val is = new ByteArrayInputStream(formatted.mkString("\n").getBytes("UTF-8"))

    val VepHome = "/mnt/app_hdd/scratch/ensembl-vep"
    val cmd = Process(VepHome + "/vep --cache -o STDOUT --assembly GRCh37 --everything --offline --no_stats --format vcf --dir_cache /mnt/app_hdd/scratch/.vep")

    val p = cmd #< is
    p.run
    return p.lineStream
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://192.168.11.247:7077").setAppName("VariantTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val hostfile = "/nfs/home/mlathara/s3_chr1_json/hostfile"
    val loader = "/nfs3/converge_10640/histogram/jsonfinal/loader_config_file.json"
    val ws = "/nfs3/converge_10640/histogram/workspace"
    val array = "array_0"
    val dbprops = "/nfs3/converge_10640/histogram/converge_10640.db-properties.flat"
    val gdbmap = new GDBMapper(spark, dbprops)
    val gdb = new GDBConnector(spark.sparkContext, loader, hostfile, gdbmap, ws, array)
    gdb.gdb_query.set_query_block_size(250000000)
    val samples = gdbmap.query("callset").select("name").rdd.map(p => p(0).toString).collect().toList
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:postgresql://oda-compute-0-5/jflint")
      .option("dbtable", "converge").option("user", "jflint").option("password", "jflint").load()
    // jdbcDF.toDF().registerTempTable("clinical")
    val regions = List("rs12415800", "rs35936514")
    //val genes = List("SIRT1", "LHPP")
    val variants = gdb.queryByGene(samples, regions, false)
    variants.cache
    println("GDB query results count: " + variants.count)
    val results = variants.mapPartitionsWithIndex {
      (index, iterator) => {
        val results = createProcess(index, iterator.toList)
        results.iterator
      }
    }
    println("\n\n")
    results.collect.map(x => if (!x.startsWith("##")) println(x))
    //    results.collect.foreach(println(_))
    println("\n\nprocesses finished.")
  }
}

