package sprkloader.gdbloader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import htsjdk.variant.variantcontext._
import org.apache.spark.rdd.RDD
import java.io.ByteArrayInputStream

import com.oda.gdbspark._

  /** defines a genomic path with name and description url.
    * the file /home/gabor/Downloads/c2.cp.v6.2.symbols.gmt is downloaded from
    *     http://software.broadinstitute.org/gsea/msigdb/download_file.jsp?filePath=/resources/msigdb/6.2/c2.cp.v6.2.symbols.gmt
    * @param sName          the name. For example: KEGG_GLYCOLYSIS_GLUCONEOGENESIS
    * @param descriptionUrl example: http://www.broadinstitute.org/gsea/msigdb/cards/KEGG_GLYCOLYSIS_GLUCONEOGENESIS
    * @param path           example: ACSS2	GCK	PGK2	PGK1	PDHB	PDHA1	PDHA2	PGM2	TPI1	ACSS1	FBP1	ADH1B	HK2	ADH1C	HK1	HK3	ADH4	PGAM2	ADH5	PGAM1	ADH1A [...]
    */
  case class Pathway(sName: String, descriptionUrl: String, path: Array[String]);

object Config {
  val prop = new java.util.Properties
  val fsin = new java.io.FileInputStream("data.flat")
  prop.load(fsin)
  val vepHome = prop.getOrDefault("VepHome","/mnt/app_hdd/scratch/ensembl-vep")
  val vepCommandLine = prop.getOrDefault("commandLine","vep --cache -o STDOUT --assembly GRCh37 --json --everything --offline --no_stats --format vcf --dir_cache /mnt/app_hdd/scratch/.vep")
  val verbose = prop.getOrDefault("verbose", "false").toString.toBoolean
  val hostfile = prop.getOrDefault("hostfile","/nfs/home/mlathara/s3_chr1_json/hostfile").toString
  val loader = prop.getOrDefault("loader","/nfs3/converge_10640/histogram/jsonfinal/loader_config_file.json").toString
  val ws = prop.getOrDefault("ws","/nfs3/converge_10640/histogram/workspace").toString
  val array = prop.getOrDefault("array","array_0").toString
  val dbprops = prop.getOrDefault("dbprops","/nfs3/converge_10640/histogram/converge_10640.db-properties.flat").toString
  val kegHome = prop.getOrDefault("kegHome","/home/gabor/Downloads/").toString

}

class GDBLoader(spark: SparkSession) extends Serializable {
  val c = Config

  // executes a VEP query in a shell. Returns the standard out stream of the process
  def executeVEP(index: Int, input: List[(String, VariantContext)]): Stream[String] = {
    import scala.sys.process._
    val formatted = input.flatMap(v => List(v._2.getAlternateAlleles).map(a =>
      s"${v._2.getContig}\t${v._2.getStart}\t${v._2.getID}\t${v._2.getReference}\t$a\t.\tPASS\t."))
    if (c.verbose) println("\n\nFormatted input: \n" + formatted.mkString("\n") + "\n")

    val is = new ByteArrayInputStream(formatted.mkString("\n").getBytes("UTF-8"))

    val cmd = Process(c.vepHome + "/" + c.vepCommandLine)

    val p = cmd #< is
    p.run
    return p.lineStream
  }
  //file name like: "c2.cp.v6.2.symbols.gmt"
  def parseKeg(fileName:String):RDD[Pathway] = {
    val inputFile = spark.sparkContext.textFile(c.kegHome + fileName, 3)
    //val results = input.mapPartitions(_=>createProcess(_.mkString),true)
    val pathways = inputFile.map(line => {
      val fields = line.split("\t| ")
      Pathway(fields(0), fields(1), fields.tail.tail)
    })//.collect()
    return pathways;
  }
  def executeQuery(regions: List[String]): RDD[RDD[String]] = {
    val gdbmap = new GDBMapper(spark, c.dbprops)
    val gdb = new GDBConnector(spark.sparkContext, c.loader, c.hostfile, gdbmap, c.ws, c.array)
    gdb.gdb_query.set_query_block_size(250000000)
    val samples = gdbmap.query("callset").select("name").rdd.map(p => p(0).toString).collect().toList

    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:postgresql://oda-compute-0-5/jflint")
      .option("dbtable", "converge").option("user", "jflint").option("password", "jflint").load()
    val variants = gdb.queryByGene(samples, regions, false)
    variants.cache
    println("GDB query results count: " + variants.count)
    val results = variants.mapPartitionsWithIndex {
      (index, iterator) => {
        val resultsText = executeVEP(index, iterator.toList)
        resultsText.iterator
      }
    }
    if (c.verbose) {
      println("\n\n")
      results.collect.map(x => if (!x.startsWith("##")) if (c.verbose) println(x))
      println("\n\nprocesses finished.")
    }

    val responseRDD = spark.sparkContext.makeRDD(results.map(x => x.toString) :: Nil)
    responseRDD.foreach(x => spark.read.json(x.toString))
    return responseRDD
  }
}
object GDBLoader {
  def apply(spark: SparkSession): GDBLoader = new GDBLoader(spark)

}

