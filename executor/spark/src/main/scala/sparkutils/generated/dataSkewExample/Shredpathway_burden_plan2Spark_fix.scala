
package sparkutils.generated

/** Generated **/

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.scalalang._
import sparkutils._
import sparkutils.loader._
import sparkutils.skew.SkewDataset._

case class Record2f933a9276d24b0da28feef471ae2f1f(name: String, gene_set_1: Long)

case class Record45639c7d2a744a3fa45f09b6f1baae7e(p_name: String, url: String, gene_set_LABEL: Long)

case class Record360708c408284b63a16e8fd7ce2760c0(p_name: String, name: String, url: String)

case class Record0f066895d0114ce59e28fe2727f07890(p_name: String, name: String, g_start: Int, url: String, gene_name: String, g_end: Int, g_contig: String)

case class Record9aa455168a19420d94d9c59e16c082f4(pathway_name: String, g_start: Int, gene_name: String, g_end: Int, g_contig: String)

case class Recordee3a25fb4d814c8292e58e0b500aa571(m_sample: String)

case class Record3f3568908dc9406ba2f8296f40935f46(sample: String, pathway_variants: String)

case class Recorde5ba5f02e8de485d8b0397a1144967f1(_LABEL: String)

case class Record35e79c116d1b4bef81506e25286233d5(reference: String, genotypes: Long, alternate: String, contig: String, start: Int, _LABEL: String)

case class Record54840f91a71944f9a329361d1085bacc(reference: String, g_sample: String, alternate: String, contig: String, start: Int, call: Double, _LABEL: String)

case class Recorddf9e455fd1ef4fdf86fc24913ea6a2a5(reference: String, pathway_name: String, g_start: Int, g_sample: String, alternate: String, gene_name: String, g_end: Int, contig: String, g_contig: String, start: Int, call: Double, _LABEL: String)

case class Recorda59f4f4ddbe94fc9b8b936c2ca2c40fe(reference: String, pathway_name: String, burden: Double, alternate: String, gene_name: String, contig: String, _1: String, start: Int, call: Double)

case class Record52733294c0e5475c995721d1cec1dcd3(sample: String, pathways: String)

case class Record1415c33fb3df4a148d0b3c2a5217c690(pathway_name: String, burden: Double, _1: String)

case class Record07e4ebe3945d44c59822458a422348f8(pathway_name: String, burden: Double, _1: String)

case class Recorda64f98cdeb4c4322ab098b269e2e11b8(pathway_name: String, _1: String, burden: Double)

case class Record0ef69cf3d72644829f785c5c13669d34(_1: String, pathway_name: String)


case class JoinRecord(_LABEL:String, _1: Long, g_sample: String, call: Double)
case class JoinRecord1 (reference: String, g_sample: String, alternate: String, contig: String, start: Int, call: Double, _LABEL: String)



object Shredpathway_burden_plan2Spark {
  def main(args: Array[String]) {
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master)
      .setAppName("Shredpathway_burden_plan2Spark" + sf)
      .set("spark.sql.shuffle.partitions", Config.lparts.toString)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val vloader = new VariantLoader(spark, "/mnt/app_hdd/data/Data/Variants/sub_chr22.vcf")
    val (variants, genotypes) = vloader.shredDS
    val IBag_variants__D = variants
    val IDict_variants__D_genotypes = genotypes

    val cloader = new TGenomesLoader(spark)
    val IBag_metadata__D = cloader.load("/mnt/app_hdd/data/Data/Phenotype/1000g.csv")
    val gtfLoader = new GTFLoader(spark, "/mnt/app_hdd/data/Data/Map/Homo_sapiens.GRCh37.87.chr.gtf")
    val Gtfs = gtfLoader.loadDS
    val IBag_Gtfs__D = Gtfs

    val pathwayLoader = new PathwayLoader(spark, "/mnt/app_hdd/data/Data/Pathway/c2.cp.v7.1.symbols.gmt")
    val (pathways, geneSet) = pathwayLoader.shredDS
    val IBag_pathways__D = pathways
    val IDict_pathways__D_gene_set = geneSet

    IBag_metadata__D.cache()
    IBag_metadata__D.count()
    IBag_Gtfs__D.cache()
    IBag_Gtfs__D.count()
    IDict_variants__D_genotypes.cache()
    IDict_variants__D_genotypes.count()
    IBag_variants__D.cache()
    IBag_variants__D.count()

    IBag_pathways__D.cache()
    IBag_pathways__D.count()
    IDict_pathways__D_gene_set.cache()
    IDict_pathways__D_gene_set.count()

    def f = {


      var start0 = System.currentTimeMillis()
      val x132 = IBag_pathways__D
      val x135 = x132.withColumnRenamed("gene_set", "gene_set_LABEL")
        .as[Record45639c7d2a744a3fa45f09b6f1baae7e].equiJoin(
        IDict_pathways__D_gene_set.withColumnRenamed("_1", "gene_set_1").as[Record2f933a9276d24b0da28feef471ae2f1f],
        Seq("gene_set_LABEL", "gene_set_1"), "left_outer").drop("gene_set_LABEL", "gene_set_1")
        .as[Record360708c408284b63a16e8fd7ce2760c0]

      val x137 = IBag_Gtfs__D
      val x140 = x135.equiJoin(x137,
        Seq("gene_name", "name"), "inner").as[Record0f066895d0114ce59e28fe2727f07890]

      val x142 = x140.select("p_name", "name", "g_start", "g_end", "g_contig")
        .withColumnRenamed("p_name", "pathway_name")
        .withColumnRenamed("name", "gene_name")
        .as[Record9aa455168a19420d94d9c59e16c082f4]

      val x143 = x142
      val MBag_pathway_by_gene_1 = x143
      //MBag_pathway_by_gene_1.print
      //MBag_pathway_by_gene_1.cache
      MBag_pathway_by_gene_1.count
      val x145 = IBag_metadata__D.select("m_sample")
        .as[Recordee3a25fb4d814c8292e58e0b500aa571] // (m_sample)
      val x147 = x145
        .withColumn("pathway_variants", col("m_sample"))
        .withColumnRenamed("m_sample", "sample")
        .as[Record3f3568908dc9406ba2f8296f40935f46]

      val x148 = x147
      val MBag_transpose_1 = x148


//      MBag_transpose_1.print
//      MBag_transpose_1.cache
//      MBag_transpose_1.count




      val x150 = MBag_transpose_1   // (sample: String, pathway_variants: String) ==> pathway_variants = sample = m_sample
      val x152 = x150.select("pathway_variants")
        .withColumnRenamed("pathway_variants", "_LABEL")
        .as[Recorde5ba5f02e8de485d8b0397a1144967f1]   // (_LABEL: String) ===> _LABEL = m_sample

      val x153 = x152.distinct
      val x154 = x153
      val Dom_pathway_variants_1 = x154
//      Dom_pathway_variants_1.print
//      Dom_pathway_variants_1.cache
//      Dom_pathway_variants_1.count





      ///// here start

      val x156 = Dom_pathway_variants_1 //(_LABEL: String)
      val x158 = IBag_variants__D // contig: String, start: Int, reference: String, alternate: String, genotypes: Long)

      val x161 = x156.join(IDict_variants__D_genotypes, col("_LABEL") === col("g_sample")).as[JoinRecord]
      //(_LABEL：String, _1: Long, g_sample: String, call: Int)

      val x164 = x161.join(x158, col("_1") === col("genotypes"), "left_outer").as[JoinRecord1]
      // (reference: String, g_sample: String, alternate: String, contig: String, start: Int, call: Double, _LABEL: String)

      ///// here end

      val x166 = MBag_pathway_by_gene_1

      x164.print
      x166.print

//      case class Recorddf9e455fd1ef4fdf86fc24913ea6a2a5(reference: String, pathway_name: String, g_start: Int, g_sample: String, alternate: String, gene_name: String, g_end: Int, contig: String, g_contig: String, start: Int, call: Double, _LABEL: String)

      col("g_contig") === col("contig")
      val x169_temp = x164.join(x166, col("start") >= col("g_start") && col("g_end") >= col("start"), "inner")
        .as[Recorddf9e455fd1ef4fdf86fc24913ea6a2a5]

      val x169 = x169_temp.where(
        x169_temp("_LABEL") === x169_temp("_LABEL") &&
        x169_temp("g_contig") === x169_temp("contig")).as[Recorddf9e455fd1ef4fdf86fc24913ea6a2a5]

      x169.print

//      val x171 = x169.select("reference", "pathway_name", "alternate", "gene_name", "contig", "start", "call", "_LABEL")
//        .withColumn("burden", when(col("call") =!= 0.0, 1.0).otherwise(0.0))
//        .withColumnRenamed("_LABEL", "_1")
//        .as[Recorda59f4f4ddbe94fc9b8b936c2ca2c40fe]
//
//
//      val x172 = x171
//      val MDict_transpose_1_pathway_variants_1 = x172.repartition($"_1")
////      MDict_transpose_1_pathway_variants_1.print
////      MDict_transpose_1_pathway_variants_1.cache
////      MDict_transpose_1_pathway_variants_1.count
//      val x174 = MBag_transpose_1
//      val x176 = x174
//        .withColumn("pathways", col("pathway_variants"))
//        .as[Record52733294c0e5475c995721d1cec1dcd3]
//
//      val x177 = x176
//      val MBag_pathway_burden_plan2_1 = x177
//      //MBag_pathway_burden_plan2_1.print
//      //MBag_pathway_burden_plan2_1.cache
////      MBag_pathway_burden_plan2_1.count
//      val x179 = MDict_transpose_1_pathway_variants_1.select("pathway_name", "burden", "_1")
//        .as[Record1415c33fb3df4a148d0b3c2a5217c690]
//      val x181 = x179
//
//        .as[Record07e4ebe3945d44c59822458a422348f8]
//
//      val x183 = x181.groupByKey(x182 => Record0ef69cf3d72644829f785c5c13669d34(x182._1, x182.pathway_name))
//        .agg(typed.sum[Record07e4ebe3945d44c59822458a422348f8](x182 => x182.burden)
//        ).mapPartitions { it =>
//        it.map { case (key, burden) =>
//          Recorda64f98cdeb4c4322ab098b269e2e11b8(key.pathway_name, key._1, burden)
//        }
//      }.as[Recorda64f98cdeb4c4322ab098b269e2e11b8]
//
//      val x184 = x183
//      val MDict_pathway_burden_plan2_1_pathways_1 = x184.repartition($"_1")
//      //MDict_pathway_burden_plan2_1_pathways_1.print
//      //MDict_pathway_burden_plan2_1_pathways_1.cache
//      //MDict_pathway_burden_plan2_1_pathways_1.count
//
//
//      var end0 = System.currentTimeMillis() - start0
//      println("test," + sf + "," + Config.datapath + "," + end0 + ",query," + spark.sparkContext.applicationId)

    }

    var start = System.currentTimeMillis()
    f
    var end = System.currentTimeMillis() - start

    println("test," + sf + "," + Config.datapath + "," + end + ",total," + spark.sparkContext.applicationId)
  }
}
