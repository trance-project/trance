package framework.examples

import framework.examples.CancerDataLoader.{AliquotLoader, GisticLoader, MAFLoader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App {

  def main(args: Array[String]) {
    // standard setup

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val root = "/home/yash/Documents/CancerData"

    val mafLoader = new MAFLoader(spark)

    val mafPath = root + "/maf/temp.maf"

    val mafData = mafLoader.load(mafPath)
    mafData.printSchema()
    mafData.show(10)
    //    root
    //    |-- donorId: string (nullable = true)
    //    |-- end: integer (nullable = false)
    //    |-- projectId: string (nullable = true)
    //    |-- start: integer (nullable = false)
    //    |-- Reference_Allele: string (nullable = true)
    //    |-- Tumor_Seq_Allele1: string (nullable = true)
    //    |-- Tumor_Seq_Allele2: string (nullable = true)
    //    |-- genes: array (nullable = true)
    //    |    |-- element: struct (containsNull = true)
    //    |    |    |-- chromosome: string (nullable = true)
    //    |    |    |-- biotype: string (nullable = true)
    //    |    |    |-- geneId: string (nullable = true)
    //    |    |    |-- Hugo_Symbol: string (nullable = true)
    //    |    |    |-- consequences: array (nullable = true)
    //    |    |    |    |-- element: struct (containsNull = true)
    //    |    |    |    |    |-- consequenceType: string (nullable = true)
    //    |    |    |    |    |-- functionalImpact: string (nullable = true)
    //    |    |    |    |    |-- SYMBOL: string (nullable = true)
    //    |    |    |    |    |-- Consequence: string (nullable = true)
    //    |    |    |    |    |-- HGVSp_Short: string (nullable = true)
    //    |    |    |    |    |-- Transcript_ID: string (nullable = true)
    //    |    |    |    |    |-- RefSeq: string (nullable = true)
    //    |    |    |    |    |-- HGVSc: string (nullable = true)
    //    |    |    |    |    |-- IMPACT: string (nullable = true)
    //    |    |    |    |    |-- CANONICAL: string (nullable = true)
    //    |    |    |    |    |-- SIFT: string (nullable = true)
    //    |    |    |    |    |-- PolyPhen: string (nullable = true)
    //    |    |    |    |    |-- Strand: string (nullable = true)
    //
    //    +--------------------+---------+---------+---------+----------------+-----------------+-----------------+--------------------+
    //    |             donorId|      end|projectId|    start|Reference_Allele|Tumor_Seq_Allele1|Tumor_Seq_Allele2|               genes|
    //    +--------------------+---------+---------+---------+----------------+-----------------+-----------------+--------------------+
    //    |8332806e-f547-4aa...|152760163|    WUGSC|152760163|               G|                G|                A|[[chr1, ENSG00000...|
    //    |8332806e-f547-4aa...| 63323535|    WUGSC| 63323535|               G|                G|                A|[[chr1, ENSG00000...|
    //    |8332806e-f547-4aa...|168246650|    WUGSC|168246650|               T|                T|                G|[[chr1, ENSG00000...|
    //    |8332806e-f547-4aa...|172133474|    WUGSC|172133474|               A|                A|                G|[[chr3, ENSG00000...|
    //    |8332806e-f547-4aa...|113965097|    WUGSC|113965097|               A|                A|                C|[[chr3, ENSG00000...|
    //    +--------------------+---------+---------+---------+----------------+-----------------+-----------------+--------------------+


    val gisticPath = root + "/focalScore/BRCA.focal_score_by_genes.txt"
    val gisticLoader = new GisticLoader(spark)
    val data2 = gisticLoader.load(gisticPath)
    data2.show()
    data2.printSchema()

    /*
    * +------------------+--------+--------------------+
      |              gene|cytoband|             samples|
      +------------------+--------+--------------------+
      |ENSG00000008128.21|       0|[[8cd0933f-c93e-4...|
      |ENSG00000008130.14|       0|[[8cd0933f-c93e-4...|
      |ENSG00000067606.14|       0|[[8cd0933f-c93e-4...|
      |ENSG00000078369.16|       0|[[8cd0933f-c93e-4...|

      root
       |-- gene: string (nullable = true)
       |-- cytoband: string (nullable = true)
       |-- samples: array (nullable = true)
       |    |-- element: struct (containsNull = true)
       |    |    |-- name: string (nullable = true)
       |    |    |-- focalScore: integer (nullable = false)
    * */

    val aliquotPath = root + "/association/nationwidechildrens.org_biospecimen_aliquot_brca.txt"
    val aliquotLoader = new AliquotLoader(spark)
    val aliquotData = aliquotLoader.load(aliquotPath)
    aliquotData.printSchema()
    aliquotData.show()

    /*
    * root
       |-- bcr_patient_uuid: string (nullable = true)
       |-- bcr_sample_barcode: string (nullable = true)
       |-- bcr_aliquot_barcode: string (nullable = true)
       |-- bcr_aliquot_uuid: string (nullable = true)
       |-- biospecimen_barcode_bottom: integer (nullable = true)
       |-- center_id: integer (nullable = true)
       |-- concentration: double (nullable = true)
       |-- date_of_shipment: timestamp (nullable = true)
       |-- is_derived_from_ffpe: string (nullable = true)
       |-- plate_column: integer (nullable = true)
       |-- plate_id: string (nullable = true)
       |-- plate_row: string (nullable = true)
       |-- quantity: double (nullable = true)
       |-- source_center: integer (nullable = true)
       |-- volume: double (nullable = true)

      +--------------------+------------------+--------------------+--------------------+--------------------------+---------+-------------+-------------------+--------------------+------------+--------+---------+--------+-------------+------+
      |    bcr_patient_uuid|bcr_sample_barcode| bcr_aliquot_barcode|    bcr_aliquot_uuid|biospecimen_barcode_bottom|center_id|concentration|   date_of_shipment|is_derived_from_ffpe|plate_column|plate_id|plate_row|quantity|source_center|volume|
      +--------------------+------------------+--------------------+--------------------+--------------------------+---------+-------------+-------------------+--------------------+------------+--------+---------+--------+-------------+------+
      |eda6d2d5-4199-4f7...|  TCGA-AR-A1AR-01A|TCGA-AR-A1AR-01A-...|05c45162-6c94-4a1...|                  99016644|        2|         0.16|2011-03-08 00:00:00|                  NO|           5|    A133|        B|    2.08|           23|  13.0|
      |eda6d2d5-4199-4f7...|  TCGA-AR-A1AR-01A|TCGA-AR-A1AR-01A-...|c7976361-e689-44f...|                 108477580|        1|         0.16|2011-03-08 00:00:00|                  NO|           5|    A134|        B|    1.07|           23|  6.67|
      |eda6d2d5-4199-4f7...|  TCGA-AR-A1AR-01A|TCGA-AR-A1AR-01A-...|008ba655-a0a3-42c...|                 108477484|        9|         0.08|2011-03-08 00:00:00|                  NO|           5|    A135|        B|     3.2|           23|  40.0|

    * */


    // experiment
//    val all_effects = "FOXD3,synonymous_variant,p.Q159Q,ENST00000371116,NM_012183.2,c.477G>A,LOW,YES,,,1;MIR6068,downstream_gene_variant,,ENST00000615405,,,MODIFIER,YES,,,-1;FOXD3-AS1,non_coding_transcript_exon_variant,,ENST00000431294,,n.48C>T,MODIFIER,,,,-1;FOXD3-AS1,intron_variant,,ENST00000427268,,n.87+820C>T,MODIFIER,,,,-1;FOXD3-AS1,upstream_gene_variant,,ENST00000418244,,,MODIFIER,YES,,,-1;FOXD3-AS1,upstream_gene_variant,,ENST00000449386,,,MODIFIER,,,,-1;FOXD3-AS1,upstream_gene_variant,,ENST00000426393,,,MODIFIER,,,,-1"
//
//    case class Consequence(SYMBOL: String, Consequence: String, HGVSp_Short: String, Transcript_ID: String, RefSeq:String , HGVSc:String , IMPACT:String ,CANONICAL:String , SIFT: String, PolyPhen:String , Strand:String)
//    all_effects.split(";").map(
//      line => {
//        val fields = line.split(",")
//        val c = Consequence(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10))
//
//        println(c.toString)
//      }
//    )
  }
}

