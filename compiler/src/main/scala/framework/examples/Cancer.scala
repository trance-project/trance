package framework.examples
import framework.common
import framework.common._
import framework.examples.CancerDataLoader._
import org.apache.spark.sql.SparkSession
trait Cancer extends Query{
  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    if (shred) // todo: add shredDS
      s"""|val aliquotLoader = new AliquotLoader(spark)
          |val association = aliquotLoader.load("/mnt/app_hdd/data/Data/association/nationwidechildrens.org_biospecimen_aliquot_brca.txt")
          |val mafLoader = new MAFLoader(spark)
          |val somatics = mafLoader.load("/mnt/app_hdd/data/Data/maf/somatics")
          |val gisticLoader = new GisticLoader(spark)
          |val gistic = gisticLoader.load("/mnt/app_hdd/data/Data/focalScore/BRCA.focal_score_by_genes.txt")
          |""".stripMargin
    else
      s"""|val aliquotLoader = new AliquotLoader(spark)
          |val association = aliquotLoader.load("/mnt/app_hdd/data/Data/association/nationwidechildrens.org_biospecimen_aliquot_brca.txt")
          |val mafLoader = new MAFLoader(spark)
          |val somatic = mafLoader.load("/mnt/app_hdd/data/Data/maf/somatics")
          |val gisticLoader = new GisticLoader(spark) //Genomic Identification of Significant Targets in Cancer
          |val gistic = gisticLoader.load("/mnt/app_hdd/data/Data/focalScore/BRCA.focal_score_by_genes.txt")
          |""".stripMargin
  }

  // define the types:
  val aliquotType = TupleType("bcr_patient_uuid" -> StringType,
    "bcr_sample_barcode" -> StringType,
    "bcr_aliquot_barcode" -> StringType,
    "bcr_aliquot_uuid" -> StringType,
    "biospecimen_barcode_bottom" -> StringType,
    "center_id" -> StringType,
    "concentration" -> StringType,
    "date_of_shipment" -> StringType,
    "is_derived_from_ffpe" -> StringType,
    "plate_column" -> StringType,
    "plate_id" -> StringType,
    "plate_row" -> StringType,
    "quantity" -> StringType,
    "source_center" -> StringType,
    "volume" -> StringType)

  val gisticSampleType = TupleType("name" -> StringType, "focalScore" -> IntType)
  val gisticType = TupleType("gene" -> StringType, "cytoband" -> StringType, "samples" -> BagType(gisticSampleType))

  val consequenceType = TupleType("consequenceType" -> StringType,
    "functionalImpact" -> StringType,
    "SYMBOL" -> StringType,
    "Consequence" -> StringType,
    "HGVSp_Short" -> StringType,
    "Transcript_ID" -> StringType,
    "RefSeq" -> StringType,
    "HGVSc" -> StringType,
    "IMPACT" -> StringType,
    "CANONICAL" -> StringType,
    "SIFT" -> StringType,
    "PolyPhen" -> StringType,
    "Strand" -> StringType)
  val geneType = TupleType("chromosome" -> StringType, "biotype" -> StringType, "geneId" -> StringType, "Hugo_Symbol" -> StringType,
    "consequences" -> BagType(consequenceType))
  val occurrenceType = TupleType("donorId" -> StringType,
    "end" -> IntType,
    "projectId" -> StringType,
    "start" -> IntType,
    "Reference_Allele" -> StringType,
    "Tumor_Seq_Allele1" -> StringType,
    "Tumor_Seq_Allele2" -> StringType,
    "genes" -> BagType(geneType))

  // define references to these types
  // aliquot
  val associations = BagVarRef("association", BagType(aliquotType))
  val ar = TupleVarRef("a", aliquotType)

  // gistic: Genomic Identification of Significant Targets in Cancer
  val gistics = BagVarRef("gistic", BagType(gisticType))
  val gr = TupleVarRef("g", gisticType)
  val gsr = TupleVarRef("gs", gisticSampleType)

  // maf
  val somatics = BagVarRef("somatic", BagType(occurrenceType))
  val sr = TupleVarRef("s", occurrenceType)
  val gir = TupleVarRef("gene", geneType)
  val cr = TupleVarRef("c", consequenceType)

}
