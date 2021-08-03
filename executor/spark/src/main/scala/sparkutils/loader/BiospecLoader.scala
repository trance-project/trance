package sparkutils.loader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import sparkutils.Config

/** Biospecimen loader designed from the GDC/ICGC biospecimen aliquot 
  * files (bcr biotab). Such as: 
  * https://portal.gdc.cancer.gov/files/e5ebf196-d464-4592-895e-addd43851c16
  *
  **/

case class Biospec(bcr_patient_uuid: String, bcr_sample_barcode: String, bcr_aliquot_barcode: String, bcr_aliquot_uuid: String, biospecimen_barcode_bottom: String, center_id: String, concentration: Double, date_of_shipment: String, is_derived_from_ffpe: String, plate_column: Int, plate_id: String, plate_row: String, quantity: Double, source_center: Int, volume: Double)

case class PradClinical(bcr_patient_uuid: String,bcr_patient_barcode: String,form_completion_date: String,histologic_diagnosis:String,histologic_diagnosis_other:String,zone_of_origin:String,gleason_pattern_primary:Int,gleason_pattern_secondary:Int,gleason_score:Int,gleason_pattern_tertiary:Int
  laterality:String,tumor_level:String,gender:String,prospective_collection:String,retrospective_collection:String,birth_days_to:Int
  birth_days_to:Int,history_other_malignancy:String,history_neoadjuvant_treatment:String,initial_pathologic_dx_year:Int,
  days_to_bone_scan:Int,bone_scan_results:String,ct_scan_ab_pelvis_indicator:String,days_to_ct_scan_ab_pelvis:Int,ct_scan_ab_pelvis_results:String,days_to_mri:Int,mri_results:String,lymph_nodes_examined:String,lymph_nodes_examined_count:Int,lymph_nodes_examined_he_count:Int,residual_tumor:String,vital_status:String,last_contact_days_to:Int,death_days_to:Int, cause_of_death:String,cause_of_death_source:String,tumor_status:String, days_to_psa_most_recent:Int,psa_most_recent_results:String,biochemical_recurrence_indicator:String,radiation_treatment_adjuvant:String,pharmaceutical_tx_adjuvant:String,treatment_outcome_first_course:String,new_tumor_event_dx_indicator:String,days_to_biochemical_recurrence_first:Int,race:String,ethnicity:String,age_at_initial_pathologic_diagnosis:Int,clinical_M:String,clinical_N:String, clinical_T:String,clinical_stage:String,days_to_initial_pathologic_diagnosis:Int,diagnostic_mri_performed:String,disease_code:String,extranodal_involvement:String,icd_10:String,icd_o_3_histology:String,icd_o_3_site:String, informed_consent_verified:String,initial_pathologic_diagnosis_method:String,pathologic_M:String,pathologic_N:String,pathologic_T:String,pathologic_stage:String,patient_id:String,project_code:String,stage_other:String,system_version:String,tissue_source_site:String,tumor_tissue_site:String)


class BiospecLoader(spark: SparkSession) extends Serializable { //extends Table[Biospec] {
 
   import spark.implicits._
   val schema = StructType(Array(StructField("bcr_patient_uuid", StringType),
      StructField("bcr_sample_barcode", StringType),
      StructField("bcr_aliquot_barcode", StringType),
      StructField("bcr_aliquot_uuid", StringType),
      StructField("biospecimen_barcode_bottom", StringType),
      StructField("center_id", StringType),
      StructField("concentration", DoubleType),
      StructField("date_of_shipment", StringType),
      StructField("is_derived_from_ffpe", StringType),
      StructField("plate_column", IntegerType),
      StructField("plate_id", StringType),
      StructField("plate_row", StringType),
      StructField("quantity", DoubleType),
      StructField("source_center", IntegerType),
      StructField("volume", DoubleType)))
     val pradSchema = StructType(Array(StructField("bcr_patient_uuid",StringType,true),
    StructField("bcr_patient_barcode",StringType,true), 
    StructField("form_completion_date",StringType,true), 
    StructField("histologic_diagnosis",StringType,true), 
    StructField("histologic_diagnosis_other",StringType,true), 
    StructField("zone_of_origin",StringType,true), 
    StructField("gleason_pattern_primary",IntegerType,true), 
    StructField("gleason_pattern_secondary",IntegerType,true), 
    StructField("gleason_score",IntegerType,true), 
    StructField("gleason_pattern_tertiary",IntegerType,true), 
    StructField("laterality",StringType,true), 
    StructField("tumor_level",StringType,true), 
    StructField("gender",StringType,true), 
    StructField("prospective_collection",StringType,true), 
    StructField("retrospective_collection",StringType,true), 
    StructField("birth_days_to",IntegerType,true), 
    StructField("history_other_malignancy",StringType,true), 
    StructField("history_neoadjuvant_treatment",StringType,true), 
    StructField("initial_pathologic_dx_year",IntegerType,true), 
    StructField("days_to_bone_scan",IntegerType,true), 
    StructField("bone_scan_results",StringType,true), 
    StructField("ct_scan_ab_pelvis_indicator",StringType,true), 
    StructField("days_to_ct_scan_ab_pelvis",IntegerType,true), 
    StructField("ct_scan_ab_pelvis_results",StringType,true),
    StructField("days_to_mri",IntegerType,true), 
    StructField("mri_results",StringType,true), 
    StructField("lymph_nodes_examined",StringType,true), 
    StructField("lymph_nodes_examined_count",IntegerType,true), 
    StructField("lymph_nodes_examined_he_count",IntegerType,true), 
    StructField("residual_tumor",StringType,true), 
    StructField("vital_status",StringType,true), 
    StructField("last_contact_days_to",IntegerType,true),
    StructField("death_days_to",IntegerType,true), 
    StructField("cause_of_death",StringType,true), 
    StructField("cause_of_death_source",StringType,true), 
    StructField("tumor_status",StringType,true), 
    StructField("days_to_psa_most_recent",IntegerType,true), 
    StructField("psa_most_recent_results",StringType,true), 
    StructField("biochemical_recurrence_indicator",StringType,true), 
    StructField("radiation_treatment_adjuvant",StringType,true),
    StructField("pharmaceutical_tx_adjuvant",StringType,true), 
    StructField("treatment_outcome_first_course",StringType,true), 
    StructField("new_tumor_event_dx_indicator",StringType,true), 
    StructField("days_to_biochemical_recurrence_first",IntegerType,true), 
    StructField("race",StringType,true), 
    StructField("ethnicity",StringType,true), 
    StructField("age_at_initial_pathologic_diagnosis",IntegerType,true), 
    StructField("clinical_M",StringType,true), 
    StructField("clinical_N",StringType,true), 
    StructField("clinical_T",StringType,true), 
    StructField("clinical_stage",StringType,true), 
    StructField("days_to_initial_pathologic_diagnosis",IntegerType,true), 
    StructField("diagnostic_mri_performed",StringType,true), 
    StructField("disease_code",StringType,true), 
    StructField("extranodal_involvement",StringType,true), 
    StructField("icd_10",StringType,true), 
    StructField("icd_o_3_histology",StringType,true), 
    StructField("icd_o_3_site",StringType,true), 
    StructField("informed_consent_verified",StringType,true), 
    StructField("initial_pathologic_diagnosis_method",StringType,true), 
    StructField("pathologic_M",StringType,true), 
    StructField("pathologic_N",StringType,true), 
    StructField("pathologic_T",StringType,true), 
    StructField("pathologic_stage",StringType,true), 
    StructField("patient_id",StringType,true), 
    StructField("project_code",StringType,true), 
    StructField("stage_other",StringType,true), 
    StructField("system_version",StringType,true), 
    StructField("tissue_source_site",StringType,true), 
    StructField("tumor_tissue_site",StringType,true)))
   
   val header: Boolean = true
   val delimiter: String = "\t"
   
   def load(path: String): Dataset[Biospec] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path).na.drop.as[Biospec]//.repartition(Config.minPartitions)
   }

   def loadPrad(path: String): DataFrame = {
      spark.read.schema(pradSchema)
        .option("header", header)
        .option("delimiter", "\t")
        .csv(path)
   }

}

