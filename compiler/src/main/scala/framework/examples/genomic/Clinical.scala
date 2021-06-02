package framework.examples.genomic

import framework.common._

trait Biospecimen {

  val biospecOtype = List(
    ("bcr_patient_uuid", StringType), ("bcr_sample_barcode", StringType), 
    ("bcr_aliquot_barcode", StringType), ("bcr_aliquot_uuid", StringType), 
    ("biospecimen_barcode_bottom", StringType), ("center_id", StringType), 
    ("concentration", DoubleType), ("date_of_shipment", StringType), 
    ("is_derived_from_ffpe", StringType), ("plate_column", IntType),
    ("plate_id", StringType), ("plate_row", StringType), 
    ("quantity", DoubleType), ("source_center", IntType), 
    ("volume", DoubleType))
  val biospecType = TupleType(biospecOtype.toMap)

  val pradType = TupleType(Map("bcr_patient_uuid"->StringType,
	"bcr_patient_barcode"->StringType,
	"form_completion_date"->StringType,
	"histologic_diagnosis"->StringType,
	"histologic_diagnosis_other"->StringType,
	"zone_of_origin"->StringType,
	"gleason_pattern_primary"->IntType,
	"gleason_pattern_secondary"->IntType,
	"gleason_score"->IntType,
	"gleason_pattern_tertiary"->IntType,
	"laterality"->StringType,
	"tumor_level"->StringType,
	"gender"->StringType,
	"prospective_collection"->StringType,
	"retrospective_collection"->StringType,
	"birth_days_to"->IntType,
	"history_other_malignancy"->StringType,
	"history_neoadjuvant_treatment"->StringType,
	"initial_pathologic_dx_year"->IntType,
	"days_to_bone_scan"->IntType,
	"bone_scan_results"->StringType,
	"ct_scan_ab_pelvis_indicator"->StringType,
	"days_to_ct_scan_ab_pelvis"->IntType,
	"ct_scan_ab_pelvis_results"->StringType,
	"days_to_mri"->IntType,
	"mri_results"->StringType,
	"lymph_nodes_examined"->StringType,
	"lymph_nodes_examined_count"->IntType,
	"lymph_nodes_examined_he_count"->IntType,
	"residual_tumor"->StringType,
	"vital_status"->StringType,
	"last_contact_days_to"->IntType,
	"death_days_to"->IntType,
	"cause_of_death"->StringType,
	"cause_of_death_source"->StringType,
	"tumor_status"->StringType,
	"days_to_psa_most_recent"->IntType,
	"psa_most_recent_results"->StringType,
	"biochemical_recurrence_indicator"->StringType,
	"radiation_treatment_adjuvant"->StringType,
	"pharmaceutical_tx_adjuvant"->StringType,
	"treatment_outcome_first_course"->StringType,
	"new_tumor_event_dx_indicator"->StringType,
	"days_to_biochemical_recurrence_first"->IntType,
	"race"->StringType,
	"ethnicity"->StringType,
	"age_at_initial_pathologic_diagnosis"->IntType,
	"clinical_M"->StringType,
	"clinical_N"->StringType,
	"clinical_T"->StringType,
	"clinical_stage"->StringType,
	"days_to_initial_pathologic_diagnosis"->IntType,
	"diagnostic_mri_performed"->StringType,
	"disease_code"->StringType,
	"extranodal_involvement"->StringType,
	"icd_10"->StringType,
	"icd_o_3_histology"->StringType,
	"icd_o_3_site"->StringType,
	"informed_consent_verified"->StringType,
	"initial_pathologic_diagnosis_method"->StringType,
	"pathologic_M"->StringType,
	"pathologic_N"->StringType,
	"pathologic_T"->StringType,
	"pathologic_stage"->StringType,
	"patient_id"->StringType,
	"project_code"->StringType,
	"stage_other"->StringType,
	"system_version"->StringType,
	"tissue_source_site"->StringType,
	"tumor_tissue_site"->StringType))

  val tcgaType = TupleType(
  	"sample" -> StringType,
  	"gender" -> StringType,
  	"race" -> StringType,
  	"ethnicity" -> StringType,
  	"tumor_tissue_site" -> StringType,
  	"histological_type" -> StringType
  )

  // this loads aliquot files or prad specific clinical information

  def loadBiospec(shred: Boolean = false, skew: Boolean = false, fname: String = "", name: String = "biospec", func: String = ""): String = {
    if (shred) loadShredBiospec(skew, fname, name, func)
    else if (skew) {
    s"""|val biospecLoader = new BiospecLoader(spark)
        |val ${name}_L = biospecLoader.load$func("$fname")
        |val $name = (biospec_L, biospec_L.empty)
        |$name.cache
        |$name.count
        |""".stripMargin
  }else{
    s"""|val biospecLoader = new BiospecLoader(spark)
        |val $name = biospecLoader.load$func("$fname")
        |$name.cache
        |$name.count
        |""".stripMargin
    }
  }
  
  def loadShredBiospec(skew: Boolean = false, fname: String = "", name: String = "biospec", func: String = ""): String = {
    val biospecLoad = if (skew) "($name, $name.empty)" else name
    s"""|val biospecLoader = new BiospecLoader(spark)
        |val $name = biospecLoader.load$func("$fname")
        |val IBag_${name}__D = $biospecLoad
        |IBag_${name}__D.cache
        |IBag_${name}__D.count
        |""".stripMargin
  }  

  // this loads all the histology data

  def loadTcga(shred: Boolean = false, skew: Boolean = false, fname: String = "", name: String = "clinical"): String = {
    if (shred) loadShredTcga(skew, fname, name)
    else if (skew) {
    s"""|val tcgaLoader = new TCGALoader(spark)
        |val ${name}_L = tcgaLoader.load("$fname", dir = true)
        |val $name = (${name}_L, ${name}_L.empty)
        |$name.cache
        |$name.count
        |""".stripMargin
  }else{
    s"""|val tcgaLoader = new TCGALoader(spark)
        |val $name = tcgaLoader.load("$fname", dir = true)
        |$name.cache
        |$name.count
        |""".stripMargin
    }
  }
  
  def loadShredTcga(skew: Boolean = false, fname: String = "", name: String = "clinical"): String = {
    val biospecLoad = if (skew) s"($name, $name.empty)" else name
    s"""|val tcgaLoader = new TCGALoader(spark)
        |val $name = tcgaLoader.load("$fname", dir = true)
        |val IBag_${name}__D = $biospecLoad
        |IBag_${name}__D.cache
        |IBag_${name}__D.count
        |""".stripMargin
  }  
}
