package framework.examples.genomic

import framework.common._
import framework.examples.Query

trait ConvergeBase extends Query with VCF with GeneLoader {

  def loadConverge(path: String, shred: Boolean = false, skew: Boolean = false): String = {
    if (shred)
      s"""|val convergeLoader = new ConvergeLoader(spark)
          |val IBag_converge__D = convergeLoader.load("$path")
          |IBag_converge__D.cache
          |IBag_converge__D.count
          |""".stripMargin
    else 
      s"""|val convergeLoader = new ConvergeLoader(spark)
          |val converge = convergeLoader.load("$path")
          |converge.cache
          |converge.count
          |""".stripMargin
  }

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    s"""|${loadVCF("/Users/jac/bioqueries/data/sub.vcf", shred, skew)}
        |${loadGene("/Users/jac/oda-irigs-python/CX_PythonLocal_iRIGS/supporting_files/All_human_genes", shred, skew)}
        |${loadConverge("/Users/jac/converge/converge.csv", shred, skew)}
        |""".stripMargin
  }
  
  val convergeType = TupleType("index" -> IntType,
    "srr_id" -> StringType,
    "id" -> StringType,
    "city" -> StringType,
    "province" -> StringType,
    "hospital_code" -> StringType,
    "iscase" -> IntType,
    "dep_e27" -> IntType, 
    "dep_e29" -> IntType,
    "md_a1" -> IntType,
    "md_a2" -> IntType,
    "md_a3" -> IntType,
    "md_a4" -> IntType,
    "md_a5" -> IntType,
    "md_a6" -> IntType,
    "md_a7" -> IntType,
    "md_a8" -> IntType,
    "md_a9" -> IntType,
    "me" -> IntType,
    "panic" -> IntType,
    "gad" -> IntType,
    "dysthymia" -> IntType,
    "postnatal_d" -> IntType,
    "neuroticism" -> IntType,
    "csa" -> IntType,
    "sle" -> IntType,
    "cold_m" -> IntType,
    "auth_m" -> IntType,
    "prot_m" -> IntType,
    "cold_f" -> IntType,
    "auth_f" -> IntType,
    "prot_f" -> IntType,
    "pms" -> IntType,
    "agora_diag" -> IntType,
    "social_diag" -> IntType,
    "animal_diag" -> IntType,
    "situational_diag" -> IntType,
    "blood_diag" -> IntType,
    "phobia" -> IntType,
    "suicidal_thought" -> IntType,
    "suicidal_plan" -> IntType,
    "suicidal_attempt" -> IntType,
    "dob_y" -> IntType,
    "dob_m" -> IntType,
    "dob_d" -> IntType,
    "age" -> IntType,
    "education" -> IntType,
    "occupation" -> IntType,
    "social_class" -> IntType,
    "marital_status" -> IntType,
    "fh_count" -> IntType,
    "height_clean" -> IntType,
    "weight_clean" -> IntType,
    "bmi" -> DoubleType)

  val converge = BagVarRef("converge", BagType(convergeType))
  val cr = TupleVarRef("sample", convergeType)

}